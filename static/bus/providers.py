# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT
# cSpell:words Kanto Keio Kogyo Kokusai Nishi Seibu Sotetsu Tobu Toei Tokyu

from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Literal, Mapping, Optional, Set, Tuple

from ..apikeys import set_apikeys
from ..err import InvalidData
from ..rail.providers.odpt import (ApiSession, get_arr_dep_times,
                                   remove_prefix, remove_prefixes)
from . import model

HOURS_22 = 22 * 3600
HOURS_24 = 24 * 3600


class ODPTProvider(model.Provider):
    def __init__(self):
        self._session: Optional[ApiSession] = None
        self._apikey: str = ""
        self.pattern_directions: Dict[model.PatternID, Literal["0", "1"]] = {}
        self.pattern_routes: Dict[model.PatternID, model.RouteID] = {}

    def _ensure_session(self):
        assert self._apikey
        if self._session is None:
            self._session = ApiSession("https://api.odpt.org/api/v4/", self._apikey)

    @property
    def name(self) -> str:
        return "odpt"

    @property
    def provides(self) -> Set[model.AgencyID]:
        return {"KeioBus", "NishiTokyoBus", "SeibuBus", "Toei", "TokyuBus", "YokohamaMunicipal"}

    @property
    def attribution(self) -> str:
        return "Public Transportation Open Data Center"

    @property
    def needs_apikey(self) -> bool:
        return True

    def set_apikey(self, key: str) -> None:
        self._apikey = key

    def close(self) -> None:
        if self._session:
            self._session.close()

    @staticmethod
    def _is_invalid_stop(api_stop: Mapping[str, Any]) -> bool:
        return api_stop["geo:long"] in {0, None} or api_stop["geo:lat"] in {0, None} \
                or ("title" not in api_stop and "dc:title" not in api_stop)

    def routes(self) -> Iterable[model.Route]:
        self._ensure_session()
        assert self._session

        seen: Set[model.RouteID] = set()

        for api_pattern in self._session.get("BusroutePattern.json"):
            agency_id = remove_prefix(api_pattern["odpt:operator"])
            route_id = remove_prefix(api_pattern["odpt:busroute"])
            pattern_id = remove_prefix(api_pattern["owl:sameAs"])

            if agency_id not in self.provides:
                continue

            self.pattern_routes[pattern_id] = route_id
            if "odpt:direction" in api_pattern:
                direction = api_pattern["odpt:direction"]
                if direction in {"0", "1"}:
                    self.pattern_directions[pattern_id] = direction

            if route_id in seen:
                continue

            route_code = api_pattern["dc:title"].partition(" ")[0]
            seen.add(route_id)
            yield model.Route(route_id, agency_id, route_code)

    def stops(self) -> Iterable[model.Stop]:
        self._ensure_session()
        assert self._session

        for api_stop in self._session.get("BusstopPole.json"):
            interesting = any(
                agency in self.provides
                for agency in remove_prefixes(api_stop["odpt:operator"])
            )

            if not interesting or self._is_invalid_stop(api_stop):
                continue

            if "title" in api_stop:
                name = model.Name(api_stop["title"]["ja"], api_stop["title"].get("en", ""))
            else:
                name = model.Name(api_stop["dc:title"], "")

            yield model.Stop(
                id=remove_prefix(api_stop["owl:sameAs"]),
                name=name,
                lat=api_stop["geo:lat"],
                lon=api_stop["geo:long"],
                code=api_stop.get("odpt:busstopPoleNumber"),
            )

    def calendars(self) -> Iterable[model.Calendar]:
        self._ensure_session()
        assert self._session

        for api_calendar in self._session.get("Calendar.json"):
            calendar_id = remove_prefix(api_calendar["owl:sameAs"])

            # Generic calendar
            if not calendar_id.startswith("Specific"):
                yield model.Calendar(calendar_id)
                continue

            # Specific calendar
            agency = remove_prefix(api_calendar["odpt:operator"]) \
                if "odpt:operator" in api_calendar \
                else calendar_id.split(".", maxsplit=3)[1]

            if agency in self.provides:
                yield model.Calendar(
                    id=calendar_id,
                    days=[datetime.strptime(i, "%Y-%m-%d").date()
                          for i in api_calendar.get("odpt:day", [])]
                )

    def trips(self) -> Iterable[model.Trip]:
        self._ensure_session()
        assert self._session

        if not self.pattern_routes:
            raise RuntimeError("Provider.routes() weren't called before Provider.trips()")

        for api_trip in self._session.get("BusTimetable.json"):
            agency_id = remove_prefix(api_trip["odpt:operator"])

            if agency_id not in self.provides:
                continue

            pattern_id = remove_prefix(api_trip["odpt:busroutePattern"])
            trip_id = remove_prefix(api_trip["owl:sameAs"])
            route_id = self.pattern_routes[pattern_id]
            direction = self.pattern_directions.get(pattern_id)

            headsign: Optional[str] = None
            non_step: Optional[bool] = None
            times: List[model.StopTime] = []

            previous_departure: int = 0

            for idx, api_time in enumerate(api_trip["odpt:busTimetableObject"]):
                arr, dep = get_arr_dep_times(api_time)
                if arr < 0 or dep < 0:
                    raise InvalidData(f"Trip {trip_id} has no times for StopTime no {idx}")

                # Shift times beyond midnight
                while arr < previous_departure:
                    arr += HOURS_24

                if api_trip.get("odpt:isMidnight", False) and arr < HOURS_22:
                    arr += HOURS_24

                while dep < previous_departure:
                    dep += HOURS_24

                previous_departure = dep

                # Other meta-data
                if headsign is None and "odpt:destinationSign" in api_time:
                    headsign = api_time["odpt:destinationSign"]

                if "odpt:isNonStepBus" in api_time:
                    non_step_value = api_time["odpt:isNonStepBus"]
                    if non_step is None:
                        non_step = non_step_value
                    elif non_step is True and non_step_value is False:
                        non_step = False

                times.append(model.StopTime(
                    stop=remove_prefix(api_time["odpt:busstopPole"]),
                    arrival=arr,
                    departure=dep,
                    no_boarding=not api_time.get("odpt:canGetOn", True),
                    no_disembarking=not api_time.get("odpt:canGetOff", True),
                ))

            yield model.Trip(
                id=trip_id,
                route=route_id,
                pattern=pattern_id,
                calendar=remove_prefix(api_trip["odpt:calendar"]),
                times=times,
                headsign=headsign,
                non_step=non_step,
                direction=direction,
            )

    def count_stops(self) -> Tuple[Mapping[model.AgencyID, int], Mapping[model.AgencyID, int]]:
        self._ensure_session()
        assert self._session

        valid_stops: defaultdict[model.AgencyID, int] = defaultdict(lambda: 0)
        invalid_stops: defaultdict[model.AgencyID, int] = defaultdict(lambda: 0)

        for api_stop in self._session.get("BusstopPole.json"):
            agencies = remove_prefixes(api_stop["odpt:operator"])

            if self._is_invalid_stop(api_stop):
                for agency in agencies:
                    invalid_stops[agency] += 1
            else:
                for agency in agencies:
                    valid_stops[agency] += 1

        return valid_stops, invalid_stops


class TokyoChallengeProvider(ODPTProvider):
    def _ensure_session(self):
        if self._session is None:
            self._session = ApiSession("https://api-tokyochallenge.odpt.org/api/v4/",
                                       self._apikey)

    @property
    def name(self) -> str:
        return "odpt_tokyo"

    @property
    def provides(self) -> Set[model.AgencyID]:
        return {"KantoBus", "KokusaiKogyoBus", "SotetsuBus", "TobuBus"}

    @property
    def attribution(self) -> str:
        return "Open Data Challenge for Public Transportation in Tokyo"


def get_all_providers() -> List[model.Provider]:
    p = [TokyoChallengeProvider(), ODPTProvider()]
    set_apikeys(p)
    return p
