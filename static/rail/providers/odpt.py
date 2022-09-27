# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from datetime import datetime
from typing import (Any, Iterable, Iterator, List, Mapping, Optional, Set,
                    Tuple, Union)
from urllib.parse import quote as urlquote
from urllib.parse import urljoin

import requests

from ...const import API_TIMEOUT
from ...util import IJsonIterator
from .. import model

# cSpell: disable
JREAST_ROUTES = {
    "JR-East.ChuoRapid", "JR-East.ChuoSobuLocal", "JR-East.Hachiko", "JR-East.Ito",
    "JR-East.Itsukaichi", "JR-East.Joban", "JR-East.JobanLocal", "JR-East.JobanRapid",
    "JR-East.Kashima", "JR-East.Kawagoe", "JR-East.KeihinTohokuNegishi", "JR-East.Keiyo",
    "JR-East.Kururi", "JR-East.Musashino", "JR-East.Nambu", "JR-East.NambuBranch",
    "JR-East.Narita", "JR-East.NaritaAbikoBranch", "JR-East.NaritaAirportBranch",
    "JR-East.Ome", "JR-East.Sagami", "JR-East.SaikyoKawagoe", "JR-East.ShonanShinjuku",
    "JR-East.Sobu", "JR-East.SobuRapid", "JR-East.SotetsuDirect", "JR-East.Sotobo",
    "JR-East.Takasaki", "JR-East.Tsurumi", "JR-East.TsurumiOkawaBranch",
    "JR-East.TsurumiUmiShibauraBranch", "JR-East.Togane", "JR-East.Tokaido", "JR-East.Uchibo",
    "JR-East.Utsunomiya", "JR-East.Yamanote", "JR-East.Yokohama", "JR-East.Yokosuka",
}
# cSpell: enable


def remove_prefix(x: str) -> str:
    return x.split(":", maxsplit=2)[1]


def remove_prefixes(x: Union[str, Iterable[str]]) -> List[str]:
    if isinstance(x, str):
        return [remove_prefix(x)]
    return [remove_prefix(i) for i in x]


def api_tt_obj_station(api_tt_obj: Any) -> model.StationID:
    if "odpt:arrivalStation" in api_tt_obj:
        return remove_prefix(api_tt_obj["odpt:arrivalStation"])
    else:
        return remove_prefix(api_tt_obj["odpt:departureStation"])


def api_time_to_int(api_time: str) -> int:
    h, m = map(int, api_time.split(":"))
    return h*3600 + m*60


def get_arr_dep_times(api_time: Mapping[str, Any]) -> Tuple[int, int]:
    arr_str = api_time.get("odpt:arrivalTime")
    dep_str = api_time.get("odpt:departureTime")

    if arr_str and dep_str:
        arr = api_time_to_int(arr_str)
        dep = api_time_to_int(dep_str)
    elif arr_str:
        arr = dep = api_time_to_int(arr_str)
    elif dep_str:
        arr = dep = api_time_to_int(dep_str)
    else:
        arr = dep = -1

    return arr, dep


class ApiSession:
    def __init__(self, addr: str, apikey: str) -> None:
        self.session = requests.Session()
        self.addr = addr
        self.apikey = apikey

    def get(self, endpoint: str) -> Iterator[Any]:
        url = urljoin(self.addr, urlquote(f"odpt:{endpoint}"))
        req = self.session.get(
            url,
            params={"acl:consumerKey": self.apikey},
            stream=True,
            timeout=API_TIMEOUT
        )

        req.raise_for_status()
        return IJsonIterator(req.raw)

    def close(self):
        self.session.close()


class ODPTProvider(model.Provider):
    def __init__(self) -> None:
        self._session: Optional[ApiSession] = None
        self._apikey: str = ""

    def _ensure_session(self):
        if self._session is None:
            self._session = ApiSession("https://api.odpt.org/api/v4/", self._apikey)

    def _valid_entry(self, agency: str, route: str = "") -> bool:
        ok = agency in self.provides
        if ok and agency == "JR-East" and route:
            ok = route in JREAST_ROUTES
        return ok

    @property
    def name(self) -> str:
        return "odpt"

    @property
    def provides(self) -> Set[model.AgencyID]:
        return {"Toei", "TokyoMetro", "TWR", "MIR", "YokohamaMunicipal", "TamaMonorail"}

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

    def trains(self) -> Iterable[model.Train]:
        self._ensure_session()
        assert self._session

        for api_train in self._session.get("TrainTimetable.json"):
            model_train = model.Train(
                id=remove_prefix(api_train["owl:sameAs"]),
                agency=remove_prefix(api_train["odpt:operator"]),
                route=remove_prefix(api_train["odpt:railway"]),
                calendar=remove_prefix(api_train["odpt:calendar"]),
                timetable=[],
                destinations=[],
            )

            # Check if we even care about such train
            if not self._valid_entry(model_train.agency, model_train.route):
                continue

            # Convert more fields
            model_train.destinations = \
                remove_prefixes(api_train.get("odpt:destinationStation", []))
            model_train.train_number = api_train["odpt:trainNumber"]

            if "odpt:originStation" in api_train:
                model_train.origins = remove_prefixes(api_train["odpt:originStation"])

            if "odpt:nextTrainTimetable" in api_train:
                model_train.next_timetable = remove_prefixes(api_train["odpt:nextTrainTimetable"])

            if "odpt:previousTrainTimetable" in api_train:
                model_train.previous_timetable = \
                    remove_prefixes(api_train["odpt:previousTrainTimetable"])

            if "odpt:trainType" in api_train:
                model_train.train_type = remove_prefix(api_train["odpt:trainType"])

            if "odpt:trainName" in api_train:
                model_train.train_name = model.Name(**api_train["odpt:trainName"])

            if "odpt:railDirection" in api_train:
                model_train.direction = remove_prefix(api_train["odpt:railDirection"])

            if "odpt:train" in api_train:
                model_train.realtime_id = remove_prefix(api_train["odpt:train"])

            # Convert the timetable
            prev_dep = 0

            for api_tt_obj in api_train["odpt:trainTimetableObject"]:
                sta = api_tt_obj_station(api_tt_obj)

                # FIXME: Handle invalid arrival and departure times
                arr, dep = get_arr_dep_times(api_tt_obj)

                # Fix midnight timetravel
                # FIXME: Also handle trains starting after midnight?
                if arr >= 0 and dep >= 0:
                    while arr < prev_dep:
                        arr += 86400
                    while dep < arr:
                        dep += 86400
                    prev_dep = dep

                model_train.timetable.append(model.TrainTimetableEntry(
                    sta, arr, dep, api_tt_obj.get("odpt:platformNumber", "")
                ))

            # Yield the object
            yield model_train

    def stations(self) -> Iterable[model.Station]:
        self._ensure_session()
        assert self._session

        for api_station in self._session.get("Station.json"):
            model_station = model.Station(
                id=remove_prefix(api_station["owl:sameAs"]),
                agency=remove_prefix(api_station["odpt:operator"]),
                route=remove_prefix(api_station["odpt:railway"]),
                code=api_station.get("odpt:stationCode"),
            )

            if self._valid_entry(model_station.agency, model_station.route):
                yield model_station

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

            if self._valid_entry(agency):
                yield model.Calendar(
                    id=calendar_id,
                    days=[datetime.strptime(i, "%Y-%m-%d").date()
                          for i in api_calendar["odpt:day"]]
                )

    def train_types(self) -> Iterable[model.TrainType]:
        self._ensure_session()
        assert self._session

        for api_train_type in self._session.get("TrainType.json"):
            agency = remove_prefix(api_train_type["odpt:operator"])
            if self._valid_entry(agency):
                yield model.TrainType(
                    id=remove_prefix(api_train_type["owl:sameAs"]),
                    name=model.Name(**api_train_type["odpt:trainTypeTitle"])
                )
