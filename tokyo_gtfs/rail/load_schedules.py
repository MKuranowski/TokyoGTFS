# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import fnmatch
from collections import defaultdict
from collections.abc import Container, Generator, Iterable
from statistics import mean
from typing import Any, cast
from zipfile import ZipFile

import holidays
import ijson  # type: ignore
from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Agency, Calendar, Date, Route, Stop, StopTime, TimePoint, Translation, Trip
from impuls.tools.strings import find_non_conflicting_id
from impuls.tools.temporal import date_range

from ..util import compact_json, pack_list

ZIP_FILE_PREFIX = "mini-tokyo-3d-master/data"
PREVIOUS_DAY_CUTOFF = TimePoint(hours=3)
DAY = 86_400

VALID_CALENDARS = {"Weekday", "Saturday", "SaturdayHoliday", "Holiday"}


class LoadSchedules(Task):
    def __init__(self, start_date: Date | None = None) -> None:
        super().__init__()
        self.start_date = start_date or Date.today()
        self.end_date = self.start_date.add_days(365)
        self.direction_map = dict[str, dict[str, Trip.Direction]]()

    def execute(self, r: TaskRuntime) -> None:
        self.clear()
        self.extend_schema(r.db)
        with (
            ZipFile(r.resources["mini-tokyo-3d.zip"].stored_at, "r") as zip,
            r.db.transaction(),
        ):
            self.create_calendars_and_exceptions(r.db)

            self.load_railways(r.db, zip)
            assert self.direction_map["JR-East.Yamanote"] == {
                "OuterLoop": Trip.Direction.OUTBOUND,
                "InnerLoop": Trip.Direction.INBOUND,
            }  # Ensure that Yamanote line directions are consistent for headsign fixing

            self.load_stations(r.db, zip)
            self.load_station_groups(r.db, zip)
            self.load_train_types(r.db, zip)
            self.load_train_timetables(r.db, zip)

    def clear(self) -> None:
        self.direction_map.clear()

    def extend_schema(self, db: DBConnection) -> None:
        db.raw_execute(
            """CREATE TABLE train_types (
                train_type_id TEXT PRIMARY KEY,
                name_ja TEXT,
                name_en TEXT,
                name_ko TEXT,
                name_zh_hans TEXT,
                name_zh_hant TEXT
            ) STRICT;"""
        )

    def create_calendars_and_exceptions(self, db: DBConnection) -> None:
        self.create_calendars(db)
        self.create_calendar_exceptions(db)

    def create_calendars(self, db: DBConnection) -> None:
        db.create_many(
            Calendar,
            [
                Calendar(
                    id="Weekday",
                    monday=True,
                    tuesday=True,
                    wednesday=True,
                    thursday=True,
                    friday=True,
                    start_date=self.start_date,
                    end_date=self.end_date,
                ),
                Calendar(
                    id="Saturday",
                    saturday=True,
                    start_date=self.start_date,
                    end_date=self.end_date,
                ),
                Calendar(
                    id="SaturdayHoliday",
                    saturday=True,
                    sunday=True,
                    start_date=self.start_date,
                    end_date=self.end_date,
                ),
                Calendar(
                    id="Holiday",
                    sunday=True,
                    start_date=self.start_date,
                    end_date=self.end_date,
                ),
            ],
        )

    def create_calendar_exceptions(self, db: DBConnection) -> None:
        japan_holidays = cast(Container[Date], holidays.JP())  # type: ignore
        for day in date_range(self.start_date, self.end_date):
            if day not in japan_holidays:
                continue  # No changes outside of holidays
            elif day.weekday() == 6:
                continue  # No changes for holidays which fall on Sundays
            elif day.weekday() == 5:
                removed = ["Saturday"]  # Weekday is already inactive on a Saturday
                added = ["Holiday"]  # SaturdayHoliday is already active on a Saturday
            else:
                removed = ["Weekday"]  # Saturday is already inactive on a weekday
                added = ["SaturdayHoliday", "Holiday"]

            day_str = day.isoformat()
            db.raw_execute_many(
                "INSERT INTO calendar_exceptions (calendar_id,date,exception_type) VALUES (?,?,2)",
                ((id, day_str) for id in removed),
            )
            db.raw_execute_many(
                "INSERT INTO calendar_exceptions (calendar_id,date,exception_type) VALUES (?,?,1)",
                ((id, day_str) for id in added),
            )

    def load_railways(self, db: DBConnection, zip: ZipFile) -> None:
        self.logger.debug("Loading railways.json")

        created_agencies = set[str]()

        for i in _json_items_from_zip(zip, f"{ZIP_FILE_PREFIX}/railways.json"):
            route_id = cast(str, i["id"])
            agency_id = route_id.partition(".")[0]

            if agency_id not in created_agencies:
                db.create(Agency(agency_id, agency_id, "https://example.com", "Asia/Tokyo"))
                created_agencies.add(agency_id)

            db.create(Route(route_id, agency_id, "", route_id, Route.Type.RAIL))
            self.direction_map[route_id] = {
                i["ascending"]: Trip.Direction.OUTBOUND,
                i["descending"]: Trip.Direction.INBOUND,
            }

    def load_stations(self, db: DBConnection, zip: ZipFile) -> None:
        self.logger.debug("Loading stations.json")
        for i in _json_items_from_zip(zip, f"{ZIP_FILE_PREFIX}/stations.json"):
            stop_id = cast(str, i["id"])
            if c := i.get("coord"):
                lat = cast(float, c[1])
                lon = cast(float, c[0])
            else:
                self.logger.warning("Stop %s has no coordinates", stop_id)
                lat = 0.0
                lon = 0.0
            name_ja = cast(str, i["title"]["ja"])
            name_en = cast(str, i["title"]["en"])
            name_ko = cast(str, i["title"].get("ko", ""))
            name_zh_hans = cast(str, i["title"].get("zh-Hans", ""))
            name_zh_hant = cast(str, i["title"].get("zh-Hant", ""))
            name = f"{name_ja} {name_en}"

            db.create(Stop(stop_id, name, lat, lon))
            db.create(Translation("stops", "stop_name", "ja", name_ja, stop_id))
            db.create(Translation("stops", "stop_name", "en", name_en, stop_id))
            if name_ko:
                db.create(Translation("stops", "stop_name", "ko", name_ko, stop_id))
            if name_zh_hans:
                db.create(Translation("stops", "stop_name", "zh-Hans", name_zh_hans, stop_id))
            if name_zh_hant:
                db.create(Translation("stops", "stop_name", "zh-Hant", name_zh_hant, stop_id))

    def load_station_groups(self, db: DBConnection, zip: ZipFile) -> None:
        self.logger.debug("Loading station-groups.json")

        used_station_ids = set[str]()
        for group in _json_items_from_zip(zip, f"{ZIP_FILE_PREFIX}/station-groups.json"):
            fixed_sub_groups = self._fix_stop_sub_groups(group)
            for stem, child_ids in fixed_sub_groups.items():
                # Don't create a station if it's just one stop
                if len(child_ids) <= 1:
                    continue

                # Generate a unique ID for the station
                station_id = find_non_conflicting_id(used_station_ids, stem, ".")
                used_station_ids.add(station_id)

                # Insert the station, copy translations and set parent_station on child stops
                name, lat, lon = self._calculate_station_data(db, child_ids)
                db.create(Stop(station_id, name, lat, lon, location_type=Stop.LocationType.STATION))
                self._copy_stop_translations(db, station_id, child_ids[0])
                self._set_parent_station(db, station_id, child_ids)

    @staticmethod
    def _fix_stop_sub_groups(group: list[list[str]]) -> defaultdict[str, list[str]]:
        by_stem = defaultdict[str, list[str]](list)
        for sub_group in group:
            for id in sub_group:
                stem = _stop_id_stem(id)
                by_stem[stem].append(id)
        return by_stem

    @staticmethod
    def _calculate_station_data(
        db: DBConnection,
        stop_ids: Iterable[str],
    ) -> tuple[str, float, float]:
        # cspell: words lons
        names = list[str]()
        lats = list[float]()
        lons = list[float]()

        for id in stop_ids:
            s = db.raw_execute("SELECT name, lat, lon FROM stops WHERE stop_id = ?", (id,))
            name, lat, lon = cast(
                tuple[str, float, float], s.one_must(f"invalid stop in group definition: {id}")
            )
            names.append(name)
            if lat != 0.0:
                lats.append(lat)
            if lon != 0.0:
                lons.append(lon)

        return names[0], mean(lats), mean(lons)

    @staticmethod
    def _copy_stop_translations(db: DBConnection, dst: str, src: str) -> None:
        db.raw_execute(
            "INSERT INTO translations (table_name, field_name, language, translation, record_id) "
            "SELECT table_name, field_name, language, translation, ? "
            "FROM translations "
            "WHERE table_name = 'stops' AND record_id = ?",
            (dst, src),
        )

    @staticmethod
    def _set_parent_station(db: DBConnection, station_id: str, stop_ids: Iterable[str]) -> None:
        db.raw_execute_many(
            "UPDATE stops SET parent_station = ? WHERE stop_id = ?",
            ((station_id, id) for id in stop_ids),
        )

    def load_train_types(self, db: DBConnection, zip: ZipFile) -> None:
        self.logger.debug("Loading train-types.json")
        db.raw_execute_many(
            "INSERT INTO train_types (train_type_id, name_ja, name_en, name_ko, name_zh_hans, "
            "name_zh_hant) VALUES (?, ?, ?, ?, ?, ?)",
            (
                (
                    i["id"],
                    i["title"]["ja"],
                    i["title"]["en"],
                    i["title"].get("ko", ""),
                    i["title"].get("zh-Hans", ""),
                    i["title"].get("zh-Hant", ""),
                )
                for i in _json_items_from_zip(zip, f"{ZIP_FILE_PREFIX}/train-types.json")
            ),
        )

    def load_train_timetables(self, db: DBConnection, zip: ZipFile) -> None:
        for filename in fnmatch.filter(
            zip.namelist(),
            f"{ZIP_FILE_PREFIX}/train-timetables/*.json",
        ):
            self.logger.debug("Loading train-timetables/%s", filename.rpartition("/")[2])
            self.load_train_timetable(db, _json_items_from_zip(zip, filename))

    def load_train_timetable(self, db: DBConnection, data: Iterable[Any]) -> None:
        for i in data:
            # Extract basic data
            trip_id = cast(str, i["id"])
            calendar_id = self.get_valid_calendar_for_trip(trip_id)
            if not calendar_id:
                continue

            # Extract the name
            number = i.get("n", "")
            if names := i.get("nm"):
                if len(names) != 1:
                    # TODO: Remember both names, this may be important for joining/splitting trains
                    self.logger.warning("Trip %s has multiple names", trip_id)
                short_name = _prepend_with_number(number, f"{names[0]['ja']} {names[0]['en']}")
            else:
                short_name = number

            # Extract the direction
            route_id = cast(str, i["r"])
            direction = self.direction_map[route_id].get(i["d"])
            if direction is None:
                self.logger.warning("Trip %s uses an unknown direction: %s", trip_id, i["d"])

            # Create a trip
            db.create(
                Trip(
                    id=trip_id,
                    route_id=route_id,
                    calendar_id=calendar_id,
                    short_name=short_name,
                    direction=direction,
                    exceptional=False,
                    extra_fields_json=compact_json(
                        {
                            "train_type": i["y"],
                            "realtime_trip_id": i.get("t", ""),
                            "vehicle_kind": i.get("v", ""),
                            "destinations": pack_list(i.get("ds", [])),
                            "previous": pack_list(i.get("pt", [])),
                            "next": pack_list(i.get("nt", [])),
                        }
                    ),
                )
            )

            # Generate short_name translations
            if names := i.get("nm"):
                for lang in ("ja", "en", "ko", "zh-Hans", "zh-Hant"):
                    t_name = names[0].get(lang)
                    if t_name:
                        t_short_name = _prepend_with_number(number, t_name)
                        db.create(
                            Translation("trips", "trip_short_name", lang, t_short_name, trip_id)
                        )

            # Create stop_times
            previous_dep = TimePoint()
            for idx, j in enumerate(i["tt"]):
                arr_s = j.get("a")
                dep_s = j.get("d")
                if not arr_s and not dep_s:
                    self.logger.warning(
                        "Trip %s has a stop_time %d without any time (%r)",
                        trip_id,
                        idx,
                        j,
                    )
                    continue

                arr = _parse_time(arr_s or dep_s)
                dep = _parse_time(dep_s or arr_s)

                if arr < PREVIOUS_DAY_CUTOFF or arr < previous_dep:
                    arr = _add_24h(arr)
                if dep < arr:
                    dep = _add_24h(dep)

                db.create(StopTime(trip_id, j["s"], idx, arr, dep))

    def get_valid_calendar_for_trip(self, trip_id: str) -> str | None:
        parts = trip_id.split(".")
        if parts[-1] in VALID_CALENDARS:
            return parts[-1]
        elif parts[-2] in VALID_CALENDARS:
            return parts[-2]
        else:
            self.logger.warning(
                "Trip %s: neither %s nor %s are valid calendars",
                trip_id,
                parts[-1],
                parts[-2],
            )
            return None


def _json_items_from_zip(zip: ZipFile, filename: str) -> Generator[Any, None, None]:
    with zip.open(filename, "r") as f:
        yield from ijson.items(f, "item", use_float=True)


def _prepend_with_number(number: str, name: str) -> str:
    return f"{number} {name}" if number else name


def _parse_time(x: str) -> TimePoint:
    h, m = map(int, x.split(":"))
    return TimePoint(hours=h, minutes=m)


def _add_24h(x: TimePoint) -> TimePoint:
    return TimePoint(seconds=x.total_seconds() + DAY)


def _stop_id_stem(stop_id: str) -> str:
    """Returns the "stem" of a stop_id, roughly identifying the station.

    >>> _stop_id_stem("Toei.Oedo.Tochomae")
    'Tochomae'
    >>> _stop_id_stem("Toei.Oedo.Tochomae.1")
    'Tochomae'
    >>> _stop_id_stem("Tochomae")
    'Tochomae'
    """
    parts = stop_id.split(".")
    if len(parts) >= 2 and parts[-1] in {"0", "1", "2"}:
        return parts[-2]
    return parts[-1]
