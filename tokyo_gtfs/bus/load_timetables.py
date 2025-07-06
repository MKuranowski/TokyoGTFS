# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Container
from typing import Any, Literal, NamedTuple, cast

from impuls import DBConnection, Task, TaskRuntime

from ..util import json_items, strip_prefix

MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR


class PatternInfo(NamedTuple):
    route_id: str
    direction: Literal[0, 1] | None


class LoadTimetables(Task):
    def __init__(self, operators: Container[str], *resources: str) -> None:
        super().__init__()
        self.operators = operators
        self.resources = resources

        self.pattern_info = dict[str, PatternInfo]()

    def execute(self, r: TaskRuntime) -> None:
        self.pattern_info = self.get_pattern_info(r.db)
        with r.db.transaction():
            for resource in self.resources:
                for obj in json_items(r.resources[resource].stored_at):
                    operator = strip_prefix(cast(str, obj["odpt:operator"]))
                    if operator not in self.operators:
                        continue

                    self.insert_timetable(r.db, obj)

    def get_pattern_info(self, db: DBConnection) -> dict[str, PatternInfo]:
        return {
            cast(str, i[0]): PatternInfo(cast(str, i[1]), cast(Literal[0, 1] | None, i[2]))
            for i in db.raw_execute("SELECT pattern_id, route_id, direction FROM patterns")
        }

    def insert_timetable(self, db: DBConnection, obj: Any) -> None:
        tt = obj["odpt:busTimetableObject"]
        if not tt:
            return

        pattern_id = strip_prefix(cast(str, obj["odpt:busroutePattern"]))
        route_id, direction = self.pattern_info[pattern_id]
        calendar_id = strip_prefix(cast(str, obj["odpt:calendar"]))
        trip_id = strip_prefix(cast(str, obj["owl:sameAs"]))
        headsign = cast(str, tt[0].get("odpt:destinationSign") or "")
        accessible = cast(bool | None, tt[0].get("odpt:isNonStepBus"))

        db.raw_execute("INSERT OR IGNORE INTO calendars (calendar_id) VALUES (?)", (calendar_id,))
        db.raw_execute(
            "INSERT INTO trips (trip_id, route_id, calendar_id, pattern_id, headsign, direction, "
            "wheelchair_accessible) VALUES (?,?,?,?,?,?,?)",
            (trip_id, route_id, calendar_id, pattern_id, headsign, direction, accessible),
        )

        tt.sort(key=lambda i: i["odpt:index"])  # type: ignore
        previous_dep = 0

        for idx, i in enumerate(tt):
            arr_str = cast(str, i.get("odpt:arrivalTime", ""))
            dep_str = cast(str, i.get("odpt:departureTime", ""))
            if not arr_str and not dep_str:
                self.logger.warning("Trip %s, index %d - no time data", trip_id, idx)
                continue

            stop_id = strip_prefix(cast(str, i["odpt:busstopPole"]))
            headsign = cast(str, i.get("odpt:destinationSign") or "")
            arr = _parse_time(arr_str or dep_str)
            dep = _parse_time(dep_str or arr_str)
            pick_up_type = 1 if i.get("odpt:canGetOn") is False else 0
            drop_off_type = 1 if i.get("odpt:canGetOff") is False else 0

            # Apply offset for night buses
            if arr < previous_dep or (idx == 0 and i.get("odpt:isMidnight") and arr < 6 * HOUR):
                arr += DAY
            if dep < arr:
                dep += DAY

            # Insert into DB
            db.raw_execute(
                "INSERT INTO stop_times (trip_id, stop_sequence, stop_id, arrival_time, "
                "departure_time, pickup_type, drop_off_type, stop_headsign) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (trip_id, idx, stop_id, arr, dep, pick_up_type, drop_off_type, headsign),
            )
            previous_dep = dep


def _parse_time(x: str) -> int:
    h, m = map(int, x.split(":"))
    return h * HOUR + m * MINUTE
