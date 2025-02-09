# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Container, Mapping
from typing import cast

import holidays
from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Date
from impuls.tools.temporal import BoundedDateRange

from ..util import json_items, strip_prefix

KNOWN_CALENDARS = {
    "Everyday": 0b111_1111,
    "Weekday": 0b001_1111,
    "SaturdayHoliday": 0b110_0000,
    "Monday": 0b000_0001,
    "Tuesday": 0b000_0010,
    "Wednesday": 0b000_0100,
    "Thursday": 0b000_1000,
    "Friday": 0b001_0000,
    "Saturday": 0b010_0000,
    "Sunday": 0b100_0000,
    "Holiday": 0b100_0000,
}


class LoadCalendars(Task):
    def __init__(
        self,
        *resources: str,
        start_date: Date | None = None,
        end_date: Date | None = None,
    ) -> None:
        super().__init__()
        start_date = start_date or Date.today()
        end_date = end_date or (Date.fromordinal(start_date.toordinal() + 365))

        self.resources = resources
        self.range = BoundedDateRange(start_date, end_date)
        self.holidays = cast(Container[Date], holidays.JP())  # type: ignore

    def execute(self, r: TaskRuntime) -> None:
        used_calendars = self.get_used_calendars(r.db)
        with r.db.transaction():
            self.merge_sunday_into_holiday(r.db, used_calendars)
            specific = self.load_specific_calendar_dates(r, used_calendars)
            for calendar in used_calendars:
                self.insert_dates_of(r.db, calendar, specific)

    def get_used_calendars(self, db: DBConnection) -> set[str]:
        return {cast(str, i[0]) for i in db.raw_execute("SELECT calendar_id FROM calendars")}

    def merge_sunday_into_holiday(self, db: DBConnection, used_calendars: set[str]) -> None:
        # Merge "Sunday" into "Holiday"
        # Ideally we'd consider every route and, if any uses both, we'd use
        # "Sunday" for non-holiday Sundays and "Holiday" for public Holidays only;
        # but for now we just assign the same set of dates for both.
        if "Sunday" in used_calendars and "Holiday" in used_calendars:
            db.raw_execute("UPDATE trips SET calendar_id = 'Holiday' WHERE calendar_id = 'Sunday'")
            db.raw_execute("DELETE FROM calendars WHERE calendar_id = 'Sunday'")
            used_calendars.discard("Sunday")
        elif "Sunday" in used_calendars:
            db.raw_execute(
                "UPDATE calendars SET calendar_id = 'Holiday' WHERE calendar_id = 'Sunday'"
            )
            used_calendars.add("Holiday")
            used_calendars.discard("Sunday")

    def load_specific_calendar_dates(
        self,
        r: TaskRuntime,
        calendars: Container[str],
    ) -> dict[str, list[Date]]:
        dates_by_calendar = dict[str, list[Date]]()

        for resource in self.resources:
            for obj in json_items(r.resources[resource].stored_at):
                id = strip_prefix(obj["owl:sameAs"])
                if id in KNOWN_CALENDARS or id not in calendars:
                    continue

                dates = cast(list[str], obj.get("odpt:day") or [])
                if not dates:
                    self.logger.warning("Calendar %s has no dates", id)

                dates_by_calendar[id] = [
                    d for i in dates if (d := Date.from_ymd_str(i)) in self.range
                ]

        return dates_by_calendar

    def insert_dates_of(
        self,
        db: DBConnection,
        calendar: str,
        specific: Mapping[str, list[Date]],
    ) -> None:
        db.raw_execute_many(
            "INSERT INTO calendar_exceptions (calendar_id, date, exception_type) VALUES (?, ?, 1)",
            ((calendar, date.isoformat()) for date in self.get_dates_of(calendar, specific)),
        )

    def get_dates_of(self, calendar: str, specific: Mapping[str, list[Date]]) -> list[Date]:
        if d := specific.get(calendar):
            return d
        weekdays = KNOWN_CALENDARS[calendar]
        return [d for d in self.range if (1 << self.effective_weekday_of(d)) & weekdays]

    def effective_weekday_of(self, date: Date) -> int:
        return 6 if date in self.holidays else date.weekday()
