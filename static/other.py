# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import csv
import io
import logging
from collections import defaultdict
from copy import copy
from datetime import date, datetime, timedelta
from typing import Any, Iterable, List, Mapping, Optional, Protocol, Set

import requests

from .const import (ATTRIBUTION_URL, DEFAULT_PUBLISHER_NAME,
                    DEFAULT_PUBLISHER_URL, DIR_GTFS, FUTURE_DAYS,
                    RAIL_GTFS_HEADERS, Color)

# Typing stuff. We don't use models, as those are either rail- or bus-specific.

class _WithSave(Protocol):
    def save(self, __thing: Mapping[str, Any]) -> Any: ...


class _CalendarLike(Protocol):
    @property
    def id(self) -> str: ...
    @property
    def days(self) -> Optional[List[date]]: ...


_CalendarID = str
_RouteID = str


# Implementations

class CalendarHandler:
    """An object which handles services, calendars and all that kind of stuff."""

    def __init__(self, start_date: date, end_date: Optional[date] = None):
        """Inits the CalendarHandler"""
        if end_date is None:
            end_date = start_date + timedelta(days=FUTURE_DAYS)

        self.start = start_date
        self.end = end_date

        self.logger = logging.getLogger("CalendarHandler")

        self.used: defaultdict[_RouteID, Set[_CalendarID]] = defaultdict(set)
        self.valid: Set[_CalendarID] = set()
        self.holidays: Set[date] = set()
        self.special: defaultdict[date, Set[_CalendarID]] = defaultdict(set)
        self.exported: Set[str] = set()

        self.built_ins_priority: List[List[_CalendarID]] = [
            ["Monday", "Weekday", "Everyday"],
            ["Tuesday", "Weekday", "Everyday"],
            ["Wednesday", "Weekday", "Everyday"],
            ["Thursday", "Weekday", "Everyday"],
            ["Friday", "Weekday", "Everyday"],
            ["Saturday", "SaturdayHoliday", "Everyday"],
            ["Sunday", "Holiday", "SaturdayHoliday", "Everyday"],
            ["Holiday", "SaturdayHoliday", "Everyday"],  # special key (-1) for public holidays
        ]
        self.built_ins: Set[_CalendarID] = {
            "Everyday", "Weekday", "SaturdayHoliday", "Holiday",
            "Sunday", "Monday", "Tuesday", "Wednesday",
            "Thursday", "Friday", "Saturday", "Sunday"
        }

    def load(self, calendars: Iterable[_CalendarLike]) -> None:
        self.load_holidays()
        self.load_valid(calendars)

    def load_holidays(self) -> None:
        """Loads Japan holidays into self.holidays.
        Data comes from Japan's Cabinet Office:
        https://www8.cao.go.jp/chosei/shukujitsu/gaiyou.html
        Only holidays within self.start and self.end are saved.
        """

        try:
            req = requests.get("https://www8.cao.go.jp/chosei/shukujitsu/syukujitsu.csv")
        except requests.exceptions.SSLError:
            req = requests.get("https://mkuran.pl/moovit/japan-cao-shukujitsu.csv")

        req.encoding = "shift-jis"
        buffer = io.StringIO(req.text)
        reader = csv.DictReader(buffer)

        for row in reader:
            date_str = row["国民の祝日・休日月日"]
            date_val = datetime.strptime(date_str, "%Y/%m/%d").date()

            if self.start <= date_val <= self.end:
                self.holidays.add(date_val)

        buffer.close()

    def load_valid(self, calendars: Iterable[_CalendarLike]) -> None:
        """Loads list of **usable** calendars into self.valid
        in order to ensure that each trips points to a
        service_id active on at least one day.

        If a calendar is not a built-in, add its dates to self.special.
        """
        for calendar in calendars:
            if calendar.id in self.built_ins:
                self.valid.add(calendar.id)

            elif calendar.days:
                days_in_range = (i for i in calendar.days if self.start <= i <= self.end)

                # Save dates of special calendars
                for day in days_in_range:
                    self.special[day].add(calendar.id)

                    # Add this special calendar to self.valid
                    self.valid.add(calendar.id)

            else:
                self.logger.warn(f"{Color.YELLOW}Calendar {calendar.id} is not built-in, "
                                 f"bus has no specific days{Color.RESET}")

    def use(self, route_id: _RouteID, calendar_id: _CalendarID) -> Optional[str]:
        """Checks if this pair of route_id and calendar_id can be used.
        If yes, returns the service_id to be used in the GTFS.
        If no, returns None.
        """
        if calendar_id not in self.valid:
            return None

        self.used[route_id].add(calendar_id)
        return route_id + "." + calendar_id

    def export(self, exporter: _WithSave):
        """Sends generated calendar_dates.txt rows to exporter"""
        for route_id, calendars_used in self.used.items():
            working_date = copy(self.start)

            while working_date <= self.end:
                active_services: list[_CalendarID] = []
                weekday = -1 if working_date in self.holidays else working_date.weekday()

                # Check if special calendars were used
                if working_date in self.special:
                    special_calendars = calendars_used.intersection(self.special[working_date])
                else:
                    special_calendars = None

                # If they were used - use them as active services,
                # otherwise check builtins
                if special_calendars:
                    active_services = list(special_calendars)
                else:
                    for potential_calendar in self.built_ins_priority[weekday]:
                        if potential_calendar in calendars_used:
                            active_services = [potential_calendar]
                            break

                for active_service in active_services:
                    service_id = route_id + "." + active_service
                    self.exported.add(service_id)

                    exporter.save({
                        "service_id": service_id,
                        "date": working_date.strftime("%Y%m%d"),
                        "exception_type": "1",
                    })

                working_date += timedelta(days=1)


def export_attribution(provider_attrs: Iterable[str]):
    with (DIR_GTFS / "attributions.txt").open(mode="w", encoding="utf-8", newline="") as f:
        wrtr = csv.DictWriter(f, RAIL_GTFS_HEADERS["attributions.txt"])
        wrtr.writeheader()

        wrtr.writerow({
            "organization_name": "TokyoRailGTFS script (written by Mikołaj Kuranowski)",
            "is_producer": "1", "is_authority": "0", "attribution_url": ATTRIBUTION_URL,
        })

        for provider_attr in provider_attrs:
            wrtr.writerow({
                "organization_name": provider_attr,
                "is_producer": "0", "is_authority": "1", "attribution_url": ATTRIBUTION_URL,
            })


def export_feedinfo(creation_date: datetime, publisher_name: str = "", publisher_url: str = ""):
    with (DIR_GTFS / "feed_info.txt").open(mode="w", encoding="utf-8", newline="") as f:
        wrtr = csv.DictWriter(f, RAIL_GTFS_HEADERS["feed_info.txt"])
        wrtr.writeheader()
        wrtr.writerow({
            "feed_publisher_name": publisher_name or DEFAULT_PUBLISHER_NAME,
            "feed_publisher_url": publisher_url or DEFAULT_PUBLISHER_URL,
            "feed_lang": "ja",
            "feed_version": creation_date.strftime("%Y%m%d_%H%M")
        })
