# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import csv
import io
from collections import defaultdict
from copy import copy
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from itertools import chain
from typing import Dict, Iterable, Mapping, Optional, Set

import requests

from ..const import (ATTRIBUTION_URL, DEFAULT_PUBLISHER_NAME,
                     DEFAULT_PUBLISHER_URL, DIR_CURATED, DIR_GTFS, FUTURE_DAYS, RAIL_GTFS_HEADERS)
from ..err import MissingLocalData
from ..util import text_color
from . import model


@dataclass
class RouteData:
    id: str
    agency: str
    code: str
    name: model.Name
    color: str
    type: str
    through_group: Optional[str] = None

    @classmethod
    def fromdict(cls, m: Mapping[str, str]) -> "RouteData":
        name = model.Name(m["name_ja"], m["name_en"])
        return cls(id=m["route"], agency=m["agency"], code=m["code"], name=name,
                   color=m["color"], type=m["type"], through_group=m["through_group"] or None)

    @classmethod
    def fromcsv(cls, f: Iterable[str]) -> Dict[str, "RouteData"]:
        return {
            r["route"]: RouteData.fromdict(r)
            for r in csv.DictReader(f)
            if r["has_geo"] == "x"
        }


class CalendarHandler:
    """An object which handles services, calendars and all that kind of stuff."""

    def __init__(self, start_date: date, end_date: Optional[date] = None):
        """Inits the CalendarHandler"""
        if end_date is None:
            end_date = start_date + timedelta(days=FUTURE_DAYS)

        self.start = start_date
        self.end = end_date

        self.used: defaultdict[model.RouteID, set[model.CalendarID]] = defaultdict(set)
        self.valid: set[model.CalendarID] = set()
        self.holidays: set[date] = set()
        self.special: defaultdict[date, set[model.CalendarID]] = defaultdict(set)

        self.built_ins_priority = {
            -1: ["Holiday", "SaturdayHoliday", "Everyday"],  # special key for public holidays
            0: ["Monday", "Weekday", "Everyday"],
            1: ["Tuesday", "Weekday", "Everyday"],
            2: ["Wednesday", "Weekday", "Everyday"],
            3: ["Thursday", "Weekday", "Everyday"],
            4: ["Friday", "Weekday", "Everyday"],
            5: ["Saturday", "SaturdayHoliday", "Everyday"],
            6: ["Sunday", "Holiday", "SaturdayHoliday", "Everyday"],
        }
        self.built_ins = {
            "Everyday", "Weekday", "SaturdayHoliday", "Holiday",
            "Sunday", "Monday", "Tuesday", "Wednesday",
            "Thursday", "Friday", "Saturday", "Sunday"
        }

    def load(self, providers: Iterable[model.Provider]) -> None:
        self.load_holidays()
        self.load_valid(
            chain.from_iterable(p.calendars() for p in providers)
        )

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

    def load_valid(self, calendars: Iterable[model.Calendar]) -> None:
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

    def use(self, route_id: str, calendar_id: str) -> Optional[str]:
        """Checks if this pair of route_id and calendar_id can be used.
        If yes, returns the service_id to be used in the GTFS.
        If no, returns None.
        """
        if calendar_id not in self.valid:
            return None

        self.used[route_id].add(calendar_id)
        return route_id + "." + calendar_id

    def export(self, exporter: model.Exporter):
        """Sends generated calendar_dates.txt rows to exporter"""
        for route_id, calendars_used in self.used.items():
            working_date = copy(self.start)

            while working_date <= self.end:
                active_services: list[model.CalendarID] = []
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

                    exporter.save({
                        "service_id": service_id,
                        "date": working_date.strftime("%Y%m%d"),
                        "exception_type": "1",
                    })

                working_date += timedelta(days=1)


def export_agencies(used_agencies: Set[str], translations: model.Exporter) -> None:
    # Read info about agencies
    with (DIR_CURATED / "agencies.csv").open(mode="r", encoding="utf-8", newline="") as f:
        agency_data = {i["id"]: i for i in csv.DictReader(f)}

    # Check if we have info on missing agencies
    missing_agencies = used_agencies.difference(agency_data)
    if missing_agencies:
        raise MissingLocalData(f"missing agency data for {', '.join(sorted(missing_agencies))}")

    # Output to GTFS
    with (DIR_GTFS / "agency.txt").open(mode="w", encoding="utf-8") as f:
        writer = csv.DictWriter(f, RAIL_GTFS_HEADERS["agency.txt"])
        writer.writeheader()

        for agency_id in sorted(used_agencies):
            data = agency_data[agency_id]

            writer.writerow({
                "agency_id": agency_id,
                "agency_name": data["name"],
                "agency_url": data["url"],
                "agency_timezone": "Asia/Tokyo",
                "agency_lang": "ja"
            }),

            translations.save({
                "table_name": "agency",
                "field_name": "agency_name",
                "record_id": agency_id,
                "language": "en",
                "translation": data["name_en"]
            })


def export_routes(used_routes: Set[model.RouteID], route_data: Mapping[model.RouteID, RouteData],
                  translations: model.Exporter) -> None:
    # Check if we have info on missing agencies
    missing_routes = used_routes.difference(route_data)
    if missing_routes:
        raise MissingLocalData(f"missing agency data for {', '.join(sorted(missing_routes))}")

    # Output to GTFS
    with (DIR_GTFS / "routes.txt").open(mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, RAIL_GTFS_HEADERS["routes.txt"])
        writer.writeheader()

        for route_id in sorted(used_routes):
            data = route_data[route_id]

            writer.writerow({
                "agency_id": data.agency,
                "route_id": route_id,
                "route_short_name": data.code,
                "route_long_name": data.name.ja,
                "route_type": data.type,
                "route_color": data.color,
                "route_text_color": text_color(data.color)
            }),

            translations.save({
                "table_name": "routes",
                "field_name": "route_long_name",
                "record_id": route_id,
                "language": "en",
                "translation": data.name.en
            })


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
