# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import csv
import os
from argparse import Namespace
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Self

from impuls import App, HTTPResource, LocalResource, Pipeline, PipelineOptions
from impuls.errors import DataError, MultipleDataErrors
from impuls.model import Attribution, FeedInfo
from impuls.tasks import AddEntity, RemoveUnusedEntities, SaveGTFS

from .curate_agencies import CurateAgencies
from .gtfs import GTFS_HEADERS
from .insert_dummy_agencies import InsertDummyAgencies
from .load_calendars import LoadCalendars
from .load_routes import LoadRoutes
from .load_stops import LoadStops
from .load_timetables import LoadTimetables


class TokyoBusGTFS(App):
    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        key = ApiKeys.load_all()
        operators = load_valid_operators()
        return Pipeline(
            tasks=[
                InsertDummyAgencies(operators),
                LoadRoutes(operators, "bus_patterns.json"),
                LoadStops(operators, "bus_stops.json"),
                LoadTimetables(operators, "bus_timetables.json"),
                LoadCalendars("bus_calendars.json"),
                RemoveUnusedEntities(),
                CurateAgencies("bus_operators.csv"),
                AddEntity(
                    task_name="AddAttribution1",
                    entity=Attribution(
                        id="1",
                        organization_name=(
                            "Schedules: Public Transportation Open Data Center "
                            "(accuracy and integrity of data is not guaranteed; "
                            "do not contact the ODPT or operators regarding this dataset)"
                        ),
                        is_producer=False,
                        is_operator=False,
                        is_authority=True,
                        is_data_source=True,
                        url="https://developer.odpt.org/terms/data_basic_license.html",
                    ),
                ),
                AddEntity(
                    task_name="AddAttribution2",
                    entity=Attribution(
                        id="2",
                        organization_name="GTFS: Mikołaj Kuranowski",
                        is_producer=True,
                        is_operator=False,
                        is_authority=True,
                        is_data_source=True,
                        url="https://github.com/MKuranowski/TokyoGTFS",
                    ),
                ),
                AddEntity(
                    task_name="AddFeedInfo",
                    entity=FeedInfo(
                        publisher_name="Mikołaj Kuranowski",
                        publisher_url="https://mkuran.pl/gtfs/",
                        lang="ja",
                    ),
                ),
                SaveGTFS(GTFS_HEADERS, "tokyo_missing_bus.zip", ensure_order=True),
            ],
            resources={
                "bus_operators.csv": LocalResource("data/bus_operators.csv"),
                "bus_calendars.json": HTTPResource.get(
                    "https://api.odpt.org/api/v4/odpt:Calendar.json",
                    params={"acl:consumerKey": key.odpt},
                ),
                "bus_patterns.json": HTTPResource.get(
                    "https://api.odpt.org/api/v4/odpt:BusroutePattern.json",
                    params={"acl:consumerKey": key.odpt},
                ),
                "bus_stops.json": HTTPResource.get(
                    "https://api.odpt.org/api/v4/odpt:BusstopPole.json",
                    params={"acl:consumerKey": key.odpt},
                ),
                "bus_timetables.json": HTTPResource.get(
                    "https://api.odpt.org/api/v4/odpt:BusTimetable.json",
                    params={"acl:consumerKey": key.odpt},
                ),
            },
            options=options,
        )


@dataclass
class ApiKeys:
    odpt: str
    # challenge: str

    @classmethod
    def load_all(cls) -> Self:
        keys = (field.name for field in fields(cls))
        return cls(
            *MultipleDataErrors.catch_all(
                "load_api_keys",
                map(cls.load_key, keys),
            )
        )

    @staticmethod
    def load_key(name: str) -> str:
        env_key = f"TOKYO_{name.upper()}_APIKEY"
        if key := os.getenv(env_key):
            return key

        env_file_key = f"{env_key}_FILE"
        if filename := os.getenv(env_file_key):
            return Path(filename).read_text(encoding="utf-8").strip()

        raise DataError(f"Missing {name} apikey; set the {env_key} environment variable")


def load_valid_operators() -> frozenset[str]:
    with open("data/bus_operators.csv", mode="r", encoding="utf-8", newline="") as f:
        return frozenset(i["id"] for i in csv.DictReader(f) if i["enabled"] == "1")
