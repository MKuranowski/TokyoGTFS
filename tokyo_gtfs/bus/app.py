# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import csv
import os
from argparse import Namespace
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from impuls import App, HTTPResource, LocalResource, Pipeline, PipelineOptions
from impuls.errors import DataError, MultipleDataErrors

from .insert_dummy_agencies import InsertDummyAgencies
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
                LoadRoutes(operators, "challenge_patterns.json"),
                LoadStops(operators, "challenge_stops.json"),
                LoadTimetables(operators, "challenge_timetables.json"),
                # TODO: LoadCalendars("challenge_calendars.json"),
                # TODO: RemoveEmptyCalendars(),
                # TODO: RemoveUnusedEntities(),
                # TODO: CurateAgencies("bus_operators.csv")
                # TODO: SaveGTFS(),
            ],
            resources={
                "bus_operators.csv": LocalResource("data/bus_operators.csv"),
                "challenge_calendars.json": HTTPResource.get(
                    "https://api-challenge2024.odpt.org/api/v4/odpt:Calendar.json",
                    params={"acl:consumerKey": key.challenge},
                ),
                "challenge_patterns.json": HTTPResource.get(
                    "https://api-challenge2024.odpt.org/api/v4/odpt:BusroutePattern.json",
                    params={"acl:consumerKey": key.challenge},
                ),
                "challenge_stops.json": HTTPResource.get(
                    "https://api-challenge2024.odpt.org/api/v4/odpt:BusstopPole.json",
                    params={"acl:consumerKey": key.challenge},
                ),
                "challenge_timetables.json": HTTPResource.get(
                    "https://api-challenge2024.odpt.org/api/v4/odpt:BusTimetable.json",
                    params={"acl:consumerKey": key.challenge},
                ),
            },
            options=options,
        )


@dataclass
class ApiKeys:
    odpt: str
    challenge: str

    @classmethod
    def load_all(cls) -> Self:
        return cls(
            *MultipleDataErrors.catch_all(
                "load_api_keys",
                map(cls.load_key, ("odpt", "challenge")),
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
