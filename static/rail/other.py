# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import csv
from dataclasses import dataclass
from typing import Dict, Iterable, Mapping, Optional, Set

from ..const import DIR_CURATED, DIR_GTFS, RAIL_GTFS_HEADERS
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
