# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections import defaultdict
from dataclasses import dataclass, field
from itertools import chain
from logging import getLogger
from operator import attrgetter, itemgetter
from typing import Any, Dict, Iterable, List, Optional

from ...const import DIR_CURATED, Color
from ...err import InvalidGeoData
from ...util import last_part, load_csv_as_mapping
from .. import model
from .osm import OSMNode, get_all_stations


@dataclass
class IntermediateStation:
    """IntermediateStations is a dataclass oly used as an intermediate structure
    between OSMNode and GeoStation."""
    node_id: int
    name: str
    lat: float
    lon: float
    routes: List[model.RouteID]
    merged_all: bool = False


@dataclass
class GeoStation:
    """GeoStations is a structure used to represent full station structure."""
    id: model.StationID
    lat: float
    lon: float
    name: Optional[model.Name] = None
    code: Optional[str] = None
    parent: Optional["GeoStation"] = None
    children: List["GeoStation"] = field(default_factory=list)
    used: bool = False

    def as_gtfs(self) -> Dict[str, Any]:
        assert self.name
        return {
            "stop_id": self.id,
            "stop_code": self.code or "",
            "stop_name": self.name.ja,
            "stop_lat": self.lat,
            "stop_lon": self.lon,
            "location_type": "1" if self.children else "0",
            "parent_station": self.parent.id if self.parent else "",
        }

    def export(self, stops: model.Exporter, translations: model.Exporter) -> None:
        stops.save(self.as_gtfs())
        if self.name and self.name.en:
            translations.save({
                "table_name": "stops",
                "field_name": "stop_name",
                "record_id": self.id,
                "language": "en",
                "translation": self.name.en,
            })


class StationHandler:
    """StationHandler is an object used for handling and exporting station data."""
    def __init__(self) -> None:
        self.names: dict[str, model.Name] = load_csv_as_mapping(
            DIR_CURATED / "station_names.csv",
            itemgetter("sta_id"),
            lambda row: model.Name(row["name_ja"], row["name_en"])
        )
        self.by_id: dict[model.StationID, GeoStation] = {}
        self.valid = True
        self.logger = getLogger("StationHandler")

    def load(self, providers: Iterable[model.Provider], validate_prefix: str = "") -> int:
        """Loads station data from all available sources. Returns the number of valid stations."""
        self._load_stations(get_all_stations())
        valid_stations = self._add_data(chain.from_iterable(i.stations() for i in providers),
                                        validate_prefix)
        if not self.valid:
            raise InvalidGeoData()
        return valid_stations

    def _load_stations(self, nodes: List[OSMNode]) -> None:
        """Creates GeoStations from a list of OSMNodes.
        Those stations won't have names or codes."""
        # Process OSM nodes into intermediate stations
        grouped_stations: defaultdict[str, list[IntermediateStation]] = defaultdict(list)

        # Iterate thru nodes while popping them from the provided list
        # to allow used nodes to bne garbage collected.
        while nodes:
            node = nodes.pop()
            name_id = node.tags["name"]
            grouped_stations[name_id].append(IntermediateStation(
                node.id,
                name_id,
                node.lat,
                node.lon,
                [k for (k, v) in node.tags.items() if "." in k and v == "yes"],
                node.tags.get("merged") == "all",
            ))

        # Convert the intermediate representations to GeoStation
        # (again popping from grouped_stations to allow intermediate representation to be gc-ed)
        while grouped_stations:
            name_id, stations = grouped_stations.popitem()
            merged_all_node = get_merged_all_node(stations)

            if len(stations) == 1 and len(stations[0].routes) == 1:
                # Case 1 - one station and one line.
                sta = stations[0]
                sta_id = sta.routes[0] + "." + name_id
                self.by_id[sta_id] = GeoStation(sta_id, sta.lat, sta.lon)

            elif len(stations) == 1:
                # Case 2 - one station and multiple lines.
                # Simple parent-children structure, all in one location.
                sta = stations[0]
                parent = GeoStation("Merged." + name_id, sta.lat, sta.lon)
                self.by_id[parent.id] = parent

                for route in sta.routes:
                    child = GeoStation(route + "." + name_id, sta.lat, sta.lon, parent=parent)
                    self.by_id[child.id] = child
                    parent.children.append(child)

            elif merged_all_node:
                # Case 3: many nodes, but all under one parent
                parent = GeoStation("Merged." + name_id, merged_all_node.lat, merged_all_node.lon)
                self.by_id[parent.id] = parent

                for ista in stations:
                    for route in ista.routes:
                        child = GeoStation(route + "." + name_id, ista.lat, ista.lon,
                                           parent=parent)
                        self.by_id[child.id] = child
                        parent.children.append(child)

            else:
                # Case 4: many nodes, no parent-of-all
                needs_merged_no = count_multiple_routes(stations) > 1
                merged_no = 1

                for sta in stations:
                    if len(sta.routes) == 1:
                        # Case 4.1 - single line - behavior as in case 1
                        sta_id = sta.routes[0] + "." + name_id
                        self.by_id[sta_id] = GeoStation(sta_id, sta.lat, sta.lon)

                    else:
                        # Case 4.2 - multiple lines - behavior as in case 2
                        parent_prefix = "Merged."
                        if needs_merged_no:
                            parent_prefix = f"Merged.{merged_no}."
                            merged_no += 1

                        parent = GeoStation(parent_prefix + name_id, sta.lat, sta.lon)
                        self.by_id[parent.id] = parent

                        for route in sta.routes:
                            child = GeoStation(route + "." + name_id, sta.lat, sta.lon,
                                               parent=parent)
                            self.by_id[child.id] = child
                            parent.children.append(child)

    def _add_data(self, model_stations: Iterable[model.Station],
                  validate_prefix: str = "") -> int:
        """Adds additional data to already-loaded GeoStations and does a little bit of validation.
        Data comes from model stations and curated CSV files."""
        valid_station_count = 0
        jreast_merged_codes: dict[model.StationID, str] = load_csv_as_mapping(
            DIR_CURATED / "jreast_merged_codes.csv",
            itemgetter("sta_id"),
            itemgetter("code")
        )

        # Add data from model stations
        for model_sta in model_stations:
            is_invalid = False
            should_validate = model_sta.id.startswith(validate_prefix)

            # Find a matching geo_sta
            geo_sta = self.by_id.get(model_sta.id)
            if not geo_sta:
                if should_validate:
                    self.logger.critical(f"{Color.RED}geo.osm is missing station "
                                         f"{Color.MAGENTA}{model_sta.id}{Color.RESET}")
                    self.valid = False
                continue

            # Find a name
            name_id = last_part(geo_sta.id)
            geo_sta.name = self.names.get(name_id)
            if geo_sta.name is None and should_validate:
                self.logger.critical(f"{Color.RED}sta_names.csv is missing name for "
                                     f"{Color.MAGENTA}{name_id}{Color.RESET}")
                is_invalid = True

            # Copy stop_code
            geo_sta.code = model_sta.code

            # Check if station was valid
            if is_invalid:
                self.valid = False
            elif should_validate:
                valid_station_count += 1

        # Generate codes and names for mother stations
        for sta in self.by_id.values():
            if not sta.children:
                continue

            name_id = last_part(sta.id)
            sta.name = self.names.get(name_id)
            if not sta.name:
                self.logger.critical(f"{Color.RED}sta_names.csv is missing name for "
                                     f"{Color.MAGENTA}{name_id}{Color.RESET}")
                is_invalid = True

            # Get children codes
            children_codes = []
            jreast_merged_code = jreast_merged_codes.get(sta.id)
            if jreast_merged_code:
                children_codes.append(jreast_merged_code)

            for child in sta.children:
                # Ignore JR-East child codes if there's a JR-East merged code
                if child.id.startswith("JR-East") and jreast_merged_code:
                    continue
                elif child.code:
                    children_codes.append(child.code)

            sta.code = "/".join(children_codes)

        return valid_station_count

    def mark_used(self, id: model.StationID) -> None:
        """Marks a station (and its parent) as used, to export it correctly."""
        sta = self.by_id[id]
        sta.used = True
        if sta.parent:
            sta.parent.used = True

    def export(self, exporter: model.Exporter, translations: model.Exporter) -> None:
        """Sends generated stops.txt rows to exporter"""

        for station in filter(attrgetter("used"), self.by_id.values()):
            station.export(exporter, translations)


def get_merged_all_node(lst: List[IntermediateStation]) -> Optional[IntermediateStation]:
    """Tries to find an IntermediateStation marked as 'merged=all'"""
    return next(filter(attrgetter("merged_all"), lst), None)


def count_multiple_routes(lst: List[IntermediateStation]) -> int:
    """Counts how many IntermediateStations have more then one line attached"""
    return sum(1 for i in filter(lambda i: len(i.routes) > 1, lst))
