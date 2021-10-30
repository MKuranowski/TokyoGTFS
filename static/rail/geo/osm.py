# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Mapping
from xml.sax import parse as sax_parse
from xml.sax.handler import ContentHandler as SAXContentHandler

from ...const import DIR_CURATED

DEFAULT_GEO_PATH = DIR_CURATED / "rail_geo.osm"


@dataclass
class OSMNode:
    """OSMNode is a dataclass used to represend osm <node> elements"""
    id: int
    lat: float
    lon: float
    tags: Dict[str, str] = field(default_factory=dict)


class OSMStationHandler(SAXContentHandler):
    """OSMStationHandler is an implementation of a SAX ContentHandler
    that only cares about OSM nodes with the railway=station tag."""
    def __init__(self) -> None:
        super().__init__()
        self.node = OSMNode(0, 0.0, 0.0)
        self.in_node = False
        self.stations: list[OSMNode] = []

    def startElement(self, name: str, attrs: Mapping[str, str]):
        if name == "node":
            self.node = OSMNode(int(attrs["id"]), float(attrs["lat"]), float(attrs["lon"]))
            self.in_node = True
        elif name == "tag" and self.in_node:
            self.node.tags[attrs["k"]] = attrs["v"]

    def endElement(self, name: str):
        if name == "node" and self.node.tags.get("railway") == "station":
            self.stations.append(self.node)
            self.in_node = False


def get_all_stations(file: Path = DEFAULT_GEO_PATH) -> List[OSMNode]:
    with file.open("rb") as stream:
        handler = OSMStationHandler()
        sax_parse(stream, handler)
        return handler.stations
