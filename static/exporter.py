# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import csv
import logging
from typing import Any, Iterable, Mapping

from .const import DIR_GTFS, RAIL_GTFS_HEADERS, Color


class SimpleExporter:
    def __init__(self, name: str) -> None:
        self.logger = logging.getLogger(f"Exporter.{name}")

        self.fname = name + ".txt"
        self.filepath = DIR_GTFS / self.fname

        self.logger.info("Opening file")
        self.fileobj = open(self.filepath, "w", encoding="utf-8", newline="")
        self.writer = csv.DictWriter(self.fileobj, RAIL_GTFS_HEADERS[self.fname])
        self.writer.writeheader()
        self.count = 0

    def __enter__(self) -> "SimpleExporter":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    def save(self, thing: Mapping[str, Any]) -> None:
        self.count += 1
        self.writer.writerow(thing)

    def save_many(self, things: Iterable[Mapping[str, Any]]) -> None:
        for thing in things:
            self.save(thing)

    def close(self) -> None:
        self.logger.info(f"Closing file - wrote {Color.BOLD}{self.count}{Color.RESET} rows")
        self.fileobj.close()
