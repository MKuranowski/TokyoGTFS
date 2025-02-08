# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Container
from typing import cast

from impuls import Task, TaskRuntime

from .util import json_items, strip_prefix


class LoadRoutes(Task):
    def __init__(self, operators: Container[str], *resources: str) -> None:
        super().__init__()
        self.operators = operators
        self.resources = resources

    def execute(self, r: TaskRuntime) -> None:
        r.db.raw_execute(
            "CREATE TABLE patterns ("
            " pattern_id TEXT PRIMARY KEY,"
            " route_id TEXT NOT NULL REFERENCES routes(route_id),"
            " description TEXT,"
            " direction INTEGER CHECK (direction IN (0, 1))"
            ") STRICT;"
        )
        r.db.raw_execute(
            "ALTER TABLE trips ADD COLUMN pattern_id TEXT REFERENCES patterns(pattern_id)"
        )

        with r.db.transaction():
            for resource in self.resources:
                for obj in json_items(r.resources[resource].stored_at):
                    operator = strip_prefix(cast(str, obj["odpt:operator"]))
                    if operator not in self.operators:
                        continue

                    route_id = strip_prefix(cast(str, obj["odpt:busroute"]))
                    pattern_id = strip_prefix(cast(str, obj["owl:sameAs"]))
                    if title := cast(str | None, obj.get("dc:title")):
                        route_name, _, pattern_desc = title.partition(" ")
                    else:
                        route_name, pattern_desc = "", ""

                    # Extract pattern description from odpt:note, if that was not present in title
                    if not pattern_desc and "odpt:note" in obj:
                        note = cast(str, obj["odpt:note"])
                        note_parts = note.split(":")
                        if len(note_parts) >= 2:
                            pattern_desc = note_parts[1]

                    # Extract inbound/outbound directions
                    match obj.get("odpt:direction"):
                        case "0":
                            direction = 0
                        case "1":
                            direction = 1
                        case _:
                            direction = None

                    # Insert into DB
                    r.db.raw_execute(
                        "INSERT OR IGNORE INTO routes (agency_id, route_id, short_name, "
                        "long_name, type) VALUES (?, ?, ?, '', 3)",
                        (operator, route_id, route_name),
                    )
                    r.db.raw_execute(
                        "INSERT INTO patterns (pattern_id, route_id, description, "
                        "direction) VALUES (?,?,?,?)",
                        (pattern_id, route_id, pattern_desc, direction),
                    )
