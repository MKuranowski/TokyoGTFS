# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Container
from typing import cast

from impuls import Task, TaskRuntime

from .util import json_items, strip_prefix


class LoadStops(Task):
    def __init__(self, operators: Container[str], *resources: str) -> None:
        super().__init__()
        self.operators = operators
        self.resources = resources

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            for resource in self.resources:
                for obj in json_items(r.resources[resource].stored_at):
                    # Ignore stops from ignored operators
                    operators = [strip_prefix(i) for i in cast(list[str], obj["odpt:operator"])]
                    if all(i not in self.operators for i in operators):
                        continue

                    stop_id = strip_prefix(cast(str, obj["owl:sameAs"]))
                    name = cast(str, obj["dc:title"] or "")
                    code = cast(str, obj.get("odpt:busstopPoleNumber") or "")
                    lat = cast(float, obj.get("geo:lat") or 0.0)
                    lon = cast(float, obj.get("geo:long") or 0.0)

                    if lat == 0.0 or lon == 0.0:
                        self.logger.warning("Stop %s has no position", stop_id)

                    r.db.raw_execute(
                        "INSERT INTO stops (stop_id, name, code, lat, lon) VALUES (?,?,?,?,?)",
                        (stop_id, name, code, lat, lon),
                    )
