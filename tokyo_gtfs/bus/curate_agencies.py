# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import cast

from impuls import Task, TaskRuntime

from ..util import text_color_for


class CurateAgencies(Task):
    def __init__(self, resource: str) -> None:
        super().__init__()
        self.resource = resource

    def execute(self, r: TaskRuntime) -> None:
        to_curate = {cast(str, i[0]) for i in r.db.raw_execute("SELECT agency_id FROM agencies")}
        with r.db.transaction():
            for row in r.resources[self.resource].csv():
                if row["id"] not in to_curate:
                    continue
                to_curate.discard(row["id"])

                r.db.raw_execute(
                    "UPDATE agencies SET name = ?, url = ? WHERE agency_id = ?",
                    (row["name_ja"], row["url"], row["id"]),
                )
                r.db.raw_execute(
                    "UPDATE routes SET color = ?, text_color = ? WHERE agency_id = ?",
                    (row["color"], text_color_for(row["color"]), row["id"]),
                )
        assert not to_curate, "data from unknown agencies should not be loaded into the db"
