# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import cast

from impuls import Task, TaskRuntime
from impuls.errors import DataError
from impuls.model import Route

from ..util import text_color_for


class CurateAgencies(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        to_curate = {cast(str, i[0]) for i in r.db.raw_execute("SELECT agency_id FROM agencies")}
        with r.db.transaction():
            for row in r.resources["agencies.csv"].csv():
                if row["id"] not in to_curate:
                    continue
                to_curate.discard(row["id"])

                r.db.raw_execute(
                    "UPDATE agencies SET name = ?, url = ?, lang = 'ja' WHERE agency_id = ?",
                    (f'{row["name_ja"]} {row["name_en"]}', row["url"], row["id"]),
                )
                r.db.raw_execute_many(
                    "INSERT INTO translations (table_name, field_name, language, translation, "
                    "record_id) VALUES ('agency', 'agency_name', ?, ?, ?)",
                    (
                        (lang, row[f"name_{lang}"], row["id"])
                        for lang in ("ja", "en", "ko", "zh-Hans", "zh-Hant")
                    ),
                )
        if to_curate:
            raise DataError("Missing curated data for agencies: " + ", ".join(sorted(to_curate)))


class CurateRoutes(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        to_curate = {cast(str, i[0]) for i in r.db.raw_execute("SELECT route_id FROM routes")}
        with r.db.transaction():
            for row in r.resources["routes.csv"].csv():
                if row["id"] not in to_curate:
                    continue
                to_curate.discard(row["id"])

                # Parse the "mode"/type
                match row["mode"]:
                    case "rail":
                        type = Route.Type.RAIL
                    case "metro":
                        type = Route.Type.METRO
                    case "tram":
                        type = Route.Type.TRAM
                    case "monorail":
                        type = Route.Type.MONORAIL
                    case _:
                        type = Route.Type.RAIL
                        self.logger.error("Route %s uses unknown mode: %s", row["id"], row["mode"])

                r.db.raw_execute(
                    "UPDATE routes SET short_name = ?, long_name = ?, type = ?, color = ?, "
                    "text_color = ? WHERE route_id = ?",
                    (
                        row["code"],
                        f'{row["name_ja"]} {row["name_en"]}',
                        type.value,
                        row["color"],
                        text_color_for(row["color"]),
                        row["id"],
                    ),
                )
                r.db.raw_execute_many(
                    "INSERT INTO translations (table_name, field_name, language, translation, "
                    "record_id) VALUES ('routes', 'route_long_name', ?, ?, ?)",
                    (
                        (lang, row[f"name_{lang}"], row["id"])
                        for lang in ("ja", "en", "ko", "zh-Hans", "zh-Hant")
                    ),
                )
        if to_curate:
            raise DataError("Missing curated data for routes: " + ", ".join(sorted(to_curate)))
