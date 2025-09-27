# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.errors import DataError
from impuls.model import Route

from ..util import combine_name, text_color_for


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


class CurateStops(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        to_curate = {cast(str, i[0]) for i in r.db.raw_execute("SELECT stop_id FROM stops")}
        with r.db.transaction():
            for row in r.resources["stations.csv"].csv():
                id = row["id"]

                # Ignore unknown stops
                if id not in to_curate:
                    self.logger.warning("stations.csv: %s: stop does not exist", id)
                    continue

                to_curate.discard(id)
                self.curate_stop_position(r.db, id, row["lat"], row["lon"])
                self.curate_stop_code(r.db, id, row["code"])
                self.curate_stop_name(r.db, id, row["name_ja"], row["name_en"])
                self.curate_other_translation(r.db, id, "ko", row.get("name_ko", ""))
                self.curate_other_translation(r.db, id, "zh-Hans", row.get("name_zh_hans", ""))
                self.curate_other_translation(r.db, id, "zh-Hant", row.get("name_zh_hant", ""))

        if to_curate:
            raise DataError("Missing curated data for stops: " + ", ".join(sorted(to_curate)))

    def curate_stop_position(self, db: DBConnection, id: str, lat_str: str, lon_str: str) -> None:
        if lat_str and lon_str:
            db.raw_execute(
                "UPDATE stops SET lat = ?, lon = ? WHERE stop_id = ?",
                (float(lat_str), float(lon_str), id),
            )
        elif lat_str or lon_str:
            raise DataError("stations.csv: %s: lat and lon must be provided simultaneously", id)

    def curate_stop_code(self, db: DBConnection, id: str, code: str) -> None:
        if code:
            db.raw_execute("UPDATE stops SET code = ? WHERE stop_id = ?", (code, id))

    def curate_stop_name(self, db: DBConnection, id: str, ja: str, en: str) -> None:
        if ja and en:
            db.raw_execute(
                "UPDATE stops SET name = ? WHERE stop_id = ?",
                (combine_name(ja, en), id),
            )
            db.raw_execute(
                (
                    "UPDATE translations SET translation = ? WHERE "
                    "table_name = 'stops' AND field_name = 'stop_name' AND "
                    "language = 'ja' AND record_id = ?"
                ),
                (ja, id),
            )
            db.raw_execute(
                (
                    "UPDATE translations SET translation = ? WHERE "
                    "table_name = 'stops' AND field_name = 'stop_name' AND "
                    "language = 'en' AND record_id = ?"
                ),
                (en, id),
            )
        elif ja or en:
            raise DataError(
                "stations.csv: %s: name_ja and name_en must be provided simultaneously",
                id,
            )

    def curate_other_translation(self, db: DBConnection, id: str, lang: str, name: str) -> None:
        if name:
            db.raw_execute(
                (
                    "UPDATE translations SET translation = ? WHERE "
                    "table_name = 'stops' AND field_name = 'stop_name' AND "
                    "language = ? AND record_id = ?"
                ),
                (name, lang, id),
            )
