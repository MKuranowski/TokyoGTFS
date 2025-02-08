# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from typing import cast

from impuls import DBConnection, Task, TaskRuntime

from .util import Translation, split_name, unpack_list

NARITA_AIRPORT_HEADSIGN_NAME = Translation(
    ja="成田空港",
    en="Narita Airport",
    ko="나리타 공항",
    zh_hans="成田机场",
    zh_hant="成田機場",
)


@dataclass
class Train:
    id: str
    type: str
    destinations: list[str]


class GenerateHeadsigns(Task):
    def __init__(self) -> None:
        super().__init__()
        self.known_invalid_train_types = set[str]()
        self.stop_names = dict[str, Translation]()
        self.train_type_names = dict[str, Translation]()

    def execute(self, r: TaskRuntime) -> None:
        self.known_invalid_train_types.clear()
        self.stop_names = self.get_stop_names(r.db)
        self.train_type_names = self.get_train_type_names(r.db)
        trains = self.get_all_trains(r.db)

        with r.db.transaction():
            for train in trains:
                r.db.raw_execute(
                    "UPDATE trips SET headsign = ? WHERE trip_id = ?",
                    (self.generate_headsign(train, "default"), train.id),
                )

                for lang in ("ja", "en", "ko", "zh-Hans", "zh-Hant"):
                    translation = self.generate_headsign(train, lang.lower().replace("-", "_"))
                    if translation:
                        r.db.raw_execute(
                            "INSERT INTO translations (table_name, field_name, language, "
                            "translation, record_id) VALUES ('trips', 'trip_headsign', ?, ?, ?)",
                            (lang, translation, train.id),
                        )

    def get_stop_names(self, db: DBConnection) -> dict[str, Translation]:
        # Initialize from stops
        names = {
            cast(str, i[0]): Translation(*split_name(cast(str, i[1])))
            for i in db.raw_execute("SELECT stop_id, name FROM stops")
        }

        # Update by using translations
        select_translation = db.raw_execute(
            "SELECT record_id, language, translation FROM translations "
            "WHERE table_name = 'stops' and field_name = 'stop_name'"
        )
        for row in select_translation:
            stop_id = cast(str, row[0])

            if stop_id.endswith(".NaritaAirportTerminal1"):
                names[stop_id] = NARITA_AIRPORT_HEADSIGN_NAME
                continue

            name = names.get(stop_id)
            if not name:
                continue

            language = cast(str, row[1]).lower().replace("-", "_")
            if not hasattr(name, language):
                continue

            translation = cast(str, row[2])
            setattr(name, language, translation)

        return names

    def get_train_type_names(self, db: DBConnection) -> dict[str, Translation]:
        return {
            i[0]: Translation(*i[1:])  # type: ignore
            for i in db.raw_execute(
                "SELECT train_type_id, name_ja, name_en, name_ko, name_zh_hans, name_zh_hant "
                "FROM train_types"
            )
        }

    def get_all_trains(self, db: DBConnection) -> list[Train]:
        return [
            Train(cast(str, i[0]), cast(str, i[1]), unpack_list(cast(str, i[2])))
            for i in db.raw_execute(
                "SELECT trip_id, extra_fields_json->>'train_type', "
                "extra_fields_json->>'destinations' FROM trips"
            )
        ]

    def generate_headsign(self, train: Train, lang: str) -> str:
        if lang in ("en", "ko"):
            left_bracket = "("
            right_bracket = ") "
            separator = " / "
        else:
            left_bracket = "【"
            right_bracket = "】"
            separator = "・"

        if tt := self.train_type_names.get(train.type):
            train_type_part = f"{left_bracket}{getattr(tt, lang)}{right_bracket}"
        else:
            train_type_part = ""
            self.warn_about_invalid_train_type(train)

        destination_names = [
            getattr(self.stop_names[stop_id], lang) for stop_id in train.destinations
        ]
        destination_part = separator.join(destination_names)

        return f"{train_type_part}{destination_part}"

    def warn_about_invalid_train_type(self, train: Train) -> None:
        if train.type not in self.known_invalid_train_types:
            self.logger.error("Invalid train type: %s (from trip %s)", train.type, train.id)
            self.known_invalid_train_types.add(train.type)
