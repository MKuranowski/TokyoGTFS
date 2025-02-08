# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from typing import Literal, cast

from impuls import DBConnection, Task, TaskRuntime

from .util import Translation


@dataclass
class Train:
    id: str
    direction: Literal[0, 1]
    next: bool
    destination: str


DIRECTION_NAMES = {
    0: Translation("外回り", "Outer Loop", "외선순환", "外环", "外環"),
    1: Translation("内回り", "Inner Loop", "내선순환", "内环", "內環"),
}


class FixYamanoteLineHeadsigns(Task):
    def __init__(self) -> None:
        super().__init__()
        self.stop_name_cache = dict[str, Translation]()

    def execute(self, r: TaskRuntime) -> None:
        self.stop_name_cache.clear()
        trains = self.get_all_trains(r.db)
        with r.db.transaction():
            for train in trains:
                r.db.raw_execute(
                    "UPDATE trips SET headsign = ? WHERE trip_id = ?",
                    (self.generate_headsign(r.db, train, "default"), train.id),
                )

                for lang in ("ja", "en", "ko", "zh-Hans", "zh-Hant"):
                    translation = self.generate_headsign(
                        r.db,
                        train,
                        lang.lower().replace("-", "_"),
                    )
                    if translation:
                        r.db.raw_execute(
                            "UPDATE translations SET translation = ? WHERE table_name = 'trips' "
                            "AND field_name = 'trip_headsign' AND language = ? AND record_id = ?",
                            (translation, lang, train.id),
                        )

    def get_all_trains(self, db: DBConnection) -> list[Train]:
        return [
            Train(
                id=cast(str, i[0]),
                direction=cast(Literal[0, 1], i[1]),
                next=bool(i[2]),
                destination=cast(str, i[3]),
            )
            for i in db.raw_execute(
                "SELECT trip_id, direction, extra_fields_json->>'next' != '', "
                "extra_fields_json->>'destinations' FROM trips "
                "WHERE route_id = 'JR-East.Yamanote'"
            )
        ]

    def get_stop_name(self, db: DBConnection, id: str) -> Translation:
        if cached := self.stop_name_cache.get(id):
            return cached

        n = Translation()
        for lang in ("ja", "en", "ko", "zh-Hans", "zh-Hant"):
            row = db.raw_execute(
                "SELECT translation FROM translations WHERE table_name = 'stops' "
                "AND field_name = 'stop_name' AND language = ? AND record_id = ?",
                (lang, id),
            ).one()
            if row:
                setattr(n, lang.lower().replace("-", "_"), cast(str, row[0]))
            else:
                self.logger.error("Missing %s name for %s", lang, id)

        self.stop_name_cache[id] = n
        return n

    def generate_headsign(self, db: DBConnection, train: Train, lang: str) -> str:
        direction = getattr(DIRECTION_NAMES[train.direction], lang)
        if train.next:
            return direction

        assert train.destination, "Yamanote line trains w/o next link must have a destination"
        last_stop = getattr(self.get_stop_name(db, train.destination), lang)

        if lang in ("en", "ko"):
            return f"({direction}) {last_stop}"
        else:
            return f"【{direction}】{last_stop}"
