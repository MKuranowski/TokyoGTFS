# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from impuls import Task, TaskRuntime

from ..util import Translation

PREFIX = Translation(
    ja="有料",
    en="Toll ",
    ko="유료 ",
    zh_hans="收费",
    zh_hant="收費",
)

TOLL_TRAIN_TYPES = [
    "Chichibu.PaleoExpress",
    "Fujikyu.LimitedExpress",
    "IzuHakone.LimitedExpress",
    "Izukyu.LimitedExpress",
    "JR-Central.LimitedExpress",
    "JR-East.LimitedExpress",
    "Keikyu.EveningWing",
    "Keikyu.MorningWing",
    "Keio.KeioLiner",
    "Keisei.Eveningliner",
    "Keisei.Morningliner",
    "Keisei.Skyliner",
    "Minatomirai.S-TRAIN",
    "Odakyu.LimitedExpress",
    "OdakyuHakone.LimitedExpress",
    "Seibu.HaijimaLiner",
    "Seibu.LimitedExpress",
    "Seibu.S-TRAIN",
    "Tobu.LimitedExpress",
    "Tobu.SL-Taiju",
    "Tobu.TH-LINER",
    "Tobu.TJ-Liner",
    "TokyoMetro.LimitedExpress",
    "TokyoMetro.S-TRAIN",
    "TokyoMetro.TH-LINER",
    "Tokyu.LimitedExpress",
    "Tokyu.S-TRAIN",
]


class MarkTollTrainTypes(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            r.db.raw_execute_many(
                "UPDATE OR IGNORE train_types "
                " SET name_ja = ? || name_ja, "
                "     name_en = ? || name_en, "
                "     name_ko = ? || name_ko, "
                "     name_zh_hans = ? || name_zh_hans, "
                "     name_zh_hant = ? || name_zh_hant "
                " WHERE train_type_id = ?",
                (
                    (PREFIX.ja, PREFIX.en, PREFIX.ko, PREFIX.zh_hans, PREFIX.zh_hant, train_type)
                    for train_type in TOLL_TRAIN_TYPES
                ),
            )
