# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import json
from dataclasses import dataclass
from typing import Any


@dataclass
class Translation:
    ja: str = ""
    en: str = ""
    ko: str = ""
    zh_hans: str = ""
    zh_hant: str = ""

    @property
    def default(self) -> str:
        return f"{self.ja} {self.en}"


def combine_name(ja: str, en: str) -> str:
    return f"{ja} {en}"


def split_name(name: str) -> tuple[str, str]:
    ja, _, en = name.partition(" ")
    return ja, en


def pack_list(elements: list[str]) -> str:
    return ";".join(elements)


def unpack_list(packed: str) -> list[str]:
    return packed.split(";") if packed else []


def compact_json(obj: Any) -> str:
    return json.dumps(obj, indent=None, separators=(",", ":"))


def text_color_for(color: str) -> str:
    r = int(color[0:2], base=16)
    g = int(color[2:4], base=16)
    b = int(color[4:6], base=16)
    yiq = 0.299 * r + 0.587 * g + 0.114 * b
    return "000000" if yiq > 128 else "FFFFFF"
