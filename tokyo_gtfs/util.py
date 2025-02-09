# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import json
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

import ijson  # type: ignore
from impuls.tools.types import StrPath


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


def json_items(path: StrPath) -> Generator[Any, None, None]:
    with open(path, "r", encoding="utf-8") as f:
        yield from ijson.items(f, "item", use_float=True)


def compact_json(obj: Any) -> str:
    return json.dumps(obj, indent=None, separators=(",", ":"))


def text_color_for(color: str) -> str:
    r = int(color[0:2], base=16)
    g = int(color[2:4], base=16)
    b = int(color[4:6], base=16)
    yiq = 0.299 * r + 0.587 * g + 0.114 * b
    return "000000" if yiq > 128 else "FFFFFF"


def strip_prefix(x: str) -> str:
    """
    >>> strip_prefix("odpt.BusroutePattern:TobuBus.Take16.101010008720")
    'TobuBus.Take16.101010008720'
    """
    return x.partition(":")[2]
