# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Generator
from typing import Any

import ijson  # type: ignore
from impuls.tools.types import StrPath


def json_items(path: StrPath) -> Generator[Any, None, None]:
    with open(path, "r", encoding="utf-8") as f:
        yield from ijson.items(f, "item", use_float=True)


def strip_prefix(x: str) -> str:
    """
    >>> strip_prefix("odpt.BusroutePattern:TobuBus.Take16.101010008720")
    'TobuBus.Take16.101010008720'
    """
    return x.partition(":")[2]
