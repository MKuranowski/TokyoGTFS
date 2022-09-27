# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT
from typing import List

from ...apikeys import set_apikeys
from ..model import Provider
from .odpt import ODPTProvider

KNOWN_PROVIDERS: List[Provider] = [ODPTProvider()]


def get_all_providers() -> List[Provider]:
    p = KNOWN_PROVIDERS.copy()
    set_apikeys(p)
    return p


def get_provider_for(agency: str) -> Provider:
    for p in KNOWN_PROVIDERS:
        if agency in p.provides:
            set_apikeys([p])
            return p

    raise KeyError(f"no known provider provides {agency}")


def get_provider_by_name(name: str) -> Provider:
    for p in KNOWN_PROVIDERS:
        if p.name == name:
            set_apikeys([p])
            return p

    raise KeyError(f"no provider with name {name}")
