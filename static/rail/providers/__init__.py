# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import json
import os
from operator import attrgetter
from typing import Dict, Iterable, List

from ...err import InvalidData, MissingApiKeys
from ..model import Provider
from .odpt import ODPTProvider, TokyoChallengeProvider

KNOWN_PROVIDERS: List[Provider] = [ODPTProvider(), TokyoChallengeProvider()]


def load_apikeys_json() -> Dict[str, str]:
    # Check if an override of the filename was provided
    filename = os.getenv("APIKEYS_FILE", "apikeys.json")
    file_exists = os.path.exists(filename)

    # Crash hard if override points to invalid file,
    # or return empty mapping if there's no override and the default file doesn't exist
    if "APIKEYS_FILE" in os.environ and not file_exists:
        raise MissingApiKeys(f"APIKEYS_FILE environ key set, but file {filename} doesn't exist")
    elif not file_exists:
        return {}

    # Load the json file
    with open(filename, mode="r", encoding="utf-8") as f:
        data = json.load(f)

    # Verify expected type
    if not isinstance(data, dict) or any(not isinstance(i, str) for i in data.keys()) \
            or any(not isinstance(i, str) for i in data.values()):
        raise InvalidData("incorrect apikeys.json structure")

    return data


def get_apikey_for(provider_name: str, preloaded_keys: Dict[str, str]) -> str:
    # Check in env variables
    key = os.getenv("APIKEY_" + provider_name.upper())

    # Check in preloaded keys
    if key is None:
        key = preloaded_keys.get(provider_name)

    # Throw an error if apikey is still not found
    if key is None:
        raise MissingApiKeys(f"no apikey for {provider_name}")

    return key


def set_apikeys(providers: Iterable[Provider]):
    preloaded_keys = load_apikeys_json()
    missing: List[str] = []

    for provider in filter(attrgetter("needs_apikey"), providers):
        try:
            provider.set_apikey(get_apikey_for(provider.name, preloaded_keys))
        except MissingApiKeys:
            missing.append(provider.name)

    if missing:
        raise MissingApiKeys("missing apikeys for: " + ", ".join(missing))


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
