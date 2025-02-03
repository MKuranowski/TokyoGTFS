import json
from typing import Any


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
