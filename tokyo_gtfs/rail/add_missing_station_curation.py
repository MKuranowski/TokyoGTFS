# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import csv
import re
from collections.abc import Container, Iterable, Mapping, MutableMapping
from zipfile import ZipFile

import ijson  # type: ignore

Row = Mapping[str, str]


def is_station(id: str) -> bool:
    return "." not in id


def get_id_stem(id: str) -> str:
    return re.sub(r"^.*?([^.]+)(?:\.[0-9]+)?$", r"\1", id)


def station_sort_key(id: str) -> str:
    return f"{id}.0" if is_station(id) else f"{get_id_stem(id)}.1.{id}"


def load_expected_rows() -> dict[str, Row]:
    with ZipFile("_impuls_workspace/mini-tokyo-3d.zip", "r") as arch:
        with arch.open("mini-tokyo-3d-master/data/stations.json", "r") as f:
            return {
                obj["id"]: {
                    "id": obj["id"],
                    "name_ja": obj["title"]["ja"],
                    "name_en": obj["title"]["en"],
                }
                for obj in ijson.items(f, "item", use_float=True)
            }


def load_got_rows() -> dict[str, Row]:
    with open("data/stations.csv", "r", encoding="utf-8-sig", newline="") as f:
        return {i["id"]: i for i in csv.DictReader(f)}


def check_unknown_stations(got: Iterable[str], expected: Container[str]) -> None:
    unknown = list[str]()

    for id in got:
        if not is_station(id) and id not in expected:
            unknown.append(id)

    if unknown:
        unknown.sort(key=station_sort_key)
        print("Extra stations not found in mini-tokyo-3d stations.json:")
        for id in unknown:
            print("-", id)


def add_missing_stations(got: MutableMapping[str, Row], expected: Mapping[str, Row]):
    missing = list[str]()

    for id, row in expected.items():
        if id not in got:
            missing.append(id)
            got[id] = row

    if missing:
        missing.sort(key=station_sort_key)
        print("Added rows to stations.csv:")
        for id in missing:
            print("-", id)


def write_updated_stations(rows: Iterable[Row]) -> None:
    rows = sorted(rows, key=lambda row: station_sort_key(row["id"]))
    with open("data/stations.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            [
                "id",
                "code",
                "name_ja",
                "name_en",
                "name_ko",
                "name_zh_hans",
                "name_zh_hant",
                "lat",
                "lon",
            ],
            restval="",
        )
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    expected = load_expected_rows()
    got = load_got_rows()
    check_unknown_stations(got, expected)
    add_missing_stations(got, expected)
    write_updated_stations(got.values())


if __name__ == "__main__":
    main()
