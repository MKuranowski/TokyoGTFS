# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import csv
import platform
import zipfile
from pathlib import Path
from typing import (IO, Any, Callable, Container, Dict, Iterator, Mapping,
                    Optional, TypeVar)

from .const import DIR_GTFS

# Restrict C backend from other runtimes than CPython
# this was causing problems in PyPy actually.
if platform.python_implementation() == "CPython":
    import ijson
else:
    import ijson.backends.yajl2_cffi as ijson


Row = Mapping[str, str]
K = TypeVar("K")
V = TypeVar("V")


def last_part(id: str) -> str:
    return id.rpartition(".")[2]


def first_part(id: str) -> str:
    return id.partition(".")[0]


def load_csv_as_mapping(file: Path, key_getter: Callable[[Row], K],
                        value_getter: Callable[[Row], V]) -> Dict[K, V]:
    """Loads a CSV file as a map of key-value pairs for every row."""
    with file.open(mode="r", encoding="utf-8", newline="") as f:
        return {
            key_getter(row): value_getter(row)
            for row in csv.DictReader(f)
        }


def text_color(color: str) -> str:
    """Given a color, estimate if it's better to
    show block or white text on top of it.

    The input color should be of RRGGBB form.
    """
    r = int(color[0:2], base=16)
    g = int(color[2:4], base=16)
    b = int(color[4:6], base=16)
    yiq = 0.299 * r + 0.587 * g + 0.114 * b

    return "000000" if yiq > 128 else "FFFFFF"


def time_to_str(s: int) -> str:
    """Convert int of seconds after midnight to a GTFS time-string"""
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h:0>2}:{m:0>2}:{s:0>2}"


def IJsonIterator(buffer: IO) -> Iterator[Any]:
    """Takes a file-like object with a json array, and yields elements from that array.
    Provided buffer will be automatically closed."""
    try:
        yield from ijson.items(buffer, "item", use_float=True)  # type: ignore
    finally:
        buffer.close()


def clear_directory(path: Path):
    """Clears the contents of a directory. Only files can reside in this directory."""
    for f in path.iterdir():
        f.unlink()


def ensure_dir_exists(path: Path, clear: bool = False) -> bool:
    """Ensures such given directory exists.
    Returns False if directory was just created, True if it already exists.
    """
    try:
        path.mkdir()
        return False
    except FileExistsError:
        if clear:
            clear_directory(path)
        return True


def compress_gtfs(path: Path = DIR_GTFS, target: str = "gtfs.zip",
                  files: Optional[Container[str]] = None) -> None:
    """Creates a zip archive with files from the provided path.
    By default only *.txt files are compressed, unless `files` is provided -
    then files are compressed if their name is in the `files` container.
    """
    # Filter with the provided `files` container.
    # If that was not provided, only compress .txt files
    ok: Callable[[str], bool] = files.__contains__ if files is not None \
        else lambda f: f.endswith(".txt")

    with zipfile.ZipFile(target, mode="w", compression=zipfile.ZIP_DEFLATED) as arch:
        for f in filter(lambda f: ok(f.name), path.iterdir()):
            arch.write(f, arcname=f.name)
