# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import csv
import logging
from typing import Set

from ...const import DIR_CURATED, Color

logger = logging.getLogger("geo.station_names")


def find_duplicates() -> bool:
    logger.info
    seen_ids: Set[str] = set()
    duplicates: int = 0
    new_file = DIR_CURATED / "station_names.csv"
    old_file = DIR_CURATED / "station_names.csv.old"

    new_file.rename(old_file)

    logger.info("Looking for duplicates")
    with old_file.open(mode="r", encoding="utf-8", newline="") as in_buf, \
            new_file.open(mode="w", encoding="utf-8", newline="") as out_buf:
        # Create CSV wrappers around file buffers
        r = csv.DictReader(in_buf)
        assert r.fieldnames
        w = csv.DictWriter(out_buf, r.fieldnames)
        w.writeheader()

        # Find duplicates in file
        for row in r:
            id = row["sta_id"]

            if id in seen_ids:
                duplicates += 1
                logger.critical(f"{Color.RED}Duplicate entry for ID "
                                f"{Color.MAGENTA}{id}{Color.RESET}")
            else:
                seen_ids.add(id)

            w.writerow(row)

    old_file.unlink()

    if duplicates:
        logger.info(f"Total {Color.MAGENTA}{duplicates}{Color.RESET} duplicates")
        return False
    else:
        logger.info(f"{Color.GREEN}No duplicates found{Color.RESET}")
        return True
