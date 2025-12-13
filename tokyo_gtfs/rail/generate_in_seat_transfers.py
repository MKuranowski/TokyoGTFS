# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from collections import defaultdict
from collections.abc import Iterable
from itertools import pairwise
from operator import itemgetter
from typing import cast

from impuls import DBConnection, Task, TaskRuntime

TripIDStartTime = tuple[str, int]


class GenerateInSeatTransfers(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        blocks = self.get_all_blocks(r.db)
        self.order_blocks(blocks.values())
        with r.db.transaction():
            r.db.raw_execute_many(
                "INSERT INTO transfers (from_trip_id, to_trip_id, transfer_type) VALUES (?, ?, 4)",
                self.generate_transfers(blocks.values()),
            )

    def get_all_blocks(self, db: DBConnection) -> defaultdict[str, list[TripIDStartTime]]:
        blocks = defaultdict[str, list[TripIDStartTime]](list)
        query = db.raw_execute(
            "SELECT trip_id, block_id, "
            "      (SELECT MIN(departure_time) FROM stop_times "
            "       WHERE stop_times.trip_id = trips.trip_id) AS start_time "
            "FROM trips "
            "WHERE block_id IS NOT NULL"
        )
        for row in query:
            trip_id = cast(str, row[0])
            block_id = cast(str, row[1])
            start_time = cast(int, row[2])
            blocks[block_id].append((trip_id, start_time))
        return blocks

    def order_blocks(self, blocks: Iterable[list[TripIDStartTime]]):
        for block in blocks:
            block.sort(key=itemgetter(1))

    def generate_transfers(
        self,
        blocks: Iterable[Iterable[TripIDStartTime]],
    ) -> Iterable[tuple[str, str]]:
        for block in blocks:
            for a, b in pairwise(block):
                yield a[0], b[0]
