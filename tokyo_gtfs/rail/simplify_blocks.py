# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Generator, Iterable
from dataclasses import dataclass, replace
from itertools import chain, groupby
from operator import attrgetter
from typing import Self, cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import StopTime, TimePoint, Trip

PREFERRED_BASE_ROUTES = [
    "JR-East.Yokosuka.",
    "JR-East.Tsurumi.",
    "JR-East.Musashino.",
    "Tobu.TobuSkytree.",
]


@dataclass
class TripWithTimes:
    trip: Trip
    times: list[StopTime]

    def __post_init__(self) -> None:
        self.times.sort(key=attrgetter("stop_sequence"))

    @property
    def first_time(self) -> TimePoint:
        return self.times[0].arrival_time

    @property
    def last_time(self) -> TimePoint:
        return self.times[-1].departure_time

    @classmethod
    def retrieve(cls, db: DBConnection, trip_id: str) -> Self:
        return cls(
            db.retrieve_must(Trip, trip_id),
            list(
                db.typed_out_execute(
                    "SELECT * FROM stop_times WHERE trip_id = ?",
                    StopTime,
                    (trip_id,),
                )
            ),
        )


@dataclass
class BlockWithTrips:
    block_id: str
    trips: list[TripWithTimes]

    def __post_init__(self) -> None:
        self.trips.sort(key=attrgetter("first_time"))

    @classmethod
    def retrieve(cls, db: DBConnection, block_id: str) -> Self:
        return cls(
            block_id,
            [
                TripWithTimes.retrieve(db, cast(str, i[0]))
                for i in db.raw_execute(
                    "SELECT trip_id FROM trips WHERE block_id = ?",
                    (block_id,),
                )
            ],
        )


class SimplifyBlocks(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        blocks = self.find_blocks_to_merge(r.db)
        with r.db.transaction():
            for block_id in blocks:
                self.logger.debug("Merging block %s", block_id)
                block = BlockWithTrips.retrieve(r.db, block_id)
                self.merge_consecutive_trips_in_block(r.db, block)
            self.logger.debug("Removing alone blocks")
            self.remove_blocks_with_one_trip(r.db)

    def find_blocks_to_merge(self, db: DBConnection) -> list[str]:
        # Find all blocks which:
        # 1. reference a single route more than once,
        # 2. use the same calendar, and
        # 3. one of the following:
        #   i. have the same short_name and headsign, or
        #   ii. belong to the JR-East.NaritaExpress or JR-East.Musashino lines.
        return [
            cast(str, i[0])
            for i in db.raw_execute(
                "SELECT block_id FROM trips WHERE block_id IS NOT NULL "
                "GROUP BY block_id, route_id "
                "HAVING count(*) > 1 AND count(distinct calendar_id) = 1 AND "
                "((count(distinct short_name) = 1 AND count(distinct headsign) = 1) "
                " OR route_id IN ('JR-East.NaritaExpress', 'JR-East.Musashino'))"
            )
        ]

    def merge_consecutive_trips_in_block(self, db: DBConnection, block: BlockWithTrips) -> None:
        for _, trips_it in groupby(block.trips, key=attrgetter("trip.route_id")):
            trips = list(trips_it)
            if len(trips) > 1:
                self.merge_trips(db, trips)

    def merge_trips(self, db: DBConnection, trips: list[TripWithTimes]) -> None:
        base = max(trips, key=score_base_candidate)
        stop_times = self.get_merged_stop_times(base.trip.id, trips)
        self.replace_stop_times(db, base.trip.id, stop_times)
        self.drop_trips(db, (i.trip.id for i in trips if i is not base))

    def drop_trips(self, db: DBConnection, trip_ids: Iterable[str]) -> None:
        for trip_id in trip_ids:
            db.raw_execute("DELETE FROM trips WHERE trip_id = ?", (trip_id,))
            db.raw_execute(
                "DELETE FROM translations WHERE table_name = 'trips' AND record_id = ?",
                (trip_id,),
            )

    def get_merged_stop_times(
        self,
        trip_id: str,
        trips: Iterable[TripWithTimes],
    ) -> Generator[StopTime, None, None]:
        combined: StopTime | None = None
        idx = 0

        for incoming in chain.from_iterable(i.times for i in trips):
            if combined is None:
                # No previous stop-time: remember current one
                combined = replace(incoming, trip_id=trip_id, stop_sequence=idx)
                idx += 1
            elif same_stop(combined.stop_id, incoming.stop_id):
                # Previous stop-time represents the same stop:
                # remember attributes from the last (departure) stop-time,
                # except for arrival time and stop_sequence
                combined = replace(
                    incoming,
                    trip_id=trip_id,
                    stop_sequence=combined.stop_sequence,
                    arrival_time=combined.arrival_time,
                )
            else:
                # Previous stop-time represents different stop:
                # yield the previous one and remember current one
                yield combined
                combined = replace(incoming, trip_id=trip_id, stop_sequence=idx)
                idx += 1

        # Generate the last stop-time
        if combined:
            yield combined

    def replace_stop_times(
        self,
        db: DBConnection,
        trip_id: str,
        stop_times: Iterable[StopTime],
    ) -> None:
        db.raw_execute("DELETE FROM stop_times WHERE trip_id = ?", (trip_id,))
        db.create_many(StopTime, stop_times)

    def remove_blocks_with_one_trip(self, db: DBConnection) -> None:
        alone_blocks = [
            cast(str, i[0])
            for i in db.raw_execute(
                "SELECT block_id FROM trips WHERE block_id IS NOT NULL "
                "GROUP BY block_id HAVING COUNT(*) = 1"
            )
        ]
        db.raw_execute_many(
            "UPDATE trips SET block_id = NULL WHERE block_id = ?",
            ((block_id,) for block_id in alone_blocks),
        )


def score_base_candidate(t: TripWithTimes) -> tuple[int, str]:
    if t.trip.id.startswith("JR-East.ShonanShinjuku."):
        return (4, t.trip.id)  # For N'EX which also stop at the Yokosuka line
    if any(t.trip.id.startswith(prefix) for prefix in PREFERRED_BASE_ROUTES):
        return (3, t.trip.id)
    elif "Branch" not in t.trip.id:
        return (2, t.trip.id)
    else:
        return (1, t.trip.id)


def same_stop(a: str, b: str) -> bool:
    a_stem = a.rpartition(".")[2]
    b_stem = b.rpartition(".")[2]
    return a_stem == b_stem
