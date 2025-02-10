# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterable
from dataclasses import dataclass
from typing import cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Route


@dataclass
class _LimitedExpressConfig:
    root_route: str
    short_name_match: str


class SeparateLimitedExpresses(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            for route_id, config in self.load_configs(r).items():
                self.insert_route(r.db, route_id)
                trips = self.find_and_fix_matching_trips(r.db, config)
                self.move_trips_to_new_route(r.db, route_id, trips)
                self.logger.info("Moved %d trips to %s", len(trips), route_id)

    def load_configs(self, r: TaskRuntime) -> dict[str, _LimitedExpressConfig]:
        return {
            route_id: _LimitedExpressConfig(**config_dict)
            for route_id, config_dict in r.resources["limited_expresses.yml"].yaml().items()
        }

    def insert_route(self, db: DBConnection, route_id: str) -> None:
        agency_id, _, short_name = route_id.partition(".")
        db.create(Route(route_id, agency_id, short_name, "", Route.Type.RAIL))

    def find_and_fix_matching_trips(
        self,
        db: DBConnection,
        config: _LimitedExpressConfig,
    ) -> list[str]:
        trips = self.find_matching_solo_trips(db, config)
        trips.extend(self.find_and_fix_matching_blocks(db, config))
        return trips

    def find_matching_solo_trips(
        self,
        db: DBConnection,
        config: _LimitedExpressConfig,
    ) -> list[str]:
        return [
            cast(str, i[0])
            for i in db.raw_execute(
                "SELECT trip_id FROM trips WHERE route_id = ? AND instr(short_name, ?) != 0 "
                "AND block_id IS NULL",
                (config.root_route, config.short_name_match),
            )
        ]

    def find_and_fix_matching_blocks(
        self,
        db: DBConnection,
        config: _LimitedExpressConfig,
    ) -> list[str]:
        # Identify all (block_id, direction) pairs which match the config
        blocks = [
            (cast(str, i[0]), cast(int | None, i[1]))
            for i in db.raw_execute(
                "SELECT block_id, direction FROM trips WHERE route_id = ? "
                "AND instr(short_name, ?) != 0 AND block_id IS NOT NULL",
                (config.root_route, config.short_name_match),
            )
        ]

        # Fix direction_id of all trips in that block to match the root route's trip
        db.raw_execute_many(
            "UPDATE trips SET direction = ? WHERE block_id = ?",
            ((direction, block_id) for block_id, direction in blocks),
        )

        # Return all trip_ids
        return [
            cast(str, i[0])
            for block_id, _ in blocks
            for i in db.raw_execute("SELECT trip_id FROM trips WHERE block_id = ?", (block_id,))
        ]

    def move_trips_to_new_route(
        self,
        db: DBConnection,
        route_id: str,
        trips: Iterable[str],
    ) -> None:
        db.raw_execute_many(
            "UPDATE trips SET route_id = ? WHERE trip_id = ?",
            ((route_id, trip_id) for trip_id in trips),
        )
