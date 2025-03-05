# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from typing import Any, Self, cast

from impuls import DBConnection, Task, TaskRuntime


@dataclass
class _MergeConfig:
    src_id: str
    reverse_direction: bool = False
    exceptional: bool = False

    @classmethod
    def from_yml(cls, data: Any) -> Self:
        return (
            cls(data)
            if isinstance(data, str)
            else cls(
                data["id"],
                data.get("reverse_direction", False),
                data.get("exceptional", False),
            )
        )


class MergeRoutes(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            for dst_id, configs in self.load_configs(r).items():
                all_stops = self.load_all_stops(r.db)
                for config in configs:
                    changes = self.apply_config_to_routes(r.db, dst_id, config)
                    self.apply_config_to_stops(r.db, all_stops, dst_id, config.src_id)
                    self.logger.info("Moved %d trips from %s to %s", changes, config.src_id, dst_id)

    def load_configs(self, r: TaskRuntime) -> dict[str, list[_MergeConfig]]:
        return {
            dst_id: [_MergeConfig.from_yml(i) for i in config_list]
            for dst_id, config_list in r.resources["route_merges.yml"].yaml().items()
        }

    def load_all_stops(self, db: DBConnection) -> set[str]:
        return {
            cast(str, i[0])
            for i in db.raw_execute("SELECT stop_id FROM stops WHERE location_type = 0")
        }

    def apply_config_to_routes(
        self,
        db: DBConnection,
        dst_id: str,
        config: _MergeConfig,
    ) -> int:
        set_clauses = "route_id = ?"
        if config.reverse_direction:
            set_clauses += ", direction = CASE direction WHEN 0 THEN 1 WHEN 1 THEN 0 ELSE NULL END"
        if config.exceptional:
            set_clauses += ", exceptional = 1"

        changes = db.raw_execute(
            f"UPDATE trips SET {set_clauses} WHERE route_id = ?",
            (dst_id, config.src_id),
        ).rowcount
        db.raw_execute("DELETE FROM routes WHERE route_id = ?", (config.src_id,))
        db.raw_execute(
            "DELETE FROM translations WHERE table_name = 'routes' AND record_id = ?",
            (config.src_id,),
        )
        return changes

    def apply_config_to_stops(
        self,
        db: DBConnection,
        all_stops: set[str],
        dst_id: str,
        src_id: str,
    ) -> None:
        src_prefix = src_id + "."
        dst_prefix = dst_id + "."
        src_stops = {i for i in all_stops if i.startswith(src_prefix)}
        for src_stop_id in src_stops:
            assert src_stop_id.startswith(src_prefix)
            dst_stop_id = dst_prefix + src_stop_id[len(src_prefix) :]

            if dst_stop_id in all_stops:
                db.raw_execute(
                    "UPDATE stop_times SET stop_id = ? WHERE stop_id = ?",
                    (dst_stop_id, src_stop_id),
                )
                db.raw_execute("DELETE FROM stops WHERE stop_id = ?", (src_stop_id,))
                db.raw_execute(
                    "DELETE FROM translations WHERE table_name = 'stops' AND record_id = ?",
                    (src_stop_id,),
                )
                all_stops.remove(src_stop_id)
            else:
                db.raw_execute(
                    "UPDATE stops SET stop_id = ? WHERE stop_id = ?",
                    (dst_stop_id, src_stop_id),
                )
                db.raw_execute(
                    "UPDATE translations SET record_id = ? "
                    "WHERE table_name = 'stops' AND record_id = ?",
                    (dst_stop_id, src_stop_id),
                )
                all_stops.remove(src_stop_id)
                all_stops.add(dst_stop_id)
