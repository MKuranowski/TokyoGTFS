# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterable
from dataclasses import dataclass, replace
from itertools import chain
from typing import cast

from impuls import DBConnection, Task, TaskRuntime

from ..util import unpack_list


@dataclass
class BlockNode:
    trip_id: str
    destinations: list[str]
    previous: list[str]
    next: list[str]
    clone_with_suffix: str = ""


class GenerateBlocks(Task):
    def __init__(self) -> None:
        super().__init__()
        self.to_process = dict[str, BlockNode]()
        self.counter = 0

    def execute(self, r: TaskRuntime) -> None:
        self.counter = 0
        self.to_process = self.find_all_nodes(r.db)
        with r.db.transaction():
            while self.to_process:
                self.solve(r.db, self.to_process.popitem()[1])

    def find_all_nodes(self, db: DBConnection) -> dict[str, BlockNode]:
        nodes = self.retrieve_all_nodes(db)
        self.fix_link_consistency(nodes)
        return nodes

    def retrieve_all_nodes(self, db: DBConnection) -> dict[str, BlockNode]:
        return {
            cast(str, i[0]): BlockNode(
                cast(str, i[0]),
                unpack_list(cast(str, i[1])),
                unpack_list(cast(str, i[2])),
                unpack_list(cast(str, i[3])),
            )
            for i in db.raw_execute(
                "SELECT trip_id, extra_fields_json->>'destinations', extra_fields_json->>'previous', "
                "       extra_fields_json->>'next' FROM trips"
            )
        }

    def fix_link_consistency(self, nodes: dict[str, BlockNode]) -> None:
        for node in nodes.values():
            self._fix_node_link_consistency(nodes, node, forward=True)
            self._fix_node_link_consistency(nodes, node, forward=False)

    def _fix_node_link_consistency(
        self,
        nodes: dict[str, BlockNode],
        n: BlockNode,
        forward: bool,
    ) -> None:
        self_attr = "next" if forward else "previous"
        other_attr = "previous" if forward else "next"

        id = n.trip_id
        fixed = list[str]()

        for linked_id in getattr(n, self_attr):
            if linked := nodes.get(linked_id):
                if id not in getattr(linked, other_attr):
                    self.logger.warning(
                        "Trip link inconsistency: %s %s has %s, but not vice versa",
                        id,
                        self_attr,
                        linked_id,
                    )
                    getattr(linked, other_attr).append(id)
                fixed.append(linked_id)
            else:
                self.logger.error(
                    "Invalid trip link: %s %s has %s, which doesn't exist",
                    id,
                    self_attr,
                    linked_id,
                )

        setattr(n, self_attr, fixed)

    def solve(self, db: DBConnection, root: BlockNode) -> None:
        linked_nodes = self.find_all_linked_nodes(root)
        if len(linked_nodes) <= 1:
            return  # Nothing linked - don't assign a block_id
        blocks = self.find_all_blocks(linked_nodes)
        self.assign_blocks(db, blocks)

    def find_all_linked_nodes(self, root: BlockNode) -> dict[str, BlockNode]:
        visited = dict[str, BlockNode]()
        queue = [root]
        while queue:
            node = queue.pop()
            for linked_id in chain(node.next, node.previous):
                if linked_id not in visited:
                    queue.append(self.to_process.pop(linked_id))
            visited[node.trip_id] = node
        return visited

    def find_all_blocks(self, nodes: dict[str, BlockNode]) -> list[list[BlockNode]]:
        # Only 3 topologies are permitted:
        # 1. "flat":  o--o--o--o--o
        # 2. "split": o--o--o--o--o
        #                    \-o--o
        # 3. "join":  o--o--o--o--o
        #             o--o-/

        forks = self._find_forks(nodes.values())
        if len(forks) == 0:
            return [list(nodes.values())]  # "flat" topology - return a single block
        elif len(forks) == 1:
            return self._assign_blocks_around_fork(nodes, forks[0])
        else:
            self.logger.warning(
                "Block with %d forks (%s)",
                len(forks),
                ", ".join(n.trip_id for n in forks),
            )
            return []

    def _find_forks(self, nodes: Iterable[BlockNode]) -> list[BlockNode]:
        # NOTE: If a Node has multiple previous and next trips, it needs to be counted twice.
        #       Such a node represents 2 forks, one at its first and one at its last stop.
        return [n for n in nodes if len(n.next) > 1] + [n for n in nodes if len(n.previous) > 1]

    def _assign_blocks_around_fork(
        self,
        nodes: dict[str, BlockNode],
        fork: BlockNode,
    ) -> list[list[BlockNode]]:
        assert len(fork.next) > 1 or len(fork.previous) > 1, "`fork` does not fork"
        assert len(fork.next) <= 1 or len(fork.previous) <= 1, "`fork` forks multiple times"

        if len(fork.previous) > 1:
            is_split = False
            shared_leg = self._find_linked_forward(nodes, fork)
            unique_legs = [self._find_linked_backward(nodes, nodes[i]) for i in fork.previous]
        else:
            is_split = True
            shared_leg = self._find_linked_backward(nodes, fork)
            unique_legs = [self._find_linked_forward(nodes, nodes[i]) for i in fork.next]

        blocks = list[list[BlockNode]]()
        for idx, unique_leg in enumerate(unique_legs, start=1):
            destinations = (unique_leg[-1] if is_split else shared_leg[-1]).destinations
            suffix = f".{idx}" if idx != 1 else ""

            # Create clones of nodes from the shared leg
            # FIXME: To be 100% kosher we should also fix the next/prev ids and add the suffix
            shared_leg_copy = [
                replace(
                    n,
                    destinations=destinations,
                    clone_with_suffix=suffix,
                )
                for n in shared_leg
            ]

            # Fix the destinations of the unique leg
            for n in unique_leg:
                n.destinations = destinations

            # Create the block
            # FIXME: To be 100% kosher we should also fix the next/prev ids and add the suffix
            if is_split:
                shared_leg_copy[-1].next = [unique_leg[0].trip_id]
                blocks.append(shared_leg_copy + unique_leg)
            else:
                shared_leg_copy[0].previous = [unique_leg[-1].trip_id]
                blocks.append(unique_leg + shared_leg_copy)

        return blocks

    def _find_linked_forward(
        self,
        nodes: dict[str, BlockNode],
        last: BlockNode,
    ) -> list[BlockNode]:
        tail: BlockNode | None = last
        visited = list[BlockNode]()
        while tail is not None:
            visited.append(tail)
            match tail.next:
                case []:
                    tail = None
                case [next_id]:
                    next_tail = nodes[next_id]
                    assert next_tail.previous == [tail.trip_id], "inconsistent trip links in data"
                    tail = next_tail
                case _:
                    raise ValueError("_find_linked_forward: encountered a disallowed fork")
        return visited

    def _find_linked_backward(
        self,
        nodes: dict[str, BlockNode],
        first: BlockNode,
    ) -> list[BlockNode]:
        head: BlockNode | None = first
        visited = list[BlockNode]()
        while head is not None:
            visited.append(head)
            match head.previous:
                case []:
                    head = None
                case [next_id]:
                    next_head = nodes[next_id]
                    assert next_head.next == [head.trip_id]
                    head = next_head
                case _:
                    raise ValueError("_find_linked_backward: encountered a disallowed fork")
        visited.reverse()
        return visited

    def assign_blocks(self, db: DBConnection, blocks: list[list[BlockNode]]) -> None:
        for block in blocks:
            block_id = str(self.counter)
            for node in block:
                if node.clone_with_suffix:
                    trip_id = node.trip_id + node.clone_with_suffix
                    self._clone_trip(db, trip_id, node.trip_id)
                else:
                    trip_id = node.trip_id

                db.raw_execute(
                    "UPDATE trips SET block_id = ?, "
                    "extra_fields_json = json_set(extra_fields_json, '$.destinations', ?, "
                    "                             '$.previous', ?, '$.next', ?) "
                    "WHERE trip_id = ?",
                    (
                        block_id,
                        ";".join(node.destinations),
                        ";".join(node.previous),
                        ";".join(node.next),
                        trip_id,
                    ),
                )

            self.counter += 1

    def _clone_trip(self, db: DBConnection, dst: str, src: str) -> None:
        db.raw_execute(
            "INSERT INTO trips (trip_id, route_id, calendar_id, short_name, direction, "
            "exceptional, extra_fields_json) SELECT ?, route_id, calendar_id, short_name, "
            "direction, exceptional, extra_fields_json FROM trips WHERE trip_id = ?",
            (dst, src),
        )
        db.raw_execute(
            "INSERT INTO stop_times (trip_id, stop_id, stop_sequence, arrival_time, "
            "departure_time) SELECT ?, stop_id, stop_sequence, arrival_time, departure_time "
            "FROM stop_times WHERE trip_id = ?",
            (dst, src),
        )
