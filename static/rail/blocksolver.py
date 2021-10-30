# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections import defaultdict
from dataclasses import dataclass, field
from logging import getLogger
from typing import Dict, List, NamedTuple, Optional, Type, TypeVar

from ..const import PROGRESS_STEP, Color
from ..err import BlockError
from ..util import last_part
from . import model

T = TypeVar("T")

WARN_REFERENCE = f"{Color.YELLOW}Train {Color.MAGENTA}{{id1}}{Color.YELLOW} " \
                 f"references {{dir}} train {Color.MAGENTA}{{id2}}{Color.YELLOW} - " \
                 f"which does not exist or was used in a different block{Color.RESET}"
WARN_MULTIPLE_HASHES = f"{Color.YELLOW}Train {Color.MAGENTA}{{id}}{Color.YELLOW} has multiple " \
                       f"possible {{dir}} trains when matching on StopTimeHash{Color.RESET}"


class StopTimeHash(NamedTuple):
    """NamedTuple used to represent a StopTime event.
    Used for matching one train's last station to another train's first station."""
    calendar: model.CalendarID
    station: model.StationID
    destination: model.StationID
    time: int

    @classmethod
    def first_of_train(cls: Type[T], train: model.Train) -> T:
        """Creates a StopTimeHash from the first ttable entry of a model.Train"""
        first_tt_entry = train.timetable[0]
        return cls(
            train.calendar,
            last_part(first_tt_entry.station),
            ";".join(last_part(i) for i in train.destinations),
            first_tt_entry.arrival
        )

    @classmethod
    def last_of_train(cls: Type[T], train: model.Train) -> T:
        """Creates a StopTimeHash from the last ttable entry of a model.Train"""
        last_tt_entry = train.timetable[-1]
        if last_tt_entry.departure < 0:
            last_tt_entry = train.timetable[-2]
        return cls(
            train.calendar,
            last_part(last_tt_entry.station),
            ";".join(last_part(i) for i in train.destinations),
            last_tt_entry.arrival
        )


@dataclass
class TrainShort:
    """Simplification of a model.Train with data only useful for block solving."""
    id: model.TrainID
    route: model.RouteID
    calendar: model.CalendarID
    first_sta: StopTimeHash
    last_sta: StopTimeHash
    destinations: List[model.StationID]
    is_last: bool
    origins: Optional[List[model.StationID]] = None
    is_first: Optional[bool] = None
    prev: Optional[List[model.TrainID]] = None
    next: Optional[List[model.TrainID]] = None

    @classmethod
    def from_model(cls: Type[T], train: model.Train) -> T:
        """Shortens a train from a full model train"""
        is_last = [train.timetable[-1].station] == train.destinations
        is_first: Optional[bool] = None
        if train.origins is not None:
            is_first = [train.timetable[0].station] == train.origins

        return cls(
            id=train.id,
            route=train.route,
            calendar=train.calendar,
            first_sta=StopTimeHash.first_of_train(train),
            last_sta=StopTimeHash.last_of_train(train),
            destinations=[last_part(i) for i in train.destinations],
            is_last=is_last,
            origins=[last_part(i) for i in train.origins] if train.origins else None,
            is_first=is_first,
            prev=train.previous_timetable,
            next=train.next_timetable,
        )


@dataclass
class BlockNode:
    """Represents a node in a graph of connected trains, connected by through-service"""
    train: TrainShort
    next: List["BlockNode"] = field(default_factory=list)
    prev: List["BlockNode"] = field(default_factory=list)

    def blocks_up_to(self) -> List[List["BlockNode"]]:
        """Recursively finds all blocks up to and including this node"""
        if not self.prev:
            # Base case - no previous trains
            blocks = [[]]
        else:
            # Recursive case - find all previous trains
            blocks = []
            for prev_train in self.prev:
                blocks.extend(prev_train.blocks_up_to())

        # Add self to all blocks
        for block in blocks:
            block.append(self)

        return blocks


class BlockSolver:
    """BlockSolver is a class that attempts to link trains with
    through service into GTFS blocks."""
    def __init__(self, prefix: str) -> None:
        self.logger = getLogger("BlockSolver." + prefix)
        self.trains_by_id: dict[model.TrainID, TrainShort] = {}
        self.trains_by_first_sta: defaultdict[StopTimeHash, List[model.TrainID]] = \
            defaultdict(list)
        self.trains_by_last_sta: defaultdict[StopTimeHash, List[model.TrainID]] = defaultdict(list)
        self.block_id = 1
        self.prefix = prefix

        self.blocks: defaultdict[model.TrainID, List[int]] = defaultdict(list)

    def add_train(self, train: model.Train) -> None:
        """Adds data about a train for further matching"""
        short = TrainShort.from_model(train)

        # Skip trains without through service
        if short.is_first and short.is_last:
            return

        self.trains_by_id[short.id] = short

        # XXX: Hotfix for trains with only one stations, like
        #      JR-East.ChuoRapid.2622M.Weekday
        if len(train.timetable) > 1:
            self.trains_by_first_sta[short.first_sta].append(short.id)
            self.trains_by_last_sta[short.last_sta].append(short.id)

    def drop_train(self, train: TrainShort) -> None:
        """Removes a train from storage, used after its blocks were assigned"""
        del self.trains_by_id[train.id]

        if len(self.trains_by_first_sta[train.first_sta]) == 1:
            del self.trains_by_first_sta[train.first_sta]
        else:
            try:
                self.trains_by_first_sta[train.first_sta].remove(train.id)
            except ValueError:
                pass

        if len(self.trains_by_last_sta[train.last_sta]) == 1:
            del self.trains_by_last_sta[train.last_sta]
        else:
            try:
                self.trains_by_last_sta[train.last_sta].remove(train.id)
            except ValueError:
                pass

    def solve(self) -> None:
        """Solve blocks of all saved trains"""
        # Logging stuff
        total_trains = len(self.trains_by_id)
        last_log = total_trains
        self.logger.debug(f"{Color.DIM}Solving - {total_trains} trains left "
                          f"(0.00% done){Color.RESET}")

        while self.trains_by_id:
            # Why is there no nicer way to get the first element of a dict
            # without removing it :'(
            train = next(iter(self.trains_by_id.values()))
            self.solve_train(train)

            if last_log - len(self.trains_by_id) > PROGRESS_STEP:
                last_log = len(self.trains_by_id)
                done_ratio = last_log / total_trains
                self.logger.debug(f"{Color.DIM}Solving - {last_log} trains left "
                                  f"({1-done_ratio:.2%} done){Color.RESET}")

    def solve_train(self, train: TrainShort) -> None:
        """Solve blocks of a particular train, and afterwards
        remove it (and others with through-service) from the BlockSolver."""
        visited: dict[model.TrainID, BlockNode] = {}
        root_node = BlockNode(train)

        # Expand the node
        self.expand_previous(root_node, visited)
        self.expand_next(root_node, visited)

        # No through service - don't do anything
        if len(visited) < 2:
            self.drop_train(train)
            return

        # Find the vary last trains while also removing visited trains from the solver
        last_trains: List[BlockNode] = []
        splits = 0
        merges = 0

        for node in visited.values():
            self.drop_train(node.train)

            if not node.next:
                last_trains.append(node)
            if len(node.prev) > 1:
                merges += 1
            if len(node.next) > 1:
                splits += 1

        # Verify linear blocks or only with one split/merge
        if splits > 2 or splits > 2 or (merges > 1 and splits > 1):
            raise BlockError(f"block around {train.id} has too many splits ({splits}) and or "
                             f"merges ({merges})")

        # Generate block_ids
        for last_train in last_trains:
            for block in last_train.blocks_up_to():
                for node in block:
                    self.blocks[node.train.id].append(self.block_id)
                self.block_id += 1

    def expand_previous(self, node: BlockNode, visited: Dict[model.TrainID, BlockNode],
                        ignore_train: model.TrainID = "") -> None:
        """Takes a node and recursively expands its previous trains.
        `node.prev` should either be empty, or contain a single train
        (done when recursively expanding in the opposite direction),
        whose ID should be provided in the `ignore_train` to avoid infinite recursion.
        """
        visited[node.train.id] = node
        prev_trains = self.previous_trains(node.train)

        # Nothing to do, rewind up
        if not prev_trains:
            return

        for prev_train_id in prev_trains:
            # Check agains ignore_train
            if prev_train_id == ignore_train:
                continue

            # Guard agains circular references
            if prev_train_id in visited:
                raise BlockError(f"Expanding block around train {node.train.id} - circular "
                                 f" reference to {prev_train_id}")

            # Create new block for the prev_train and link it up
            prev_train = self.trains_by_id.get(prev_train_id)

            if not prev_train:
                self.logger.warn(WARN_REFERENCE.format(id1=node.train.id, dir="prev",
                                                       id2=prev_train_id))
                continue

            new_node = BlockNode(prev_train, next=[node])
            node.prev.append(new_node)

            # Recursively expand the new node
            self.expand_previous(new_node, visited)

            # Try to expand the new node forwards, ignoring current node.
            # This is to recurse into branches we didn't come from -
            # marked in bold on this diagram:
            # new_node ━━━┱───  node
            #             ┗━━━→ separate_branch
            self.expand_next(new_node, visited, node.train.id)

    def expand_next(self, node: BlockNode, visited: Dict[model.TrainID, BlockNode],
                    ignore_train: model.TrainID = "") -> None:
        """Takes a node and recursively expands its next trains.
        `node.next` should either be empty, or contain a single train
        (done when recursively expanding in the opposite direction),
        whose ID should be provided in the `ignore_train` to avoid infinite recursion.
        """
        visited[node.train.id] = node
        next_trains = self.next_trains(node.train)

        # Nothing to do, rewind up
        if not next_trains:
            return

        for next_train_id in next_trains:
            # Check agains ignore_train
            if next_train_id == ignore_train:
                continue

            # Guard agains circular references
            if next_train_id in visited:
                raise BlockError(f"Expanding block around train {node.train.id} - circular "
                                 f"circular reference to {next_train_id}")

            # Create new block for the prev_train and link it up
            next_train = self.trains_by_id.get(next_train_id)

            if not next_train:
                self.logger.warn(WARN_REFERENCE.format(id1=node.train.id, dir="next",
                                                       id2=next_train_id))
                continue

            new_node = BlockNode(next_train, prev=[node])
            node.next.append(new_node)

            # Recursively expand the new node
            self.expand_next(new_node, visited)

            # Try to expand the new node backwards, ignoring current node.
            # This is to recurse into branches we didn't come from -
            # marked in bold on this diagram:
            #             node  ───┲━━━ new_node
            #  separate_branch ←━━━┛
            self.expand_previous(new_node, visited, node.train.id)

    def previous_trains(self, train: TrainShort) -> List[model.TrainID]:
        """Tries to find all immediately preceding trains for given train."""
        if train.is_first:
            return []

        matches: List[model.TrainID] = []
        hashes = self.trains_by_last_sta.get(train.first_sta)

        if train.prev is not None:
            # Train had nicely defined previousTrainTimetable field (or equivalent)
            matches = train.prev

        elif hashes:
            if len(hashes) > 1:
                self.logger.warn(WARN_MULTIPLE_HASHES.format(id=train.id, dir="prev"))
            else:
                matches = hashes

        # Fallback mechanism - check if this train is mentioned in any other train's next trains
        # Wasn't useful on ODPT data alone.
        # if not matches:
        #     for other in self.trains_by_id.values():
        #         if other.next and train.id in other.next:
        #             matches.append(other.id)

        #     if matches:
        #         self.logger.info(f"Train {Color.CYAN}{train.id}{Color.RESET} - fallback mechanism "
        #                          "when looking for previous trains was useful :^)")

        return matches

    def next_trains(self, train: TrainShort) -> List[model.TrainID]:
        """Tries to find all immediately following trains for given train."""
        if train.is_last:
            return []

        matches: List[model.TrainID] = []
        hashes = self.trains_by_first_sta.get(train.last_sta)

        if train.next is not None:
            # Train had nicely defined previousTrainTimetable field (or equivalent)
            matches = train.next

        elif hashes:
            if len(hashes) > 1:
                self.logger.warn(WARN_MULTIPLE_HASHES.format(id=train.id, dir="next"))
            else:
                matches = hashes

        # Fallback mechanism - check if this train is mentioned in any other train's next trains
        # Wasn't useful on ODPT data alone
        # if not matches:
        #     for other in self.trains_by_id.values():
        #         if other.prev and train.id in other.prev:
        #             matches.append(other.id)

        #     if matches:
        #         self.logger.info(f"Train {Color.CYAN}{train.id}{Color.RESET} - fallback mechanism "
        #                          "when looking for next trains was useful :^)")

        return matches

    def get_block(self, id: model.TrainID) -> List[str]:
        """Check to which blocks this train belongs."""
        if id in self.blocks:
            return [f"{self.prefix}.{block_no}" for block_no in self.blocks[id]]
        else:
            return []
