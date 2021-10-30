# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import json
import logging
from typing import IO, Iterable, List, Mapping, Optional

from ..const import DIR_CACHE, PROGRESS_STEP, Color
from ..util import IJsonIterator
from . import model
from .blocksolver import BlockSolver

logger = logging.getLogger("Mux")


class Cache:
    def __init__(self, name: str) -> None:
        self.f: Optional[IO[str]] = None
        self.path = DIR_CACHE / (name + ".json")
        self.first_train = True
        self.train_count = 0

    def start_saving(self) -> None:
        self.f = self.path.open(mode="w", encoding="utf-8")
        self.f.write("[\n")

    def finish_saving(self) -> None:
        assert self.f
        self.f.write("\n]\n")
        self.f.close()
        self.f = None

    def save_train(self, t: model.Train) -> None:
        assert self.f
        json_train = json.dumps(t.as_json(), ensure_ascii=False, indent=2)

        # Add a trailing comma
        if not self.first_train:
            self.f.write(",\n")
        else:
            self.first_train = False

        # Add another indent level
        self.f.write("\n".join("  " + line for line in json_train.split("\n")))

        self.train_count += 1

    def get_trains(self) -> Iterable[model.Train]:
        return map(
            model.Train.from_json,
            IJsonIterator(self.path.open(mode="r", encoding="utf-8"))
        )


def multiplex_trains(providers: List[model.Provider], solvers: Mapping[str, BlockSolver],
                     route_to_through_group: Mapping[str, str]) -> Mapping[str, Cache]:
    """Takes trains from every provider and pipes them to appropriate BlockSolver
    and writes those trains to the cache.
    """
    caches: dict[str, Cache] = {}

    for provider in providers:
        cache = Cache(provider.name)
        caches[provider.name] = cache
        cache.start_saving()

        try:
            multiplex_trains_of_provider(provider.trains(), cache, solvers,
                                         route_to_through_group, provider.name)
        finally:
            cache.finish_saving()

    return caches


def multiplex_trains_of_provider(trains: Iterable[model.Train], cache: Cache,
                                 solvers: Mapping[str, BlockSolver],
                                 route_to_through_group: Mapping[str, str],
                                 provider_name: str) -> None:
    logger.debug(f"{Color.DIM}Received 0 trains from provider {provider_name}{Color.RESET}")

    for train in trains:
        cache.save_train(train)
        through_group = route_to_through_group.get(train.route)

        if through_group:
            solver = solvers[through_group]
            solver.add_train(train)

        # Logging stuff
        if cache.train_count % PROGRESS_STEP == 0:
            logger.debug(f"{Color.DIM}Received {cache.train_count} trains from provider "
                         f"{provider_name}{Color.RESET}")


def multiplex_trains_from_cache(
        providers: List[model.Provider],
        solvers: Mapping[str, BlockSolver],
        route_to_through_group: Mapping[str, str]) -> Mapping[str, Cache]:
    """Same as multiplex_trains, but instead of getting trains directly
    from providers - trains are generated from the already cached schedules."""
    caches = {p.name: Cache(p.name) for p in providers}

    for cache in caches.values():
        for train in cache.get_trains():
            cache.train_count += 1
            through_group = route_to_through_group.get(train.route)

            if through_group:
                solver = solvers[through_group]
                solver.add_train(train)

    return caches
