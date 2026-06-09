# © Copyright 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import logging
import os
import random
import re
from collections import defaultdict
from collections.abc import Collection, Container, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import ceil
from operator import itemgetter
from typing import Any, Self, cast

import requests
from impuls import DBConnection, PipelineOptions, Task, TaskRuntime
from impuls.tools.types import StrPath

MAX_BATCH_SIZE = 1000
STALE_GROUP_AGE_DAYS = 90
MIN_REFRESH_AGE_DAYS = 2

IGNORE_NO_CACHE = bool(os.getenv("FORCE_SCRAPE_STOPS"))


logger = logging.getLogger("TokyuBusStopCache")


@dataclass
class CachedStopGroup:
    name: str
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    stops: dict[str, tuple[float, float]] = field(default_factory=dict[str, tuple[float, float]])

    def as_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "updated_at": self.updated_at.isoformat(),
            "stops": {code: list(pos) for code, pos in self.stops.items()},
        }

    @classmethod
    def from_json(cls, o: Mapping[str, Any]) -> Self:
        return cls(
            name=o["name"],
            updated_at=datetime.fromisoformat(o["updated_at"]),
            stops={code: (pos[0], pos[1]) for code, pos in o.get("stops", {}).items()},
        )

    def age_seconds(self, now: datetime | None = None) -> float:
        now = now or datetime.now(timezone.utc)
        return (now - self.updated_at).total_seconds()

    def age_days(self, now: datetime | None = None) -> int:
        return max(0, ceil(self.age_seconds(now) / 86400))

    def has_missing_stops(self, requested: Iterable[str]) -> bool:
        # XXX: This is not a simple `any(code not in self.stops)`, due to the weird ""
        #      codes. If "" is requested, not present in self.stops, we don't consider it missing
        #      if there's only one stop in the group. The task will deal with this stop by
        #      either updating the code or merging the code-less stop.
        for code in requested:
            if code and code not in requested:
                return True
            elif not code and len(self.stops) != 1:
                return True
        return False

    @classmethod
    def scrape(cls, group_id: str, session: requests.Session | None = None) -> Self:
        session = session or requests.session()

        with session.get(
            "https://transfer.navitime.biz/tokyubus/pc/diagram/BusAboardMap",
            params={"stCode": group_id},
        ) as r:
            # Using regular expressions to parse HTML and JavaScript :^)
            name_match = re.search(r"platform-title\">([^<\r\n]+)<", r.text)
            name = name_match[1] if name_match else ""
            self = cls(name)

            stop_matches = re.finditer(
                r"dispPole\(([0-9.]+),\s*([0-9.]+),\s*\"([^\"\t\r\n\\]+)\","
                r"\s*\"[^\"\t\r\n\\]+\",\s*\"([^\"\t\r\n\\]+)\",?\s*\)",
                r.text,
            )
            for match in stop_matches:
                # The website sometimes shows multiple groups at once.
                # Stops of the requested group will have the last parameter of distPole
                # set to "0".
                if match[4] == "0":
                    self.stops[match[3]] = float(match[1]), float(match[2])

            return self


@dataclass
class CachedStops:
    groups: dict[str, CachedStopGroup] = field(default_factory=dict[str, CachedStopGroup])

    def as_json(self) -> dict[str, Any]:
        return {
            "groups": {id: group.as_json() for id, group in self.groups.items()},
        }

    def dump(self, filename: StrPath) -> None:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(self.as_json(), f, separators=(",", ":"), ensure_ascii=False)

    @classmethod
    def from_json(cls, o: Mapping[str, Any]) -> Self:
        return cls(
            groups={
                id: CachedStopGroup.from_json(group) for id, group in o.get("groups", {}).items()
            }
        )

    @classmethod
    def load(cls, filename: StrPath) -> Self:
        try:
            with open(filename, "r", encoding="utf-8") as f:
                return cls.from_json(json.load(f))
        except FileNotFoundError:
            return cls()

    def get_groups_to_update(
        self,
        requested: Mapping[str, Iterable[str]],
        limit: int = MAX_BATCH_SIZE,
        now: datetime | None = None,
    ) -> set[str]:
        required = self.get_missing_groups(requested)
        extra = self.get_extra_groups_to_refresh(
            limit=limit - len(required),
            already_refreshed=required,
            now=now or datetime.now(timezone.utc),
        )
        logger.info(
            "%d groups to request - %d required, %d extra",
            len(required) + len(extra),
            len(required),
            len(extra),
        )
        return required.union(extra)

    def get_missing_groups(self, requested: Mapping[str, Iterable[str]]) -> set[str]:
        return {
            group_id
            for group_id, stop_codes in requested.items()
            if (group := self.groups.get(group_id)) is None or group.has_missing_stops(stop_codes)
        }

    def get_extra_groups_to_refresh(
        self,
        limit: int,
        already_refreshed: Container[str] = (),
        now: datetime | None = None,
    ) -> set[str]:
        extra = set[str]()
        if limit <= 0:
            return extra

        # Group all groups by their age; ignoring groups which are already going to be refreshed,
        # or which are too young to refresh
        by_age = defaultdict[int, list[str]](list)
        for group_id, group in self.groups.items():
            if group_id in already_refreshed:
                continue
            age = group.age_days(now)
            if age >= MIN_REFRESH_AGE_DAYS:
                by_age[age].append(group_id)

        # Add stop groups until we hit limit, starting with oldest
        by_age_ordered = sorted(by_age.items(), reverse=True, key=itemgetter(0))
        for _, groups in by_age_ordered:
            to_add = min(len(groups), limit)
            extra.update(random.sample(groups, to_add))
            limit -= to_add

            if limit <= 0:
                break

        return extra

    def purge_stale_groups(
        self,
        requested: Container[str],
        stale_age_days: int = STALE_GROUP_AGE_DAYS,
        now: datetime | None = None,
    ) -> None:
        stale = self.get_stale_groups(requested, stale_age_days, now)
        logger.info("Purging %d stale stop groups", len(stale))
        for group_id in stale:
            self.groups.pop(group_id)

    def get_stale_groups(
        self,
        requested: Container[str],
        stale_age_days: int = STALE_GROUP_AGE_DAYS,
        now: datetime | None = None,
    ) -> set[str]:
        return {
            group_id
            for group_id, group in self.groups.items()
            if group_id not in requested and group.age_days(now) > stale_age_days
        }

    def scrape(
        self,
        to_scrape: Collection[str],
        session: requests.Session | None = None,
    ) -> None:
        session = session or requests.session()
        for i, group_id in enumerate(to_scrape, start=1):
            # Log progress
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Scraping - %.2f %% - %d/%d - group %r",
                    100 * i / len(to_scrape),
                    i,
                    len(to_scrape),
                    group_id,
                )
            elif i % 50 == 0:
                logger.info("Scraping - %.2f - %d/%d", 100 * i / len(to_scrape), i, len(to_scrape))

            # Actually scrape the group
            self.groups[group_id] = CachedStopGroup.scrape(group_id, session)

        logger.info("Scraping - done")


class ScrapeTokyuBusStops(Task):
    def __init__(self, cache_file_name: str = "scraped_tokyu_bus_stops.json") -> None:
        super().__init__()
        self.cache_file_name = cache_file_name

    def execute(self, r: TaskRuntime) -> None:
        to_curate = self.list_required_stops(r.db)
        cache = self.get_updated_cache(to_curate, r.options)
        self.curate_stops(r.db, to_curate, cache)

    def list_required_stops(self, db: DBConnection) -> defaultdict[str, dict[str, str]]:
        grouped = defaultdict[str, dict[str, str]](dict)
        with db.raw_execute(
            "SELECT stop_id, code FROM stops WHERE stop_id LIKE 'TokyuBus.%'",
        ) as query:
            for row in query:
                stop_id = cast(str, row[0])
                code = cast(str, row[1])

                stop_id_parts = stop_id.split(".")
                if len(stop_id_parts) < 3:
                    raise ValueError(f"unable to extract group_id from stop_id {stop_id!r}")

                group_id = stop_id_parts[2]
                if not re.match(r"^[0-9]+$", group_id):
                    raise ValueError(f"unable to extract group_id from stop_id {stop_id!r}")

                grouped[group_id][code] = stop_id
        return grouped

    def get_updated_cache(
        self,
        to_curate: Mapping[str, Iterable[str]],
        options: PipelineOptions,
    ) -> CachedStops:
        cache_path = options.workspace_directory / self.cache_file_name
        cache = CachedStops.load(cache_path)
        if options.from_cache and not IGNORE_NO_CACHE:
            return cache

        cache.purge_stale_groups(to_curate)
        to_update = cache.get_groups_to_update(to_curate)
        cache.scrape(to_update)
        cache.dump(cache_path)
        return cache

    def curate_stops(
        self,
        db: DBConnection,
        to_curate: Mapping[str, Mapping[str, str]],
        cache: CachedStops,
    ) -> None:
        missing_stops = MissingCounter()
        missing_groups = MissingCounter()

        to_update = list[tuple[float, float, str]]()
        to_update_with_code = list[tuple[float, float, str, str]]()
        to_merge = list[tuple[str, str]]()

        for group_id, stop_code_to_id in to_curate.items():
            group = cache.groups.get(group_id)
            if group:
                missing_groups.add_ok()
                for code, stop_id in stop_code_to_id.items():
                    stop = group.stops.get(code)

                    if stop:
                        missing_stops.add_ok()
                        to_update.append((*stop, stop_id))

                    elif len(group.stops) == 1 and len(stop_code_to_id) == 1:
                        # XXX: ODPT will set the code to "" for stops with only one group,
                        #      even if those stops have an actual code.
                        missing_stops.add_ok()
                        code_override, stop = next(iter(group.stops.items()))
                        to_update_with_code.append((*stop, code_override, stop_id))

                    elif code == "" and len(group.stops) == 1 and len(stop_code_to_id) == 2:
                        # XXX: Sometimes ODPT will give two stops. Say, the website only has
                        #      "004267.a", but we have "004267.a" and "004267." in the DB.
                        #      We can merge "004267." into "004267.a".
                        missing_stops.add_ok()
                        id_override = first(id for code, id in stop_code_to_id.items() if code)
                        to_merge.append((id_override, stop_id))

                    else:
                        self.logger.warning("Missing data for stop %r", stop_id)
                        missing_stops.add_missing()

            else:
                missing_groups.add_missing()
                missing_stops.add_missing(len(stop_code_to_id))
                self.logger.warning("Missing data for group %r", group_id)

        with db.transaction():
            db.raw_execute_many("UPDATE stops SET lat = ?, lon = ? WHERE stop_id = ?", to_update)
            db.raw_execute_many(
                "UPDATE stops SET lat = ?, lon = ?, code = ? WHERE stop_id = ?",
                to_update_with_code,
            )
            db.raw_execute_many(
                "UPDATE stop_times SET stop_id = ? WHERE stop_id = ?",
                to_merge,
            )
            db.raw_execute_many(
                "DELETE FROM stops WHERE stop_id = ?", ((src,) for _, src in to_merge)
            )

        self.logger.info("Missing groups: %s", missing_groups)
        self.logger.info("Missing stops: %s", missing_stops)


@dataclass
class MissingCounter:
    total: int = 0
    missing: int = 0

    def __str__(self) -> str:
        return f"{self.missing} / {self.total} ({100 * self.missing / self.total:.2f} %)"

    def add_missing(self, count: int = 1) -> None:
        self.total += count
        self.missing += count

    def add_ok(self, count: int = 1) -> None:
        self.total += count


def first[T](it: Iterable[T]) -> T:
    return next(iter(it))
