# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import logging
from itertools import chain
from typing import Any, Callable, Collection, Iterable, List, Mapping, Optional

from ..const import PROGRESS_STEP, Color
from ..exporter import SimpleExporter
from ..other import CalendarHandler
from ..util import last_part, time_to_str
from . import model
from .blocksolver import BlockSolver
from .geo import StationHandler
from .mux import Cache
from .other import RouteData

BlockGetter = Callable[[model.TrainID], List[str]]


class Converter:
    def __init__(self, calendars: CalendarHandler, stations: StationHandler,
                 block_solvers: Mapping[str, BlockSolver], translations: SimpleExporter):
        self.logger = logging.getLogger("Converter")

        # Handlers
        self.calendars = calendars
        self.block_solvers = block_solvers
        self.stations = stations
        self.translations = translations

        # Exporters
        self.trips = SimpleExporter("trips")
        self.times = SimpleExporter("stop_times")

        # State of the converter
        self.used_agencies: set[str] = set()
        self.used_routes: set[str] = set()
        self.primary_direction_ids: dict[model.RouteID, str] = {}

        # Data
        self.route_data: dict[model.RouteID, RouteData] = {}
        self.train_types: dict[model.TrainTypeID, model.Name] = {
            "Local": model.Name(ja="普通", en="Local"),
        }

    def load_train_types(self, providers: Iterable[model.Provider]) -> None:
        for train_type in chain.from_iterable(p.train_types() for p in providers):
            self.train_types[train_type.id] = train_type.name

    def block_getter(self, route: model.RouteID) -> BlockGetter:
        through_group = self.route_data[route].through_group
        if not through_group:
            return lambda _: []
        else:
            return self.block_solvers[through_group].get_block

    def get_station_name(self, station_id: model.StationID) -> model.Name:
        station_name_id = last_part(station_id)
        name = self.stations.names.get(station_name_id)

        if name:
            return name

        else:
            self.logger.warn(f"{Color.YELLOW}No station name for {Color.MAGENTA}"
                             f"{station_name_id}{Color.RESET}")
            return model.Name(station_name_id, station_name_id)

    def generate_short_name(self, t: model.Train) -> Optional[model.Name]:
        if t.train_number and t.train_name:
            return model.Name(
                f"{t.train_number} {t.train_name.ja}",
                f"{t.train_number} {t.train_name.en}",
            )

        elif t.train_name:
            return t.train_name

        elif t.train_number:
            return model.Name(t.train_number, t.train_number)

        return None

    def generate_headsign(self, t: model.Train) -> model.Name:
        dest_str = model.Name(
            "・".join(self.get_station_name(s).ja for s in t.destinations),
            " / ".join(self.get_station_name(s).en for s in t.destinations),
        )

        # XXX: Special case for the Yamanote line where the destination
        #      and train type is not important
        if t.route == "JR-East.Yamanote" and t.direction in {"InnerLoop", "OuterLoop"}:
            if t.destinations and t.direction == "InnerLoop":
                return model.Name(f"（内回り ⟲）{dest_str.ja}", f"(Inner Loop ⟲) {dest_str.en}")

            elif t.destinations and t.direction == "OuterLoop":
                return model.Name(f"（外回り ⟳）{dest_str.ja}", f"(Outer Loop ⟳) {dest_str.en}")

            elif t.direction == "InnerLoop":
                return model.Name("内回り ⟲", "Inner Loop ⟲")

            else:
                return model.Name("外回り ⟳", "Outer Loop ⟳")

        type = self.train_types[t.train_type]
        return model.Name(f"（{type.ja}）{dest_str.ja}", f"({type.en}) {dest_str.en}")

    def save_translation(self, record_id: model.TrainID, field: str,
                         name: Optional[model.Name]) -> None:
        if not name:
            return

        self.translations.save({
            "table_name": "trips",
            "field_name": field,
            "record_id": record_id,
            "language": "en",
            "translation": name.en,
        })

    def save_train(self, t: model.Train) -> None:
        # Ensure route_data is loaded
        assert self.route_data, "Converter.route_data must be set before " \
                                "calling Converter.save_train"

        # Check calendar validity
        service = self.calendars.use(t.route, t.calendar)
        if not service:
            return

        short_name = self.generate_short_name(t)
        headsign = self.generate_headsign(t)

        # Basic meta-data
        gtfs_train: dict[str, Any] = {
            "route_id": t.route,
            "trip_id": t.id,
            "service_id": service,
        }

        # Short name
        if short_name:
            gtfs_train["trip_short_name"] = short_name.ja

        # Headsign
        gtfs_train["trip_headsign"] = headsign.ja

        # Direction
        if t.direction:
            # Figure out the direction_id
            route_primary_dir = self.primary_direction_ids.get(t.route)

            if route_primary_dir is None:
                self.primary_direction_ids[t.route] = t.direction
                direction_id = "0"

            elif route_primary_dir == t.direction:
                direction_id = "0"

            else:
                direction_id = "1"

            gtfs_train["direction_id"] = direction_id
            gtfs_train["direction_name"] = t.direction

        if t.realtime_id:
            gtfs_train["train_realtime_id"] = t.realtime_id

        # Stop_times
        gtfs_stop_times: list[dict[str, Any]] = []

        for idx, ttable_entry in enumerate(t.timetable):
            # Export only valid stop times
            # FIXME: Some of this data can be filled in from through service data
            if ttable_entry.arrival < 0 or ttable_entry.departure < 0:
                continue

            self.stations.mark_used(ttable_entry.station)
            gtfs_stop_times.append({
                "trip_id": t.id,
                "stop_sequence": idx,
                "stop_id": ttable_entry.station,
                "platform": ttable_entry.platform,
                "arrival_time": time_to_str(ttable_entry.arrival),
                "departure_time": time_to_str(ttable_entry.departure),
            })

        # Mark agency and route as used
        self.used_agencies.add(t.agency)
        self.used_routes.add(t.route)

        # Export n trips, depending on the amount of blocks
        # this train belongs to.
        blocks = self.block_getter(t.route)(t.id)

        if len(blocks) <= 1:
            if blocks:
                gtfs_train["block_id"] = blocks[0]

            self.trips.save(gtfs_train)
            self.times.save_many(gtfs_stop_times)
            self.save_translation(t.id, "trip_headsign", headsign)
            self.save_translation(t.id, "trip_short_name", short_name)

        else:
            for block_id in blocks:
                trip_id = f"{t.id}.Block{block_id}"

                gtfs_train["block_id"] = block_id
                gtfs_train["trip_id"] = trip_id
                for stop_time in gtfs_stop_times:
                    stop_time["trip_id"] = trip_id

                self.trips.save(gtfs_train)
                self.times.save_many(gtfs_stop_times)
                self.save_translation(trip_id, "trip_headsign", headsign)
                self.save_translation(trip_id, "trip_short_name", short_name)

    def save_trains(self, caches: Collection[Cache]) -> None:
        last_log = trains_left = total_trains = sum(c.train_count for c in caches)

        self.logger.debug(f"{Color.DIM}Saving trains - {trains_left} left"
                          f"(0.00% done){Color.RESET}")

        for train in chain.from_iterable(c.get_trains() for c in caches):
            self.save_train(train)
            trains_left -= 1

            if last_log - trains_left > PROGRESS_STEP:
                last_log = trains_left
                done_ratio = last_log / total_trains
                self.logger.debug(f"{Color.DIM}Saving trains - {trains_left} left"
                                  f"({1-done_ratio:.2%} done){Color.RESET}")
