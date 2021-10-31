# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import logging
from contextlib import closing
from datetime import datetime

from pytz import timezone

from ..const import RAIL_GTFS_HEADERS, Color, DIR_CURATED
from ..util import first_part, compress_gtfs
from .blocksolver import BlockSolver
from .converter import Converter
from ..exporter import SimpleExporter
from .geo import StationHandler, station_names
from .model import ConvertOptions
from .mux import (Cache, multiplex_trains, multiplex_trains_from_cache,
                  multiplex_trains_of_provider)
from .other import (CalendarHandler, RouteData, export_agencies,
                    export_attribution, export_feedinfo, export_routes)
from .providers import (get_all_providers, get_provider_by_name,
                        get_provider_for)

logger = logging.getLogger("Main")


def create_gtfs(opts: ConvertOptions) -> int:
    logger.info("Getting providers")
    providers = get_all_providers()
    creation_time = datetime.now(timezone("Asia/Tokyo"))

    logger.info("Creating helper objects")
    translations = SimpleExporter("translations")
    calendars = CalendarHandler(opts.start_date, opts.end_date)
    stations = StationHandler()

    # Load station data
    logger.info("Loading stations")
    stations.load(providers)

    # Load calendar data
    logger.info("Loading calendars")
    calendars.load(providers)

    # Load route data
    logger.info("Loading route data")
    with (DIR_CURATED / "rail_routes.csv").open(mode="r", encoding="utf-8", newline="") as f:
        route_data = RouteData.fromcsv(f)

    route_to_through_group = {
        r.id: r.through_group
        for r in route_data.values()
        if r.through_group
    }

    block_solvers = {
        group_name: BlockSolver(group_name)
        for group_name in set(route_to_through_group.values())
    }

    # Solve blocks
    if opts.from_cache:
        logger.info("Getting trains (from cache)")
        caches = multiplex_trains_from_cache(
            providers,
            block_solvers,
            route_to_through_group
        )
    else:
        logger.info("Getting trains")
        caches = multiplex_trains(
            providers,
            block_solvers,
            route_to_through_group,
        )

    for solver in block_solvers.values():
        logger.info(f"Solving blocks of group {solver.prefix}")
        solver.solve()

    # Create the converter
    converter = Converter(calendars, stations, block_solvers, translations)
    converter.route_data = route_data

    logger.info("Loading train types")
    converter.load_train_types(providers)

    logger.info("Exporting trains")
    converter.save_trains(caches.values())

    logger.info("Exporting stations")
    with SimpleExporter("stops") as e:
        stations.export(e, translations)

    logger.info("Exporting calendars")
    with SimpleExporter("calendar_dates") as e:
        calendars.export(e)

    logger.info("Exporting other files")
    export_agencies(converter.used_agencies, translations)
    export_routes(converter.used_routes, route_data, translations)
    export_attribution(filter(None, (p.attribution for p in providers)))
    export_feedinfo(creation_time, opts.publisher_name, opts.publisher_url)
    translations.close()
    converter.trips.close()
    converter.times.close()

    logger.info(f"Compressing to {opts.target}")
    compress_gtfs(target=opts.target, files=RAIL_GTFS_HEADERS)

    logger.info(f"{Color.GREEN}Done!{Color.RESET}")
    return 0


def dump_provider(provider_name: str) -> int:
    provider = get_provider_by_name(provider_name)
    cache = Cache(provider_name)
    cache.start_saving()
    try:
        multiplex_trains_of_provider(provider.trains(), cache, {}, {}, provider_name)
    finally:
        cache.finish_saving()
    logger.info(f"Received {Color.BOLD}{cache.train_count}{Color.RESET} trains from "
                f"provider {provider_name}")
    return 0


def check_geo(validate_prefix: str) -> int:
    if validate_prefix:
        providers = [get_provider_for(first_part(validate_prefix))]
    else:
        providers = get_all_providers()

    count = StationHandler().load(providers, validate_prefix)
    logger.info(f"{Color.GREEN}Validated {Color.CYAN}{count}{Color.GREEN} stations "
                f"successfully{Color.RESET}")
    return 0


def check_names() -> int:
    ok = station_names.find_duplicates()
    return 0 if ok else 1
