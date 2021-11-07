# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import logging
from datetime import datetime
from itertools import chain

from pytz import timezone

from ..const import BUS_GTFS_HEADERS, Color
from ..exporter import SimpleExporter
from ..other import CalendarHandler, export_attribution, export_feedinfo
from ..util import compress_gtfs
from .converter import Converter
from .model import ConvertOptions
from .providers import get_all_providers

logger = logging.getLogger("Main")


def create_gtfs(opts: ConvertOptions) -> int:
    logger.info("Getting providers")
    providers = get_all_providers()
    creation_time = datetime.now(timezone("Asia/Tokyo"))

    logger.info("Creating helper objects")
    with SimpleExporter("translations") as translations:
        calendars = CalendarHandler(opts.start_date, opts.end_date)
        converter = Converter(providers, calendars, translations)

        logger.info("Loading calendars")
        calendars.load(chain.from_iterable(p.calendars() for p in providers))

        logger.info("Saving agencies")
        converter.export_agencies()

        logger.info("Saving routes")
        converter.export_routes()

        logger.info("Saving stops")
        converter.export_stops()

        logger.info("Saving trips")
        converter.export_trips()

        logger.info("Saving calendars")
        with SimpleExporter("calendar_dates", is_bus=True) as e:
            calendars.export(e)

    logger.info("Cleaning up trips")
    converter.cleanup_trips_without_service()

    logger.info("Saving feed_info and attributions")
    export_feedinfo(creation_time, opts.publisher_name, opts.publisher_url)
    export_attribution(p.attribution for p in providers)

    for p in providers:
        p.close()

    logger.info(f"Compressing to {opts.target}")
    compress_gtfs(target=opts.target, files=BUS_GTFS_HEADERS)

    logger.info(f"{Color.GREEN}Done!{Color.RESET}")

    return 0


def count_stops() -> int:
    for p in get_all_providers():
        print(f"=== Provider: {p.name} ===")
        valid, invalid = p.count_stops()
        agencies = set(chain.from_iterable((valid, invalid)))

        for agency in sorted(agencies):
            total = valid[agency] + invalid[agency]
            print(
                f"{agency}: {valid[agency]} valid; {invalid[agency]} "
                f"({invalid[agency]/total:.2%}) invalid"
            )

    return 0
