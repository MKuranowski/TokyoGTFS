# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from pathlib import Path

API_TIMEOUT = 180

ATTRIBUTION_URL = "https://github.com/MKuranowski/TokyoGTFS/attributions.md"
DEFAULT_PUBLISHER_NAME = "Created using TokyoRailGTFS script (written by Mikołaj Kuranowski)"
DEFAULT_PUBLISHER_URL = "https://github.com/MKuranowski/TokyoRailGTFS"


RAIL_GTFS_HEADERS = {
    "agency.txt": ["agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang"],

    "stops.txt": ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon",
                  "location_type", "parent_station"],

    "routes.txt": ["agency_id", "route_id", "route_short_name", "route_long_name",
                   "route_type", "route_color", "route_text_color"],

    "calendar_dates.txt": ["service_id", "date", "exception_type"],

    "translations.txt": ["table_name", "field_name", "record_id", "language", "translation"],

    "trips.txt": ["route_id", "trip_id", "service_id", "trip_short_name",
                  "trip_headsign", "direction_id", "direction_name",
                  "block_id", "train_realtime_id"],

    "stop_times.txt": ["trip_id", "stop_sequence", "stop_id",
                       "platform", "arrival_time", "departure_time"],

    "attributions.txt": ["organization_name", "is_producer", "is_authority", "attribution_url"],
    "feed_info.txt": ["feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_version"]
}

BUS_GTFS_HEADERS = {
    "agency.txt": ["agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang"],

    "stops.txt": ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon"],

    "routes.txt": ["agency_id", "route_id", "route_short_name", "route_long_name",
                   "route_type", "route_color", "route_text_color"],

    "calendar_dates.txt": ["service_id", "date", "exception_type"],

    "translations.txt": ["table_name", "field_name", "record_id", "language", "translation"],

    "trips.txt": ["route_id", "trip_id", "service_id", "trip_headsign", "direction_id",
                  "wheelchair_accessible"],

    "stop_times.txt": ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"],

    "attributions.txt": ["organization_name", "is_producer", "is_authority", "attribution_url"],
    "feed_info.txt": ["feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_version"]
}

FUTURE_DAYS = 120
PROGRESS_STEP = 500

DIR_CACHE = Path("data_cached")
DIR_CURATED = Path("data_curated")
DIR_GTFS = Path("data_gtfs")


class Color:
    RESET = "\x1B[0m"
    BOLD = "\x1B[1m"
    DIM = "\x1B[2m"

    NON_BOLD = "\x1B[22m"

    RED = "\x1B[31m"
    GREEN = "\x1B[32m"
    YELLOW = "\x1B[33m"
    BLUE = "\x1B[34m"
    MAGENTA = "\x1B[35m"
    CYAN = "\x1B[36m"
