# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

GTFS_HEADERS = {
    "agency.txt": (
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_timezone",
        "agency_lang",
    ),
    "routes.txt": (
        "agency_id",
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_type",
        "route_color",
        "route_text_color",
    ),
    "stops.txt": (
        "stop_id",
        "stop_name",
        "tts_stop_name",
        "stop_lat",
        "stop_lon",
    ),
    "calendar_dates.txt": (
        "service_id",
        "date",
        "exception_type",
    ),
    "trips.txt": (
        "trip_id",
        "route_id",
        "service_id",
        "trip_headsign",
        "direction_id",
        "wheelchair_accessible",
    ),
    "stop_times.txt": (
        "trip_id",
        "stop_sequence",
        "stop_id",
        "arrival_time",
        "departure_time",
        "pickup_type",
        "drop_off_type",
        "stop_headsign",
    ),
    "attributions.txt": (
        "attribution_id",
        "organization_name",
        "attribution_url",
        "is_producer",
        "is_operator",
        "is_authority",
        "is_data_source",
    ),
    "feed_info.txt": (
        "feed_publisher_name",
        "feed_publisher_url",
        "feed_lang",
    ),
}
