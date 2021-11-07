# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections import defaultdict
import logging
from itertools import chain
from typing import Sequence, Set
import csv

from ..const import DIR_CURATED, DIR_GTFS, PROGRESS_STEP, Color
from ..err import MissingLocalData
from ..exporter import SimpleExporter
from ..other import CalendarHandler
from ..util import load_csv_as_mapping, text_color, time_to_str, first_part
from . import model


class Converter:
    def __init__(self, providers: Sequence[model.Provider], calendars: CalendarHandler,
                 translations: SimpleExporter):
        self.logger = logging.getLogger("Converter")
        self.providers = providers
        self.calendars = calendars
        self.translations = translations
        self.valid_stops: Set[model.StopID] = set()
        self.removed_trips_by_agency: defaultdict[model.AgencyID, int] = defaultdict(lambda: 0)

    def export_agencies(self) -> None:
        agency_data = load_csv_as_mapping(DIR_CURATED / "agencies.csv",
                                          lambda row: str(row["id"]),
                                          lambda row: row)

        agencies = frozenset(chain.from_iterable(p.provides for p in self.providers))
        missing_agencies = agencies.difference(agency_data)

        if missing_agencies:
            raise MissingLocalData("missing agency data for " +
                                   ", ".join(sorted(missing_agencies)))

        with SimpleExporter("agency", is_bus=True) as exporter:
            for agency_id in sorted(agencies):
                data = agency_data[agency_id]

                exporter.save({
                    "agency_id": agency_id,
                    "agency_name": data["name"],
                    "agency_url": data["url"],
                    "agency_timezone": "Asia/Tokyo",
                    "agency_lang": "ja"
                }),

                self.translations.save({
                    "table_name": "agency",
                    "field_name": "agency_name",
                    "record_id": agency_id,
                    "language": "en",
                    "translation": data["name_en"]
                })

    def export_routes(self) -> None:
        agency_colors = load_csv_as_mapping(
            DIR_CURATED / "bus_data.csv",
            lambda row: str(row["agency"]),
            lambda row: str(row["color"])
        )
        agency_text_colors = {
            agency: text_color(color) for (agency, color) in agency_colors.items()
        }

        with SimpleExporter("routes", is_bus=True) as exporter:
            for route in chain.from_iterable(p.routes() for p in self.providers):
                if route.agency not in agency_colors:
                    raise MissingLocalData(f"Agency {route.agency} missing in "
                                           "curated bus_data.csv")

                exporter.save({
                    "agency_id": route.agency,
                    "route_id": route.id,
                    "route_short_name": route.code,
                    "route_long_name": route.name or "",
                    "route_type": "3",
                    "route_color": agency_colors[route.agency],
                    "route_text_color": agency_text_colors[route.agency],
                })

    def export_stops(self) -> None:
        with SimpleExporter("stops", is_bus=True) as exporter:
            count = 0

            for stop in chain.from_iterable(p.stops() for p in self.providers):
                self.valid_stops.add(stop.id)

                exporter.save({
                    "stop_id": stop.id,
                    "stop_code": stop.code or "",
                    "stop_name": stop.name.ja,
                    "stop_lat": stop.lat,
                    "stop_lon": stop.lon,
                })

                if stop.name.en:
                    self.translations.save({
                        "table_name": "stops",
                        "field_name": "stop_name",
                        "record_id": stop.id,
                        "language": "en",
                        "translation": stop.name.en,
                    })

                count += 1
                if count % PROGRESS_STEP == 0:
                    self.logger.debug(f"{Color.DIM}Processed {count} stops {Color.RESET}")

    def export_trips(self) -> None:
        with SimpleExporter("trips", is_bus=True) as trips, \
                SimpleExporter("stop_times", is_bus=True) as times:
            count = 0

            for trip in chain.from_iterable(p.trips() for p in self.providers):
                self.export_trip(trips, times, trip)

                count += 1
                if count % PROGRESS_STEP == 0:
                    self.logger.debug(f"{Color.DIM}Processed {count} trips {Color.RESET}")

        for agency, count in sorted(self.removed_trips_by_agency.items()):
            self.logger.warn(f"{Color.YELLOW}Removed {count} {agency} trips due to invalid "
                             f"stops{Color.RESET}")

    def export_trip(self, trips: SimpleExporter, times: SimpleExporter, trip: model.Trip) -> bool:
        # Ensure valid service
        service = self.calendars.use(trip.route, trip.calendar)
        if not service:
            return False

        # Ensure all stops are valid
        valid_times = [t for t in trip.times if t.stop in self.valid_stops]

        if len(valid_times) <= 1:
            self.removed_trips_by_agency[first_part(trip.route)] += 1
            return False

        # Export to trips.txt
        # TODO: Translate headsigns
        trips.save({
            "route_id": trip.route,
            "trip_id": trip.id,
            "service_id": service,
            "trip_headsign": trip.headsign or "",
            "direction_id": trip.direction or "",
            "wheelchair_accessible": trip.gtfs_wheelchair_accessible,
        })

        for idx, time in enumerate(valid_times):
            times.save({
                "trip_id": trip.id,
                "stop_sequence": idx,
                "stop_id": time.stop,
                "arrival_time": time_to_str(time.arrival),
                "departure_time": time_to_str(time.departure),
            })

        return True

    def cleanup_trips_without_service(self) -> None:
        trips_to_remove: Set[model.TripID] = set()

        # Re-write trips
        trips_new = DIR_GTFS / "trips.txt"
        trips_old = DIR_GTFS / "trips.txt.old"
        trips_new.rename(trips_old)

        with trips_old.open(mode="r", encoding="utf-8", newline="") as old_buff, \
                trips_new.open(mode="w", encoding="utf-8", newline="") as new_buff:
            reader = csv.DictReader(old_buff)
            assert reader.fieldnames
            writer = csv.DictWriter(new_buff, reader.fieldnames)
            writer.writeheader()

            for r in reader:
                if r["service_id"] in self.calendars.exported:
                    writer.writerow(r)
                else:
                    trips_to_remove.add(r["trip_id"])

        trips_old.unlink()

        # Re-write stop_times.txt
        times_new = DIR_GTFS / "trips.txt"
        times_old = DIR_GTFS / "trips.txt.old"
        times_new.rename(times_old)

        with times_old.open(mode="r", encoding="utf-8", newline="") as old_buff, \
                times_new.open(mode="w", encoding="utf-8", newline="") as new_buff:
            reader = csv.DictReader(old_buff)
            assert reader.fieldnames
            writer = csv.DictWriter(new_buff, reader.fieldnames)
            writer.writeheader()

            for r in reader:
                if r["trip_id"] not in trips_to_remove:
                    writer.writerow(r)

        times_old.unlink()

        self.logger.info(f"Removed {len(trips_to_remove)} trips due to invalid services")
