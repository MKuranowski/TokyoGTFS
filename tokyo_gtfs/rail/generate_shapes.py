from collections.abc import Mapping
from itertools import pairwise
from logging import Logger
from pathlib import Path
from typing import NamedTuple, cast

import pyroutelib3
from impuls import DBConnection, Task, TaskRuntime
from impuls.resource import ManagedResource

type Stops = tuple["Stop", ...]
type Distances = tuple[float, ...]


class Trip(NamedTuple):
    id: str
    stop_times: Stops


class Stop(NamedTuple):
    idx: int
    id: str


class Shape(NamedTuple):
    id: str
    distances: Distances


class GenerateShapes(Task):
    def __init__(self) -> None:
        super().__init__()
        self.shape_id_counter = 0
        self.stop_positions = dict[str, tuple[float, float]]()

    def execute(self, r: TaskRuntime) -> None:
        self.shape_id_counter = 0
        self.stop_positions = load_stop_positions(r.db)
        graph_path = r.resources["rail_geo.osm"].stored_at
        routes = load_routes_with_geo(r.resources["routes.csv"])

        with r.db.transaction():
            for route_id in routes:
                self.generate_shapes_for_route(r.db, route_id, graph_path)

    def generate_shapes_for_route(self, db: DBConnection, route_id: str, graph_path: Path) -> None:
        self.logger.debug("Generating shapes for %s", route_id)
        generator = ShapeGenerator(
            self.logger.getChild(route_id),
            route_id,
            graph_path,
            self.stop_positions,
            self.shape_id_counter,
        )

        for trip_id, stops in load_trips(db, route_id):
            shape_id, distances = generator.get_shape(db, stops)
            set_shape_on_trip(db, trip_id, shape_id, stops, distances)

        self.shape_id_counter = generator.shape_id_counter


class ShapeGenerator:
    def __init__(
        self,
        logger: Logger,
        route_id: str,
        graph_path: Path,
        stop_positions: Mapping[str, tuple[float, float]],
        shape_id_counter: int,
    ) -> None:
        self.logger = logger
        self.graph = create_graph(graph_path, route_id)
        self.stop_positions = stop_positions
        self.shape_id_counter = shape_id_counter

        self.cached_shapes = dict[Stops, Shape]()
        self.cached_nodes = dict[str, int]()

    def get_shape(self, db: DBConnection, stops: Stops) -> Shape:
        if cached := self.cached_shapes.get(stops):
            return cached

        shape = self.generate_and_insert_shape(db, stops)
        self.cached_shapes[stops] = shape
        return shape

    def generate_and_insert_shape(self, db: DBConnection, stops: Stops) -> Shape:
        shape_id = str(self.shape_id_counter)
        self.shape_id_counter += 1
        db.raw_execute("INSERT INTO shapes (shape_id) VALUES (?)", (shape_id,))

        legs = (self.get_leg_shape(a.id, b.id) for a, b in pairwise(stops))
        distances = [0.0]
        total_idx = 0
        total_dist = 0.0

        for leg in legs:
            # The first point of a leg is the same as the last point of the previous leg,
            # so we don't write it to avoid duplicates. The exception is the very first leg,
            # where we can't omit the first point.
            if total_idx == 0:
                db.raw_execute(
                    "INSERT INTO shape_points (shape_id, sequence, lat, lon, shape_dist_traveled) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (shape_id, total_idx, leg[0][0], leg[0][1], total_dist),
                )
                total_idx += 1

            for prev_pt, pt in pairwise(leg):
                total_dist += pyroutelib3.haversine_earth_distance(prev_pt, pt)
                db.raw_execute(
                    "INSERT INTO shape_points (shape_id, sequence, lat, lon, shape_dist_traveled) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (shape_id, total_idx, pt[0], pt[1], total_dist),
                )
                total_idx += 1

            distances.append(total_dist)

        assert len(distances) == len(stops)
        return Shape(shape_id, tuple(distances))

    def get_leg_shape(self, a_id: str, b_id: str) -> list[tuple[float, float]]:
        a_node = self.stop_to_node(a_id)
        b_node = self.stop_to_node(b_id)

        try:
            nodes = pyroutelib3.find_route(self.graph, a_node, b_node)
        except pyroutelib3.StepLimitExceeded:
            nodes = []

        if not nodes:
            self.logger.error("No shape from %s to %s", a_id, b_id)
            nodes = [a_node, b_node]

        return [self.graph.nodes[i].position for i in nodes]

    def stop_to_node(self, stop_id: str) -> int:
        if cached := self.cached_nodes.get(stop_id):
            return cached

        id = self.graph.find_nearest_node(self.stop_positions[stop_id]).id
        self.cached_nodes[stop_id] = id
        return id


class FilteredRailwayProfile(pyroutelib3.osm.RailwayProfile):
    def __init__(self, route_id: str) -> None:
        super().__init__(penalties={"rail": 1})
        self.route_id = route_id

    def way_penalty(self, tags: Mapping[str, str]) -> float | None:
        if tags.get(self.route_id) == "yes":
            return self.penalties.get(tags.get("railway", ""))
        return None


def create_graph(path: Path, route_id: str) -> pyroutelib3.osm.Graph:
    with path.open("rb") as f:
        return pyroutelib3.osm.Graph.from_file(FilteredRailwayProfile(route_id), f)


def load_stop_positions(db: DBConnection) -> dict[str, tuple[float, float]]:
    return {
        cast(str, i[0]): (cast(float, i[1]), cast(float, i[2]))
        for i in db.raw_execute("SELECT stop_id, lat, lon FROM stops")
    }


def load_routes_with_geo(r: ManagedResource) -> list[str]:
    return [i["id"] for i in r.csv() if i["has_geo"] == "1"]


def load_trips(db: DBConnection, route_id: str) -> list[Trip]:
    trip_ids = [
        cast(str, i[0])
        for i in db.raw_execute("SELECT trip_id FROM trips WHERE route_id = ?", (route_id,))
    ]

    trips = list[Trip]()
    for trip_id in trip_ids:
        stops = tuple(
            Stop(cast(int, i[0]), cast(str, i[1]))
            for i in db.raw_execute(
                "SELECT stop_sequence, stop_id FROM stop_times WHERE trip_id = ? "
                "ORDER BY stop_sequence ASC",
                (trip_id,),
            )
        )
        trips.append(Trip(trip_id, stops))
    return trips


def set_shape_on_trip(
    db: DBConnection,
    trip_id: str,
    shape_id: str,
    stops: Stops,
    distances: Distances,
) -> None:
    db.raw_execute("UPDATE trips SET shape_id = ? WHERE trip_id = ?", (shape_id, trip_id))
    db.raw_execute_many(
        "UPDATE stop_times SET shape_dist_traveled = ? WHERE trip_id = ? AND stop_sequence = ?",
        ((distance, trip_id, stop.idx) for stop, distance in zip(stops, distances, strict=True)),
    )
