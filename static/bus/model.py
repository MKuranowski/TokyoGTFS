# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from abc import ABC, abstractmethod, abstractproperty
from datetime import date, timedelta
from dataclasses import dataclass
from typing import Any, Iterable, List, Literal, Mapping, NamedTuple, Optional, Set, Tuple

from ..const import FUTURE_DAYS

AgencyID = str
RouteID = str
PatternID = str
StopID = str
CalendarID = str
TripID = str


class ConvertOptions(NamedTuple):
    start_date: date
    end_date: Optional[date] = None
    publisher_name: str = ""
    publisher_url: str = ""
    target: str = "tokyo_trains.zip"

    @classmethod
    def from_namespace(cls, ns: Any) -> "ConvertOptions":
        start_date = date.today()
        end_date = start_date + timedelta(days=FUTURE_DAYS)
        return cls(start_date, end_date, ns.publisher_name, ns.publisher_url, ns.target)


class Name(NamedTuple):
    ja: str
    en: str


@dataclass
class Stop:
    id: StopID
    name: Name
    lat: float
    lon: float
    code: Optional[str] = None


@dataclass
class StopTime:
    stop: StopID
    arrival: int
    departure: int
    no_boarding: bool = False
    no_disembarking: bool = False


@dataclass
class Trip:
    id: TripID
    route: RouteID
    pattern: PatternID
    calendar: CalendarID
    times: List[StopTime]
    headsign: Optional[str] = None
    non_step: Optional[bool] = None
    direction: Optional[Literal["0", "1"]] = None

    @property
    def gtfs_wheelchair_accessible(self) -> Literal["0", "1", "2"]:
        if self.non_step is True:
            return "1"
        elif self.non_step is False:
            return "2"
        else:
            return "0"


@dataclass
class Route:
    id: RouteID
    agency: AgencyID
    code: str
    name: Optional[str] = None


@dataclass
class Calendar:
    id: CalendarID
    days: Optional[List[date]] = None


class Provider(ABC):
    @abstractproperty
    def name(self) -> str: ...

    @abstractproperty
    def provides(self) -> Set[AgencyID]: ...

    @abstractproperty
    def attribution(self) -> str: ...

    @property
    def needs_apikey(self) -> bool:
        return False

    def set_apikey(self, key: str) -> None:
        pass

    def close(self) -> None:
        pass

    @abstractmethod
    def routes(self) -> Iterable[Route]: ...

    @abstractmethod
    def stops(self) -> Iterable[Stop]: ...

    @abstractmethod
    def calendars(self) -> Iterable[Calendar]: ...

    @abstractmethod
    def trips(self) -> Iterable[Trip]: ...

    @abstractmethod
    def count_stops(self) -> Tuple[Mapping[AgencyID, int], Mapping[AgencyID, int]]: ...
