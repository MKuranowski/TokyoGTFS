# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from abc import ABC, abstractmethod, abstractproperty
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Mapping, NamedTuple, Optional, Protocol, Set
from datetime import date, datetime, timedelta

from ..const import FUTURE_DAYS

AgencyID = str
RouteID = str
TrainID = str
TrainTypeID = str
StationID = str
CalendarID = str
Seconds = int


class ConvertOptions(NamedTuple):
    start_date: date
    end_date: Optional[date] = None
    from_cache: bool = False
    publisher_name: str = ""
    publisher_url: str = ""
    target: str = "tokyo_trains.zip"

    @classmethod
    def from_namespace(cls, ns: Any) -> "ConvertOptions":
        start_date = date.today()
        end_date = start_date + timedelta(days=FUTURE_DAYS)
        return cls(start_date, end_date, ns.from_cache, ns.publisher_name, ns.publisher_url,
                   ns.target)


class Name(NamedTuple):
    ja: str
    en: str

    def as_json(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, o: Dict[str, Any]) -> "Name":
        return cls(**o)


@dataclass
class TrainTimetableEntry:
    station: StationID
    arrival: Seconds
    departure: Seconds
    platform: str

    def as_json(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, o: Dict[str, Any]) -> "TrainTimetableEntry":
        return cls(**o)


@dataclass
class Train:
    id: TrainID
    agency: AgencyID
    route: RouteID
    calendar: CalendarID
    timetable: List[TrainTimetableEntry]
    destinations: List[StationID]
    train_number: Optional[str] = None
    origins: Optional[List[StationID]] = None
    next_timetable: Optional[List[TrainID]] = None
    previous_timetable: Optional[List[TrainID]] = None
    train_type: TrainTypeID = "Local"
    train_name: Optional[Name] = None
    direction: Optional[str] = None
    realtime_id: Optional[str] = None

    def as_json(self) -> Dict[str, Any]:
        # dataclasses.asdict() recurses into lists and dataclasses,
        return asdict(self)

    @classmethod
    def from_json(cls, o: Dict[str, Any]) -> "Train":
        # contrary to json serialization, we need to handle nested objects on our own
        o["timetable"] = [TrainTimetableEntry.from_json(i) for i in o["timetable"]]
        o["train_name"] = Name.from_json(o["train_name"]) if o["train_name"] else None
        return cls(**o)


@dataclass
class Station:
    id: StationID
    agency: AgencyID
    route: RouteID
    code: Optional[str] = None

    def as_json(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, o: Dict[str, Any]) -> "Station":
        return cls(**o)


@dataclass
class Calendar:
    id: CalendarID
    days: Optional[List[date]] = None

    def as_json(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "days": None if self.days is None else [i.strftime("%Y-%m-%d") for i in self.days],
        }

    @classmethod
    def from_json(cls, o: Dict[str, Any]) -> "Calendar":
        return cls(
            id=o["id"],
            days=None if o["days"] is None else [datetime.strptime(i, "%Y-%m-%d").date()
                                                 for i in o["days"]],
        )


@dataclass
class TrainType:
    id: TrainTypeID
    name: Name

    def as_json(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, o: Dict[str, Any]) -> "TrainType":
        return cls(
            id=o["id"],
            name=Name.from_json(o["name"]),
        )


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
    def trains(self) -> Iterable[Train]: ...

    @abstractmethod
    def stations(self) -> Iterable[Station]: ...

    @abstractmethod
    def calendars(self) -> Iterable[Calendar]: ...

    @abstractmethod
    def train_types(self) -> Iterable[TrainType]: ...


class Exporter(Protocol):
    def __enter__(self) -> "Exporter": ...
    def __exit__(self, __exc_type, __exc_value, __traceback) -> Any: ...
    def save(self, __thing: Mapping[str, Any]) -> None: ...
    def save_many(self, __things: Iterable[Mapping[str, Any]]) -> None: ...
    def close(self) -> None: ...
