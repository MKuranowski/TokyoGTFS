# Copyright (c) 2021 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

class MissingApiKeys(ValueError):
    pass


class NoProvider(RuntimeError):
    pass


class InvalidData(ValueError):
    pass


class MissingLocalData(InvalidData):
    pass


class BlockError(InvalidData):
    pass


class InvalidGeoData(InvalidData):
    def __init__(self) -> None:
        super().__init__("Invalid geo.osm data. See logging messages for details.")
