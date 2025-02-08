# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from unittest import TestCase

from .generate_blocks import BlockNode, GenerateBlocks


class TestFindAllBlocks(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.maxDiff = 4096

    def test_flat_topology(self) -> None:
        blocks = find_all_blocks(
            BlockNode(
                trip_id="JR-East.SaikyoKawagoe.225M.Weekday",
                destinations=["Sotetsu.Main.Ebina"],
                previous=[],
                next=["JR-East.SotetsuDirect.225M.Weekday"],
            ),
            BlockNode(
                trip_id="JR-East.SotetsuDirect.225M.Weekday",
                destinations=["Sotetsu.Main.Ebina"],
                previous=["JR-East.SaikyoKawagoe.225M.Weekday"],
                next=["Sotetsu.SotetsuShinYokohama.6225.Weekday"],
            ),
            BlockNode(
                trip_id="Sotetsu.SotetsuShinYokohama.6225.Weekday",
                destinations=["Sotetsu.Main.Ebina"],
                previous=["JR-East.SotetsuDirect.225M.Weekday"],
                next=["Sotetsu.Main.6225.Weekday"],
            ),
            BlockNode(
                trip_id="Sotetsu.Main.6225.Weekday",
                destinations=["Sotetsu.Main.Ebina"],
                previous=["Sotetsu.SotetsuShinYokohama.6225.Weekday"],
                next=[],
            ),
        )
        self.assertEqual(len(blocks), 1)
        self.assertListEqual(
            blocks[0],
            [
                BlockNode(
                    trip_id="JR-East.SaikyoKawagoe.225M.Weekday",
                    destinations=["Sotetsu.Main.Ebina"],
                    previous=[],
                    next=["JR-East.SotetsuDirect.225M.Weekday"],
                ),
                BlockNode(
                    trip_id="JR-East.SotetsuDirect.225M.Weekday",
                    destinations=["Sotetsu.Main.Ebina"],
                    previous=["JR-East.SaikyoKawagoe.225M.Weekday"],
                    next=["Sotetsu.SotetsuShinYokohama.6225.Weekday"],
                ),
                BlockNode(
                    trip_id="Sotetsu.SotetsuShinYokohama.6225.Weekday",
                    destinations=["Sotetsu.Main.Ebina"],
                    previous=["JR-East.SotetsuDirect.225M.Weekday"],
                    next=["Sotetsu.Main.6225.Weekday"],
                ),
                BlockNode(
                    trip_id="Sotetsu.Main.6225.Weekday",
                    destinations=["Sotetsu.Main.Ebina"],
                    previous=["Sotetsu.SotetsuShinYokohama.6225.Weekday"],
                    next=[],
                ),
            ],
        )

    def test_split_topology(self) -> None:
        blocks = find_all_blocks(
            BlockNode(
                trip_id="TokyoMetro.Chiyoda.A1003E.SaturdayHoliday",
                destinations=[
                    "HakoneTozan.HakoneTozan.HakoneYumoto",
                    "Odakyu.Enoshima.KataseEnoshima",
                ],
                previous=[],
                next=["Odakyu.Odawara.493.SaturdayHoliday.1"],
            ),
            BlockNode(
                trip_id="Odakyu.Odawara.6505.Weekday",
                destinations=[
                    "HakoneTozan.HakoneTozan.HakoneYumoto",
                    "Odakyu.Enoshima.KataseEnoshima",
                ],
                previous=["TokyoMetro.Chiyoda.A1003E.SaturdayHoliday"],
                next=[
                    "Odakyu.Odawara.493.SaturdayHoliday.2",
                    "Odakyu.Enoshima.593.SaturdayHoliday",
                ],
            ),
            BlockNode(
                trip_id="Odakyu.Odawara.493.SaturdayHoliday.2",
                destinations=["HakoneTozan.HakoneTozan.HakoneYumoto"],
                previous=["Odakyu.Odawara.493.SaturdayHoliday.2"],
                next=[],
            ),
            BlockNode(
                trip_id="Odakyu.Enoshima.593.SaturdayHoliday",
                destinations=["Odakyu.Enoshima.KataseEnoshima"],
                previous=["Odakyu.Odawara.493.SaturdayHoliday.1"],
                next=[],
            ),
        )
        self.assertEqual(len(blocks), 2)
        self.assertListEqual(
            blocks[0],
            [
                BlockNode(
                    trip_id="TokyoMetro.Chiyoda.A1003E.SaturdayHoliday",
                    destinations=["HakoneTozan.HakoneTozan.HakoneYumoto"],
                    previous=[],
                    next=["Odakyu.Odawara.493.SaturdayHoliday.1"],
                ),
                BlockNode(
                    trip_id="Odakyu.Odawara.6505.Weekday",
                    destinations=["HakoneTozan.HakoneTozan.HakoneYumoto"],
                    previous=["TokyoMetro.Chiyoda.A1003E.SaturdayHoliday"],
                    next=["Odakyu.Odawara.493.SaturdayHoliday.2"],
                ),
                BlockNode(
                    trip_id="Odakyu.Odawara.493.SaturdayHoliday.2",
                    destinations=["HakoneTozan.HakoneTozan.HakoneYumoto"],
                    previous=["Odakyu.Odawara.493.SaturdayHoliday.2"],
                    next=[],
                ),
            ],
        )
        self.assertListEqual(
            blocks[1],
            [
                BlockNode(
                    trip_id="TokyoMetro.Chiyoda.A1003E.SaturdayHoliday",
                    destinations=["Odakyu.Enoshima.KataseEnoshima"],
                    previous=[],
                    next=["Odakyu.Odawara.493.SaturdayHoliday.1"],
                    clone_with_suffix=".2",
                ),
                BlockNode(
                    trip_id="Odakyu.Odawara.6505.Weekday",
                    destinations=["Odakyu.Enoshima.KataseEnoshima"],
                    previous=["TokyoMetro.Chiyoda.A1003E.SaturdayHoliday"],
                    next=["Odakyu.Enoshima.593.SaturdayHoliday"],
                    clone_with_suffix=".2",
                ),
                BlockNode(
                    trip_id="Odakyu.Enoshima.593.SaturdayHoliday",
                    destinations=["Odakyu.Enoshima.KataseEnoshima"],
                    previous=["Odakyu.Odawara.493.SaturdayHoliday.1"],
                    next=[],
                ),
            ],
        )

    def test_join_topology(self) -> None:
        blocks = find_all_blocks(
            BlockNode(
                trip_id="JR-East.ShonanShinjuku.2201M.Weekday",
                destinations=["JR-East.SobuRapid.NaritaAirportTerminal1"],
                previous=[],
                next=["JR-East.YamanoteFreight.2201M.Weekday"],
            ),
            BlockNode(
                trip_id="JR-East.YamanoteFreight.2201M.Weekday",
                destinations=["JR-East.SobuRapid.NaritaAirportTerminal1"],
                previous=["JR-East.ShonanShinjuku.2201M.Weekday"],
                next=["JR-East.SobuRapid.2001M.Weekday"],
            ),
            BlockNode(
                trip_id="JR-East.Yokosuka.2001M.Weekday",
                destinations=["JR-East.SobuRapid.NaritaAirportTerminal1"],
                previous=[],
                next=["JR-East.SobuRapid.2001M.Weekday"],
            ),
            BlockNode(
                trip_id="JR-East.SobuRapid.2001M.Weekday",
                destinations=["JR-East.SobuRapid.NaritaAirportTerminal1"],
                previous=[
                    "JR-East.Yokosuka.2001M.Weekday",
                    "JR-East.YamanoteFreight.2201M.Weekday",
                ],
                next=[],
            ),
        )
        self.assertEqual(len(blocks), 2)
        self.assertListEqual(
            blocks[0],
            [
                BlockNode(
                    trip_id="JR-East.Yokosuka.2001M.Weekday",
                    destinations=["JR-East.SobuRapid.NaritaAirportTerminal1"],
                    previous=[],
                    next=["JR-East.SobuRapid.2001M.Weekday"],
                ),
                BlockNode(
                    trip_id="JR-East.SobuRapid.2001M.Weekday",
                    destinations=["JR-East.SobuRapid.NaritaAirportTerminal1"],
                    previous=["JR-East.Yokosuka.2001M.Weekday"],
                    next=[],
                ),
            ],
        )
        self.assertListEqual(
            blocks[1],
            [
                BlockNode(
                    trip_id="JR-East.ShonanShinjuku.2201M.Weekday",
                    destinations=["JR-East.SobuRapid.NaritaAirportTerminal1"],
                    previous=[],
                    next=["JR-East.YamanoteFreight.2201M.Weekday"],
                ),
                BlockNode(
                    trip_id="JR-East.YamanoteFreight.2201M.Weekday",
                    destinations=["JR-East.SobuRapid.NaritaAirportTerminal1"],
                    previous=["JR-East.ShonanShinjuku.2201M.Weekday"],
                    next=["JR-East.SobuRapid.2001M.Weekday"],
                ),
                BlockNode(
                    trip_id="JR-East.SobuRapid.2001M.Weekday",
                    destinations=["JR-East.SobuRapid.NaritaAirportTerminal1"],
                    previous=["JR-East.YamanoteFreight.2201M.Weekday"],
                    next=[],
                    clone_with_suffix=".2",
                ),
            ],
        )

    def test_multiple_forks(self) -> None:
        #  1  2  3  4
        #  o--o--o--o A
        #  o-/    \-o B
        #  5        6
        blocks = find_all_blocks(
            BlockNode("1", ["A"], [], ["2"]),
            BlockNode("2", ["A", "B"], ["1", "5"], ["3"]),
            BlockNode("3", ["A", "B"], ["2"], ["4", "6"]),
            BlockNode("4", ["A"], ["3"], []),
            BlockNode("5", ["B"], [], ["2"]),
            BlockNode("6", ["B"], ["3"], []),
        )
        self.assertListEqual(blocks, [])

    def test_multiple_forks_with_one_trip(self) -> None:
        #  1  2  3
        #  o--o--o A
        #  o-/ \-o B
        #  4     5
        blocks = find_all_blocks(
            BlockNode("1", ["A"], [], ["2"]),
            BlockNode("2", ["A", "B"], ["1", "5"], ["3", "5"]),
            BlockNode("3", ["A"], ["2"], []),
            BlockNode("4", ["B"], [], ["2"]),
            BlockNode("5", ["B"], ["2"], []),
        )
        self.assertListEqual(blocks, [])


def find_all_blocks(*nodes: BlockNode) -> list[list[BlockNode]]:
    return GenerateBlocks().find_all_blocks({n.trip_id: n for n in nodes})
