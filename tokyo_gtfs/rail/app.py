# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from argparse import Namespace

from impuls import App, HTTPResource, LocalResource, Pipeline, PipelineOptions
from impuls.model import Attribution, FeedInfo
from impuls.tasks import AddEntity, SaveGTFS

from .curate import CurateAgencies, CurateRoutes
from .fix_yamanote_headsigns import FixYamanoteLineHeadsigns
from .generate_blocks import GenerateBlocks
from .generate_headsigns import GenerateHeadsigns
from .gtfs import GTFS_HEADERS
from .load_schedules import LoadSchedules


class TokyoRailGTFS(App):
    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        return Pipeline(
            options=options,
            tasks=[
                LoadSchedules(),
                GenerateBlocks(),
                GenerateHeadsigns(),
                FixYamanoteLineHeadsigns(),
                # TODO: MergeDuplicateRoutes()
                # TODO: SeparateAirportLinks(),
                # TODO: SimplifyBlocks(),  # merge consecutive trips belonging to the same route
                # TODO: GenerateCalendarExceptions(),
                # TODO: GenerateShapes(),
                CurateAgencies(),
                CurateRoutes(),
                AddEntity(
                    task_name="AddAttribution1",
                    entity=Attribution(
                        id="1",
                        organization_name=(
                            "Schedules: Copyright (c) 2019-2025 Akihiko Kusanagi "
                            "(under MIT license)"
                        ),
                        is_producer=False,
                        is_operator=False,
                        is_authority=True,
                        is_data_source=True,
                        url="https://github.com/nagix/mini-tokyo-3d/",
                    ),
                ),
                AddEntity(
                    task_name="AddAttribution2",
                    entity=Attribution(
                        id="2",
                        organization_name="GTFS: Mikołaj Kuranowski",
                        is_producer=True,
                        is_operator=False,
                        is_authority=True,
                        is_data_source=True,
                        url="https://github.com/MKuranowski/TokyoGTFS",
                    ),
                ),
                AddEntity(
                    task_name="AddFeedInfo",
                    entity=FeedInfo(
                        publisher_name="Mikołaj Kuranowski",
                        publisher_url="https://mkuran.pl/gtfs/",
                        lang="mul",
                    ),
                ),
                # TODO: RemoveUnusedEntities(),
                SaveGTFS(GTFS_HEADERS, "tokyo_rail.zip", ensure_order=True),
            ],
            resources={
                "mini-tokyo-3d.zip": HTTPResource.get(
                    "https://github.com/nagix/mini-tokyo-3d/archive/refs/heads/master.zip"
                ),
                "agencies.csv": LocalResource("data/agencies.csv"),
                "routes.csv": LocalResource("data/routes.csv"),
            },
        )
