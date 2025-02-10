# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from argparse import Namespace

from impuls import App, HTTPResource, LocalResource, Pipeline, PipelineOptions
from impuls.model import Attribution, FeedInfo
from impuls.tasks import AddEntity, ExecuteSQL, RemoveUnusedEntities, SaveGTFS

from .curate import CurateAgencies, CurateRoutes
from .fix_yamanote_headsigns import FixYamanoteLineHeadsigns
from .generate_blocks import GenerateBlocks
from .generate_headsigns import GenerateHeadsigns
from .gtfs import GTFS_HEADERS
from .load_schedules import LoadSchedules
from .merge_routes import MergeRoutes
from .separate_limited_expresses import SeparateLimitedExpresses
from .simplify_blocks import SimplifyBlocks


class TokyoRailGTFS(App):
    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        return Pipeline(
            options=options,
            tasks=[
                ExecuteSQL(
                    task_name="CreateTranslationsIndex",
                    statement=(
                        "CREATE INDEX idx_translations_table_record "
                        "ON translations(table_name, record_id, record_sub_id)"
                    ),
                ),
                LoadSchedules(),
                GenerateBlocks(),
                GenerateHeadsigns(),
                FixYamanoteLineHeadsigns(),
                SeparateLimitedExpresses(),
                MergeRoutes(),
                SimplifyBlocks(),
                # TODO: GenerateShapes(),
                RemoveUnusedEntities(),
                # TODO: RemoveInvalidTranslations
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
                SaveGTFS(GTFS_HEADERS, "tokyo_rail.zip", ensure_order=True),
            ],
            resources={
                "mini-tokyo-3d.zip": HTTPResource.get(
                    "https://github.com/nagix/mini-tokyo-3d/archive/refs/heads/master.zip"
                ),
                "agencies.csv": LocalResource("data/agencies.csv"),
                "routes.csv": LocalResource("data/routes.csv"),
                "limited_expresses.yml": LocalResource("data/limited_expresses.yml"),
                "route_merges.yml": LocalResource("data/route_merges.yml"),
            },
        )
