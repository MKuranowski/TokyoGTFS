from argparse import Namespace

from impuls import App, HTTPResource, LocalResource, Pipeline, PipelineOptions
from impuls.tasks import SaveGTFS

from .curate import CurateAgencies, CurateRoutes
from .generate_blocks import GenerateBlocks
from .generate_headsigns import GenerateHeadsigns
from .gtfs import GTFS_HEADERS
from .load_schedules import LoadSchedules


class TokyoGTFS(App):
    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        return Pipeline(
            options=options,
            tasks=[
                LoadSchedules(),
                GenerateBlocks(),
                GenerateHeadsigns(),
                CurateAgencies(),
                CurateRoutes(),
                # TODO
                # AddAttributions(),
                # AddFeedInfo(),
                # RemoveUnusedEntities(),
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
