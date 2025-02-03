from argparse import Namespace

from impuls import App, HTTPResource, Pipeline, PipelineOptions

from .generate_blocks import GenerateBlocks
from .load_schedules import LoadSchedules


class TokyoGTFS(App):
    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        return Pipeline(
            options=options,
            tasks=[
                LoadSchedules(),
                GenerateBlocks(),
                # TODO
                # GenerateHeadsigns(),
                # CurateAgencies(),
                # CurateRoutes(),
                # AddAttributions(),
                # AddFeedInfo(),
                # RemoveUnusedEntities(),
                # SaveGTFS()
            ],
            resources={
                "mini-tokyo-3d.zip": HTTPResource.get(
                    "https://github.com/nagix/mini-tokyo-3d/archive/refs/heads/master.zip"
                ),
            },
        )
