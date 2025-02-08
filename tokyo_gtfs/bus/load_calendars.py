from impuls import Task, TaskRuntime


class LoadCalendars(Task):
    def __init__(self, *resources: str) -> None:
        super().__init__()
        self.resources = resources

    def execute(self, r: TaskRuntime) -> None:
        raise NotImplementedError
