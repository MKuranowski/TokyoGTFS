# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterable

from impuls import Task, TaskRuntime


class InsertDummyAgencies(Task):
    def __init__(self, operators: Iterable[str]) -> None:
        super().__init__()
        self.operators = operators

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            r.db.raw_execute_many(
                "INSERT INTO agencies (agency_id, name, url, timezone, lang) "
                "VALUES (?, ?, 'https://example.com/', 'Asia/Tokyo', 'ja')",
                ((id, id) for id in self.operators),
            )
