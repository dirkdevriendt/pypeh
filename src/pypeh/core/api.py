"""
Functional API for pypeh core.
TODO: expose functions
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pypeh.core.commands.ingestion import ManifestIngestionCommand, DataIngestionCommand

if TYPE_CHECKING:
    from typing import Union
    from pypeh.core.cache.dataview import DataView


def load_peh_fdo(
    root: Union[str, Path],  # directory or URL
    *,
    load_data: bool = True,
    lazy_loading: bool = True,
) -> DataView:
    """
    TODO: decide on argument structure
    """
    # Task 1: Ingest manifest file
    command = ManifestIngestionCommand.create(load_peh_fdo, root=root)
    task = command.get_task()
    context = task.execute()

    # Task 2: Ingest data described in manifest
    if load_data:
        task = context.add_task(
            DataIngestionCommand,
            load_data=load_data,
            lazy_loading=lazy_loading,
        )
        _ = task.execute()
    _ = task.complete()

    return task._context.data
