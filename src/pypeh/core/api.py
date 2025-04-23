"""
Functional API for pypeh core.
TODO: expose functions
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pypeh.core.commands.ingestion import ManifestIngestionCommand, DataIngestionCommand
from pypeh.core.cache.dataview import get_dataview
from pypeh.core.persistence.formats import YamlIO
from pypeh.core.models.peh import EntityList

if TYPE_CHECKING:
    from typing import Union, Optional, Mapping, List
    from pypeh.core.cache.dataview import DataView
    from os import PathLike


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


def read_yaml(
    file: Union[str, PathLike[str]],
    *,
    importmap: Optional[
        Mapping[str, Union[List[str], str]]
    ] = None,  # optional mapping between schema names and local paths
    lazy_loading: bool = True,
    data_view: Optional[DataView] = None,
):
    if data_view is None:
        data_view = get_dataview(importmap=importmap)  # type:ignore
    root = YamlIO().load(file, EntityList)  # type: ignore
    data_view.add(root)  # type: ignore

    return data_view


def read_fdo(
    fdo_path: Union[str, PathLike[str]],
    *,
    importmap: Optional[Mapping[str, str]] = None,  # optional mapping between schema names and local paths
    lazy_loading: bool = True,
):
    pass
