from __future__ import annotations

import logging
import polars as pl
import io

from pathlib import Path
from typing import TYPE_CHECKING, Union, IO
from polars.datatypes import DataType, DataTypeClass

from pypeh.core.models.constants import ObservablePropertyValueType
from pypeh.adapters.persistence.serializations import IOAdapter

if TYPE_CHECKING:
    from typing import Mapping


logger = logging.getLogger(__name__)


DATAFRAME_TYPE_MAPPING: dict[
    ObservablePropertyValueType, DataType | DataTypeClass
] = {
    ObservablePropertyValueType.DATE: pl.Date,
    ObservablePropertyValueType.DATETIME: pl.Datetime,
    ObservablePropertyValueType.BOOLEAN: pl.Boolean,
    ObservablePropertyValueType.FLOAT: pl.Float64,
    ObservablePropertyValueType.INTEGER: pl.Int64,
    ObservablePropertyValueType.STRING: pl.Utf8,
    ObservablePropertyValueType.CATEGORICAL: pl.Utf8,
    ObservablePropertyValueType.DECIMAL: pl.Float64,
}


class CsvIOImpl(IOAdapter):
    def load(
        self, source: Union[str, Path, IO[str], IO[bytes]], **kwargs
    ) -> pl.DataFrame:
        try:
            if hasattr(source, "read") and not isinstance(source, (str, Path)):
                encoding = kwargs.get("encoding", None)
                if encoding is None:
                    encoding = "utf-8"
                data = source.read()
                if isinstance(data, memoryview):
                    data = data.tobytes().decode(encoding)
                elif isinstance(data, (bytes, bytearray)):
                    data = data.decode(encoding)
                buffer = io.StringIO(data)
                return pl.read_csv(buffer, **kwargs)
            else:
                return pl.read_csv(source=str(source), **kwargs)

        except Exception as e:
            logger.error(f"Error in CSVIOImpl: {e}")
            raise

    def dump(self, destination: str, **kwargs):
        raise NotImplementedError(
            "CsvIOImpl.dump is not implemented yet. "
            f"destination={destination!r}, kwargs={kwargs!r}."
        )


class ExcelIOImpl(IOAdapter):
    def _load(
        self, source: Union[str, Path, IO[str], IO[bytes], bytes], **options
    ) -> pl.DataFrame | dict[str, pl.DataFrame]:
        if isinstance(source, bytes):
            # Handle raw bytes data
            result = pl.read_excel(
                source=io.BytesIO(source),
                **options,
            )
        elif hasattr(source, "read") and not isinstance(source, (str, Path)):
            # Handle file-like objects
            if isinstance(source, IO) and "b" not in getattr(
                source, "mode", "b"
            ):
                raise ValueError("Excel source must be opened in binary mode")
            data = source.read()  # type: ignore
            result = pl.read_excel(  # type: ignore
                source=io.BytesIO(data),  # type: ignore
                **options,
            )
        else:
            # Handle file paths
            result = pl.read_excel(  # type: ignore
                source=str(source),  # type: ignore
                **options,
            )
        return result

    def _read_source_data(
        self, source: Union[str, Path, IO[str], IO[bytes]]
    ) -> bytes | None:
        """Read data from source once and cache it"""
        if hasattr(source, "read") and not isinstance(source, (str, Path)):
            if isinstance(source, IO) and "b" not in getattr(
                source, "mode", "b"
            ):
                raise ValueError("Excel source must be opened in binary mode")
            ret = source.read()
            assert isinstance(ret, bytes)
            return ret
        return None

    def load_section(
        self,
        source: Union[str, Path, IO[str], IO[bytes], bytes],
        section_name: str,
        data_schema: dict[str, str] | None = None,
        cached_data: bytes | None = None,
    ) -> pl.DataFrame:
        typed_schema: Mapping[str, DataType | DataTypeClass] | None = None
        if data_schema is not None:
            typed_schema = {}
            for key, value in data_schema.items():
                polars_type = DATAFRAME_TYPE_MAPPING.get(value, None)
                if polars_type is None:
                    logger.debug(
                        f"Cound not find {value} in DATAFRAME_TYPE_MAPPING"
                    )
                    raise KeyError(
                        f"Could not find {value} in DATAFRAME_TYPE_MAPPING"
                    )
                typed_schema[key] = polars_type

        default = {
            "engine": "calamine",
            "has_header": True,
        }
        options = {
            **default,
            "sheet_name": section_name,
            "schema_overrides": typed_schema,
        }

        ret = self._load(source, **options)
        assert isinstance(ret, pl.DataFrame)
        return ret

    def load(
        self,
        source: Union[str, Path, IO[str], IO[bytes]],
        data_schema: dict[str, dict[str, str]] | None = None,
        **kwargs,
    ) -> dict[str, pl.DataFrame]:
        try:
            # if data_schema is provided we need to load each sheet individually
            if data_schema is not None:
                cached_data = self._read_source_data(source)
                assert cached_data is not None
                result = {}
                for section_name, typing_dict in data_schema.items():
                    result[section_name] = self.load_section(
                        cached_data, section_name, typing_dict
                    )

            else:
                default = {
                    "sheet_id": 0,
                    "engine": "calamine",
                    "has_header": True,
                }
                options = {**default, **kwargs}
                result = self._load(source, **options)
            assert isinstance(result, dict)

            return result

        except Exception as e:
            logger.error(f"Error in ExcelIOImpl: {e}")
            raise

    def dump(self, destination: str, **kwargs):
        raise NotImplementedError(
            "ExcelIOImpl.dump is not implemented yet. "
            f"destination={destination!r}, kwargs={kwargs!r}."
        )
