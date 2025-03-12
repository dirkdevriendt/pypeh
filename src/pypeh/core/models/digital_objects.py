from __future__ import annotations

import json

from pydantic import BaseModel, Field, ConfigDict
from pydantic.json_schema import GenerateJsonSchema
from typing import List, Optional, TYPE_CHECKING, Union, Mapping

from pypeh.core.models import constants, uri_regex

if TYPE_CHECKING:
    from pathlib import Path


# Classes for array items
class DataBitSeq(BaseModel):
    value: str


class DataRef(DataBitSeq):
    """PID Reference to data bit sequence"""

    value: str = Field(pattern=uri_regex.PID_PATTERN, json_schema_extra={"@id": "21.T11969/864894d7bd7eac7fb6e1"})


class DataLoc(DataBitSeq):
    """Location of data bit sequence URL or path"""

    value: str = Field(json_schema_extra={"@id": "21.T11969/085f37963798fc303aaa"})


class MetaDataRef(DataBitSeq):
    """PID Reference to metadata bit sequence"""

    value: str = Field(pattern=uri_regex.PID_PATTERN, json_schema_extra={"@id": "21.T11969/9a970a96052df3301648"})


class MetaDataLoc(DataBitSeq):
    """Location of metadata bit sequence URL or path"""

    value: str = Field(json_schema_extra={"@id": "21.T11969/fe4704a151e06bbed1ae"})


# General FDO
class FDO(BaseModel):
    model_config = ConfigDict(
        title="General FDO_Config",
        description="Profile of a general FDO.",
        extra="allow",
        json_schema_extra={"$schema": GenerateJsonSchema.schema_dialect},
    )  # type: ignore

    FDO_Profile_Ref: str = Field(
        pattern=uri_regex.PID_PATTERN,
        json_schema_extra={"@id": "21.T11969/bcc54a2a9ab5bf2a8f2c"},
        description="Identifier of the FDO profile descibing the attributes and characteristics of the resources associated with the FDO PID.",
    )

    FDO_Type_Ref: str = Field(
        pattern=uri_regex.PID_PATTERN,
        json_schema_extra={"@id": "21.T11969/2bb5fec05c00bb89793e"},
        description="Should enable client to quickly determine the type of data associated with the FDO.",
    )


class PehFDOHeader(BaseModel):
    peh_fdo_version: str = Field(
        description="The semantic version of the  pehFDO schema.",
        pattern=uri_regex.SEMANTIC_VERSION_PATTERN,
    )
    entry_point: str = Field(default="pypeh.loads")
    peh_fdo_data_set_identifier: str = Field(description="Identifier pointing to the data set location.")


class PehData(DataBitSeq):
    content: constants.FolderEnum = Field(
        description="Describes  on which this data reports.",
    )
    peh_model_version: str = Field(
        description="The semantic version of the pehData Model.",
        pattern=uri_regex.SEMANTIC_VERSION_PATTERN,
    )
    file_type: str = Field(
        json_schema_extra={},
        default="application/yaml",
        # TODO: add further info @id, this is always a yaml file.
    )


class PehFDO(FDO):
    model_config = ConfigDict(
        title="peh FAIR Digital Object",
        description="The pehFDO is a human- and machine-readible file format collecting"
        "metadata on defining the content of a dataset on personal exposure and health data.",
        extra="allow",
        json_schema_extra={
            "@id": "https://www.w3id.org/schemas/pehfdo-v1.0.0.json",  # THIS IS THE FDO_PROFILE ID
            "$schema": GenerateJsonSchema.schema_dialect,
            "version": "0.0.1",
        },
    )  # type: ignore

    # Field that will be included in all instances
    schema_ref: str = Field(
        default="https://www.w3id.org/schemas/pehfdo-v1.0.0.json",  # should match schema@id !!!
        alias="$schema",
    )

    peh_fdo_header: PehFDOHeader = Field(
        json_schema_extra={
            "@id": "TBD",
        }
    )
    peh_fdo_data_set: Optional[List[PehData]] = Field(
        default=None,
        json_schema_extra={
            "@id": "TBD",
        },
    )

    @classmethod
    def _dump(cls, schema_dict: Mapping, file, **kwargs) -> None:
        with open(file, "w") as f:
            return json.dump(schema_dict, f, **kwargs)

    @classmethod
    def _dumps(cls, schema_dict: Mapping, **kwargs) -> str:
        return json.dumps(schema_dict, **kwargs)

    @classmethod
    def dump_json_schema(cls, file: Union[str, Path, None] = None, **kwargs: Mapping) -> Union[None, str]:
        schema_dict = cls.model_json_schema()

        if file:
            return cls._dump(schema_dict, file, **kwargs)
        else:
            return cls._dumps(schema_dict, **kwargs)

    def dump_json(self, file: Optional[Union[str, Path]] = None) -> Union[None, str]:
        json_data = self.model_dump_json(by_alias=True, indent=2)

        if file:
            with open(file, "w") as f:
                print(json_data, file=f)
        else:
            return json_data
