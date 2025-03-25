# Auto generated from peh.yaml by pythongen.py version: 0.0.1
# Generation date: 2025-03-19T10:34:54
# Schema: PEH-Model
#
# id: https://w3id.org/peh/peh-model
# description: Entity and relation ontology and datamodel for Personal Exposure and Health data
# license: https://creativecommons.org/publicdomain/zero/1.0/

import dataclasses
import re
from jsonasobj2 import JsonObj, as_dict
from typing import Optional, List, Union, Dict, ClassVar, Any
from dataclasses import dataclass
from datetime import date, datetime, time
from linkml_runtime.linkml_model.meta import EnumDefinition, PermissibleValue, PvFormulaOptions

from linkml_runtime.utils.slot import Slot
from linkml_runtime.utils.metamodelcore import empty_list, empty_dict, bnode
from linkml_runtime.utils.yamlutils import YAMLRoot, extended_str, extended_float, extended_int
from linkml_runtime.utils.dataclass_extensions_376 import dataclasses_init_fn_with_kwargs
from linkml_runtime.utils.formatutils import camelcase, underscore, sfx
from linkml_runtime.utils.enumerations import EnumDefinitionImpl
from rdflib import Namespace, URIRef
from linkml_runtime.utils.curienamespace import CurieNamespace
from linkml_runtime.linkml_model.types import Boolean, Date, Datetime, Decimal, Integer, String
from linkml_runtime.utils.metamodelcore import Bool, Decimal, XSDDate, XSDDateTime

metamodel_version = "1.7.0"
version = None

# Overwrite dataclasses _init_fn to add **kwargs in __init__
dataclasses._init_fn = dataclasses_init_fn_with_kwargs

# Namespaces
IOP = CurieNamespace("iop", "http://example.org/UNKNOWN/iop/")
LINKML = CurieNamespace("linkml", "https://w3id.org/linkml/")
PEH = CurieNamespace("peh", "https://w3id.org/peh/peh-model")
PROV = CurieNamespace("prov", "http://www.w3.org/ns/prov#")
QUDT = CurieNamespace("qudt", "http://qudt.org/2.1/schema/qudt")
QUDTQK = CurieNamespace("qudtqk", "http://qudt.org/2.1/vocab/quantitykind")
RDFS = CurieNamespace("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
SCHEMA = CurieNamespace("schema", "http://schema.org/")
DEFAULT_ = PEH


# Types


# Class references
class NamedThingId(extended_str):
    pass


class GroupingId(NamedThingId):
    pass


class UnitId(NamedThingId):
    pass


class BioChemEntityId(NamedThingId):
    pass


class BioChemIdentifierSchemaId(NamedThingId):
    pass


class MatrixId(NamedThingId):
    pass


class IndicatorId(NamedThingId):
    pass


class PhysicalEntityId(NamedThingId):
    pass


class SampleId(PhysicalEntityId):
    pass


class PersonId(PhysicalEntityId):
    pass


class GeolocationId(PhysicalEntityId):
    pass


class EnvironmentId(PhysicalEntityId):
    pass


class HomeEnvironmentId(EnvironmentId):
    pass


class WorkEnvironmentId(EnvironmentId):
    pass


class ObservablePropertyId(NamedThingId):
    pass


class ObservablePropertyMetadataFieldId(NamedThingId):
    pass


class StakeholderId(NamedThingId):
    pass


class StudyEntityId(NamedThingId):
    pass


class ProjectId(StudyEntityId):
    pass


class StudyId(StudyEntityId):
    pass


class ObservationGroupId(StudyEntityId):
    pass


class StudyPopulationId(StudyEntityId):
    pass


class SampleCollectionId(StudyEntityId):
    pass


class StudySubjectId(StudyEntityId):
    pass


class StudySubjectGroupId(StudyEntityId):
    pass


class ObservationId(NamedThingId):
    pass


class MetadataObservationId(ObservationId):
    pass


class QuestionnaireObservationId(ObservationId):
    pass


class SamplingObservationId(ObservationId):
    pass


class GeospatialObservationId(ObservationId):
    pass


class ObservationResultId(NamedThingId):
    pass


class MetadataResultId(ObservationResultId):
    pass


class QuestionnaireResultId(ObservationResultId):
    pass


class SamplingResultId(ObservationResultId):
    pass


class GeospatialResultId(ObservationResultId):
    pass


class DataLayoutId(NamedThingId):
    pass


class DataLayoutSectionId(NamedThingId):
    pass


class DataRequestId(NamedThingId):
    pass


class DataStakeholderId(NamedThingId):
    pass


class ResearchObjectiveId(NamedThingId):
    pass


class ProcessingActionId(NamedThingId):
    pass


class ProcessingStepId(NamedThingId):
    pass


@dataclass(repr=False)
class EntityList(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["EntityList"]
    class_class_curie: ClassVar[str] = "peh:EntityList"
    class_name: ClassVar[str] = "EntityList"
    class_model_uri: ClassVar[URIRef] = PEH.EntityList

    matrices: Optional[Union[Dict[Union[str, MatrixId], Union[dict, "Matrix"]], List[Union[dict, "Matrix"]]]] = (
        empty_dict()
    )
    metadata_fields: Optional[
        Union[
            Dict[Union[str, ObservablePropertyMetadataFieldId], Union[dict, "ObservablePropertyMetadataField"]],
            List[Union[dict, "ObservablePropertyMetadataField"]],
        ]
    ] = empty_dict()
    biochementities: Optional[
        Union[Dict[Union[str, BioChemEntityId], Union[dict, "BioChemEntity"]], List[Union[dict, "BioChemEntity"]]]
    ] = empty_dict()
    groupings: Optional[Union[Dict[Union[str, GroupingId], Union[dict, "Grouping"]], List[Union[dict, "Grouping"]]]] = (
        empty_dict()
    )
    indicators: Optional[
        Union[Dict[Union[str, IndicatorId], Union[dict, "Indicator"]], List[Union[dict, "Indicator"]]]
    ] = empty_dict()
    units: Optional[Union[Dict[Union[str, UnitId], Union[dict, "Unit"]], List[Union[dict, "Unit"]]]] = empty_dict()
    observable_properties: Optional[
        Union[
            Dict[Union[str, ObservablePropertyId], Union[dict, "ObservableProperty"]],
            List[Union[dict, "ObservableProperty"]],
        ]
    ] = empty_dict()
    stakeholders: Optional[
        Union[Dict[Union[str, StakeholderId], Union[dict, "Stakeholder"]], List[Union[dict, "Stakeholder"]]]
    ] = empty_dict()
    projects: Optional[Union[Dict[Union[str, ProjectId], Union[dict, "Project"]], List[Union[dict, "Project"]]]] = (
        empty_dict()
    )
    studies: Optional[Union[Dict[Union[str, StudyId], Union[dict, "Study"]], List[Union[dict, "Study"]]]] = empty_dict()
    study_entities: Optional[
        Union[Dict[Union[str, StudyEntityId], Union[dict, "StudyEntity"]], List[Union[dict, "StudyEntity"]]]
    ] = empty_dict()
    physical_entities: Optional[
        Union[Dict[Union[str, PhysicalEntityId], Union[dict, "PhysicalEntity"]], List[Union[dict, "PhysicalEntity"]]]
    ] = empty_dict()
    observation_groups: Optional[
        Union[
            Dict[Union[str, ObservationGroupId], Union[dict, "ObservationGroup"]], List[Union[dict, "ObservationGroup"]]
        ]
    ] = empty_dict()
    observations: Optional[
        Union[Dict[Union[str, ObservationId], Union[dict, "Observation"]], List[Union[dict, "Observation"]]]
    ] = empty_dict()
    observation_results: Optional[
        Union[
            Dict[Union[str, ObservationResultId], Union[dict, "ObservationResult"]],
            List[Union[dict, "ObservationResult"]],
        ]
    ] = empty_dict()
    observed_values: Optional[Union[Union[dict, "ObservedValue"], List[Union[dict, "ObservedValue"]]]] = empty_list()
    layouts: Optional[
        Union[Dict[Union[str, DataLayoutId], Union[dict, "DataLayout"]], List[Union[dict, "DataLayout"]]]
    ] = empty_dict()
    data_requests: Optional[
        Union[Dict[Union[str, DataRequestId], Union[dict, "DataRequest"]], List[Union[dict, "DataRequest"]]]
    ] = empty_dict()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        self._normalize_inlined_as_list(slot_name="matrices", slot_type=Matrix, key_name="id", keyed=True)

        self._normalize_inlined_as_list(
            slot_name="metadata_fields", slot_type=ObservablePropertyMetadataField, key_name="id", keyed=True
        )

        self._normalize_inlined_as_list(slot_name="biochementities", slot_type=BioChemEntity, key_name="id", keyed=True)

        self._normalize_inlined_as_list(slot_name="groupings", slot_type=Grouping, key_name="id", keyed=True)

        self._normalize_inlined_as_list(slot_name="indicators", slot_type=Indicator, key_name="id", keyed=True)

        self._normalize_inlined_as_list(slot_name="units", slot_type=Unit, key_name="id", keyed=True)

        self._normalize_inlined_as_list(
            slot_name="observable_properties", slot_type=ObservableProperty, key_name="id", keyed=True
        )

        self._normalize_inlined_as_list(slot_name="stakeholders", slot_type=Stakeholder, key_name="id", keyed=True)

        self._normalize_inlined_as_list(slot_name="projects", slot_type=Project, key_name="id", keyed=True)

        self._normalize_inlined_as_list(slot_name="studies", slot_type=Study, key_name="id", keyed=True)

        self._normalize_inlined_as_list(slot_name="study_entities", slot_type=StudyEntity, key_name="id", keyed=True)

        self._normalize_inlined_as_list(
            slot_name="physical_entities", slot_type=PhysicalEntity, key_name="id", keyed=True
        )

        self._normalize_inlined_as_list(
            slot_name="observation_groups", slot_type=ObservationGroup, key_name="id", keyed=True
        )

        self._normalize_inlined_as_list(slot_name="observations", slot_type=Observation, key_name="id", keyed=True)

        self._normalize_inlined_as_list(
            slot_name="observation_results", slot_type=ObservationResult, key_name="id", keyed=True
        )

        if not isinstance(self.observed_values, list):
            self.observed_values = [self.observed_values] if self.observed_values is not None else []
        self.observed_values = [
            v if isinstance(v, ObservedValue) else ObservedValue(**as_dict(v)) for v in self.observed_values
        ]

        self._normalize_inlined_as_list(slot_name="layouts", slot_type=DataLayout, key_name="id", keyed=True)

        self._normalize_inlined_as_list(slot_name="data_requests", slot_type=DataRequest, key_name="id", keyed=True)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class NamedThing(YAMLRoot):
    """
    A generic grouping for any identifiable entity
    """

    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["NamedThing"]
    class_class_curie: ClassVar[str] = "peh:NamedThing"
    class_name: ClassVar[str] = "NamedThing"
    class_model_uri: ClassVar[URIRef] = PEH.NamedThing

    id: Union[str, NamedThingId] = None
    unique_name: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    label: Optional[str] = None
    remark: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, NamedThingId):
            self.id = NamedThingId(self.id)

        if self.unique_name is not None and not isinstance(self.unique_name, str):
            self.unique_name = str(self.unique_name)

        if self.name is not None and not isinstance(self.name, str):
            self.name = str(self.name)

        if self.description is not None and not isinstance(self.description, str):
            self.description = str(self.description)

        if self.label is not None and not isinstance(self.label, str):
            self.label = str(self.label)

        if self.remark is not None and not isinstance(self.remark, str):
            self.remark = str(self.remark)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Grouping(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["Grouping"]
    class_class_curie: ClassVar[str] = "peh:Grouping"
    class_name: ClassVar[str] = "Grouping"
    class_model_uri: ClassVar[URIRef] = PEH.Grouping

    id: Union[str, GroupingId] = None
    sort_order: Optional[Decimal] = None
    abstract: Optional[Union[bool, Bool]] = None
    parent_grouping_id_list: Optional[Union[Union[str, GroupingId], List[Union[str, GroupingId]]]] = empty_list()
    context_aliases: Optional[Union[Union[dict, "ContextAlias"], List[Union[dict, "ContextAlias"]]]] = empty_list()
    translations: Optional[Union[Union[dict, "Translation"], List[Union[dict, "Translation"]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, GroupingId):
            self.id = GroupingId(self.id)

        if self.sort_order is not None and not isinstance(self.sort_order, Decimal):
            self.sort_order = Decimal(self.sort_order)

        if self.abstract is not None and not isinstance(self.abstract, Bool):
            self.abstract = Bool(self.abstract)

        if not isinstance(self.parent_grouping_id_list, list):
            self.parent_grouping_id_list = (
                [self.parent_grouping_id_list] if self.parent_grouping_id_list is not None else []
            )
        self.parent_grouping_id_list = [
            v if isinstance(v, GroupingId) else GroupingId(v) for v in self.parent_grouping_id_list
        ]

        if not isinstance(self.context_aliases, list):
            self.context_aliases = [self.context_aliases] if self.context_aliases is not None else []
        self.context_aliases = [
            v if isinstance(v, ContextAlias) else ContextAlias(**as_dict(v)) for v in self.context_aliases
        ]

        if not isinstance(self.translations, list):
            self.translations = [self.translations] if self.translations is not None else []
        self.translations = [v if isinstance(v, Translation) else Translation(**as_dict(v)) for v in self.translations]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class HasValidationStatus(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["HasValidationStatus"]
    class_class_curie: ClassVar[str] = "peh:HasValidationStatus"
    class_name: ClassVar[str] = "HasValidationStatus"
    class_model_uri: ClassVar[URIRef] = PEH.HasValidationStatus

    current_validation_status: Optional[Union[str, "ValidationStatus"]] = None
    validation_history: Optional[
        Union[Union[dict, "ValidationHistoryRecord"], List[Union[dict, "ValidationHistoryRecord"]]]
    ] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.current_validation_status is not None and not isinstance(
            self.current_validation_status, ValidationStatus
        ):
            self.current_validation_status = ValidationStatus(self.current_validation_status)

        if not isinstance(self.validation_history, list):
            self.validation_history = [self.validation_history] if self.validation_history is not None else []
        self.validation_history = [
            v if isinstance(v, ValidationHistoryRecord) else ValidationHistoryRecord(**as_dict(v))
            for v in self.validation_history
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ValidationHistoryRecord(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ValidationHistoryRecord"]
    class_class_curie: ClassVar[str] = "peh:ValidationHistoryRecord"
    class_name: ClassVar[str] = "ValidationHistoryRecord"
    class_model_uri: ClassVar[URIRef] = PEH.ValidationHistoryRecord

    validation_datetime: Optional[Union[str, XSDDateTime]] = None
    validation_status: Optional[Union[str, "ValidationStatus"]] = None
    validation_actor: Optional[str] = None
    validation_institute: Optional[str] = None
    validation_remark: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.validation_datetime is not None and not isinstance(self.validation_datetime, XSDDateTime):
            self.validation_datetime = XSDDateTime(self.validation_datetime)

        if self.validation_status is not None and not isinstance(self.validation_status, ValidationStatus):
            self.validation_status = ValidationStatus(self.validation_status)

        if self.validation_actor is not None and not isinstance(self.validation_actor, str):
            self.validation_actor = str(self.validation_actor)

        if self.validation_institute is not None and not isinstance(self.validation_institute, str):
            self.validation_institute = str(self.validation_institute)

        if self.validation_remark is not None and not isinstance(self.validation_remark, str):
            self.validation_remark = str(self.validation_remark)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class HasAliases(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["HasAliases"]
    class_class_curie: ClassVar[str] = "peh:HasAliases"
    class_name: ClassVar[str] = "HasAliases"
    class_model_uri: ClassVar[URIRef] = PEH.HasAliases

    aliases: Optional[Union[str, List[str]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if not isinstance(self.aliases, list):
            self.aliases = [self.aliases] if self.aliases is not None else []
        self.aliases = [v if isinstance(v, str) else str(v) for v in self.aliases]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class HasContextAliases(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["HasContextAliases"]
    class_class_curie: ClassVar[str] = "peh:HasContextAliases"
    class_name: ClassVar[str] = "HasContextAliases"
    class_model_uri: ClassVar[URIRef] = PEH.HasContextAliases

    context_aliases: Optional[Union[Union[dict, "ContextAlias"], List[Union[dict, "ContextAlias"]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if not isinstance(self.context_aliases, list):
            self.context_aliases = [self.context_aliases] if self.context_aliases is not None else []
        self.context_aliases = [
            v if isinstance(v, ContextAlias) else ContextAlias(**as_dict(v)) for v in self.context_aliases
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ContextAlias(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ContextAlias"]
    class_class_curie: ClassVar[str] = "peh:ContextAlias"
    class_name: ClassVar[str] = "ContextAlias"
    class_model_uri: ClassVar[URIRef] = PEH.ContextAlias

    property_name: Optional[str] = None
    context: Optional[Union[str, NamedThingId]] = None
    alias: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.property_name is not None and not isinstance(self.property_name, str):
            self.property_name = str(self.property_name)

        if self.context is not None and not isinstance(self.context, NamedThingId):
            self.context = NamedThingId(self.context)

        if self.alias is not None and not isinstance(self.alias, str):
            self.alias = str(self.alias)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class HasTranslations(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["HasTranslations"]
    class_class_curie: ClassVar[str] = "peh:HasTranslations"
    class_name: ClassVar[str] = "HasTranslations"
    class_model_uri: ClassVar[URIRef] = PEH.HasTranslations

    translations: Optional[Union[Union[dict, "Translation"], List[Union[dict, "Translation"]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if not isinstance(self.translations, list):
            self.translations = [self.translations] if self.translations is not None else []
        self.translations = [v if isinstance(v, Translation) else Translation(**as_dict(v)) for v in self.translations]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Translation(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["Translation"]
    class_class_curie: ClassVar[str] = "peh:Translation"
    class_name: ClassVar[str] = "Translation"
    class_model_uri: ClassVar[URIRef] = PEH.Translation

    property_name: Optional[str] = None
    language: Optional[str] = None
    translated_value: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.property_name is not None and not isinstance(self.property_name, str):
            self.property_name = str(self.property_name)

        if self.language is not None and not isinstance(self.language, str):
            self.language = str(self.language)

        if self.translated_value is not None and not isinstance(self.translated_value, str):
            self.translated_value = str(self.translated_value)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Unit(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["Unit"]
    class_class_curie: ClassVar[str] = "peh:Unit"
    class_name: ClassVar[str] = "Unit"
    class_model_uri: ClassVar[URIRef] = PEH.Unit

    id: Union[str, UnitId] = None
    same_unit_as: Optional[Union[str, "QudtUnit"]] = None
    quantity_kind: Optional[Union[str, "QudtQuantityKind"]] = None
    context_aliases: Optional[Union[Union[dict, ContextAlias], List[Union[dict, ContextAlias]]]] = empty_list()
    translations: Optional[Union[Union[dict, Translation], List[Union[dict, Translation]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, UnitId):
            self.id = UnitId(self.id)

        if self.quantity_kind is not None and not isinstance(self.quantity_kind, QudtQuantityKind):
            self.quantity_kind = QudtQuantityKind(self.quantity_kind)

        if not isinstance(self.context_aliases, list):
            self.context_aliases = [self.context_aliases] if self.context_aliases is not None else []
        self.context_aliases = [
            v if isinstance(v, ContextAlias) else ContextAlias(**as_dict(v)) for v in self.context_aliases
        ]

        if not isinstance(self.translations, list):
            self.translations = [self.translations] if self.translations is not None else []
        self.translations = [v if isinstance(v, Translation) else Translation(**as_dict(v)) for v in self.translations]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class BioChemEntity(NamedThing):
    """
    A biological, chemical or biochemical entity that is relevant to the Personal Exposure and Health domain
    """

    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = SCHEMA["BioChemEntity"]
    class_class_curie: ClassVar[str] = "schema:BioChemEntity"
    class_name: ClassVar[str] = "BioChemEntity"
    class_model_uri: ClassVar[URIRef] = PEH.BioChemEntity

    id: Union[str, BioChemEntityId] = None
    grouping_id_list: Optional[Union[Union[str, GroupingId], List[Union[str, GroupingId]]]] = empty_list()
    molweight_grampermol: Optional[Decimal] = None
    biochemidentifiers: Optional[Union[Union[dict, "BioChemIdentifier"], List[Union[dict, "BioChemIdentifier"]]]] = (
        empty_list()
    )
    biochementity_links: Optional[Union[Union[dict, "BioChemEntityLink"], List[Union[dict, "BioChemEntityLink"]]]] = (
        empty_list()
    )
    aliases: Optional[Union[str, List[str]]] = empty_list()
    context_aliases: Optional[Union[Union[dict, ContextAlias], List[Union[dict, ContextAlias]]]] = empty_list()
    translations: Optional[Union[Union[dict, Translation], List[Union[dict, Translation]]]] = empty_list()
    current_validation_status: Optional[Union[str, "ValidationStatus"]] = None
    validation_history: Optional[
        Union[Union[dict, ValidationHistoryRecord], List[Union[dict, ValidationHistoryRecord]]]
    ] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, BioChemEntityId):
            self.id = BioChemEntityId(self.id)

        if not isinstance(self.grouping_id_list, list):
            self.grouping_id_list = [self.grouping_id_list] if self.grouping_id_list is not None else []
        self.grouping_id_list = [v if isinstance(v, GroupingId) else GroupingId(v) for v in self.grouping_id_list]

        if self.molweight_grampermol is not None and not isinstance(self.molweight_grampermol, Decimal):
            self.molweight_grampermol = Decimal(self.molweight_grampermol)

        if not isinstance(self.biochemidentifiers, list):
            self.biochemidentifiers = [self.biochemidentifiers] if self.biochemidentifiers is not None else []
        self.biochemidentifiers = [
            v if isinstance(v, BioChemIdentifier) else BioChemIdentifier(**as_dict(v)) for v in self.biochemidentifiers
        ]

        if not isinstance(self.biochementity_links, list):
            self.biochementity_links = [self.biochementity_links] if self.biochementity_links is not None else []
        self.biochementity_links = [
            v if isinstance(v, BioChemEntityLink) else BioChemEntityLink(**as_dict(v)) for v in self.biochementity_links
        ]

        if not isinstance(self.aliases, list):
            self.aliases = [self.aliases] if self.aliases is not None else []
        self.aliases = [v if isinstance(v, str) else str(v) for v in self.aliases]

        if not isinstance(self.context_aliases, list):
            self.context_aliases = [self.context_aliases] if self.context_aliases is not None else []
        self.context_aliases = [
            v if isinstance(v, ContextAlias) else ContextAlias(**as_dict(v)) for v in self.context_aliases
        ]

        if not isinstance(self.translations, list):
            self.translations = [self.translations] if self.translations is not None else []
        self.translations = [v if isinstance(v, Translation) else Translation(**as_dict(v)) for v in self.translations]

        if self.current_validation_status is not None and not isinstance(
            self.current_validation_status, ValidationStatus
        ):
            self.current_validation_status = ValidationStatus(self.current_validation_status)

        if not isinstance(self.validation_history, list):
            self.validation_history = [self.validation_history] if self.validation_history is not None else []
        self.validation_history = [
            v if isinstance(v, ValidationHistoryRecord) else ValidationHistoryRecord(**as_dict(v))
            for v in self.validation_history
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class BioChemIdentifier(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["BioChemIdentifier"]
    class_class_curie: ClassVar[str] = "peh:BioChemIdentifier"
    class_name: ClassVar[str] = "BioChemIdentifier"
    class_model_uri: ClassVar[URIRef] = PEH.BioChemIdentifier

    identifier_schema: Optional[Union[str, BioChemIdentifierSchemaId]] = None
    identifier_code: Optional[str] = None
    current_validation_status: Optional[Union[str, "ValidationStatus"]] = None
    validation_history: Optional[
        Union[Union[dict, ValidationHistoryRecord], List[Union[dict, ValidationHistoryRecord]]]
    ] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.identifier_schema is not None and not isinstance(self.identifier_schema, BioChemIdentifierSchemaId):
            self.identifier_schema = BioChemIdentifierSchemaId(self.identifier_schema)

        if self.identifier_code is not None and not isinstance(self.identifier_code, str):
            self.identifier_code = str(self.identifier_code)

        if self.current_validation_status is not None and not isinstance(
            self.current_validation_status, ValidationStatus
        ):
            self.current_validation_status = ValidationStatus(self.current_validation_status)

        if not isinstance(self.validation_history, list):
            self.validation_history = [self.validation_history] if self.validation_history is not None else []
        self.validation_history = [
            v if isinstance(v, ValidationHistoryRecord) else ValidationHistoryRecord(**as_dict(v))
            for v in self.validation_history
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class BioChemIdentifierSchema(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["BioChemIdentifierSchema"]
    class_class_curie: ClassVar[str] = "peh:BioChemIdentifierSchema"
    class_name: ClassVar[str] = "BioChemIdentifierSchema"
    class_model_uri: ClassVar[URIRef] = PEH.BioChemIdentifierSchema

    id: Union[str, BioChemIdentifierSchemaId] = None
    web_uri: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, BioChemIdentifierSchemaId):
            self.id = BioChemIdentifierSchemaId(self.id)

        if self.web_uri is not None and not isinstance(self.web_uri, str):
            self.web_uri = str(self.web_uri)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Matrix(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["Matrix"]
    class_class_curie: ClassVar[str] = "peh:Matrix"
    class_name: ClassVar[str] = "Matrix"
    class_model_uri: ClassVar[URIRef] = PEH.Matrix

    id: Union[str, MatrixId] = None
    sort_order: Optional[Decimal] = None
    aggregation_target: Optional[Union[bool, Bool]] = None
    parent_matrix: Optional[Union[str, MatrixId]] = None
    secondary_parent_matrix_id_list: Optional[Union[Union[str, MatrixId], List[Union[str, MatrixId]]]] = empty_list()
    context_aliases: Optional[Union[Union[dict, ContextAlias], List[Union[dict, ContextAlias]]]] = empty_list()
    translations: Optional[Union[Union[dict, Translation], List[Union[dict, Translation]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, MatrixId):
            self.id = MatrixId(self.id)

        if self.sort_order is not None and not isinstance(self.sort_order, Decimal):
            self.sort_order = Decimal(self.sort_order)

        if self.aggregation_target is not None and not isinstance(self.aggregation_target, Bool):
            self.aggregation_target = Bool(self.aggregation_target)

        if self.parent_matrix is not None and not isinstance(self.parent_matrix, MatrixId):
            self.parent_matrix = MatrixId(self.parent_matrix)

        if not isinstance(self.secondary_parent_matrix_id_list, list):
            self.secondary_parent_matrix_id_list = (
                [self.secondary_parent_matrix_id_list] if self.secondary_parent_matrix_id_list is not None else []
            )
        self.secondary_parent_matrix_id_list = [
            v if isinstance(v, MatrixId) else MatrixId(v) for v in self.secondary_parent_matrix_id_list
        ]

        if not isinstance(self.context_aliases, list):
            self.context_aliases = [self.context_aliases] if self.context_aliases is not None else []
        self.context_aliases = [
            v if isinstance(v, ContextAlias) else ContextAlias(**as_dict(v)) for v in self.context_aliases
        ]

        if not isinstance(self.translations, list):
            self.translations = [self.translations] if self.translations is not None else []
        self.translations = [v if isinstance(v, Translation) else Translation(**as_dict(v)) for v in self.translations]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Indicator(NamedThing):
    """
    A measurable indicator that is relevant to the Personal Exposure and Health domain
    """

    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["Indicator"]
    class_class_curie: ClassVar[str] = "peh:Indicator"
    class_name: ClassVar[str] = "Indicator"
    class_model_uri: ClassVar[URIRef] = PEH.Indicator

    id: Union[str, IndicatorId] = None
    indicator_type: Optional[Union[str, "IndicatorType"]] = None
    varname: Optional[str] = None
    property: Optional[str] = None
    quantity_kind: Optional[Union[str, "QudtQuantityKind"]] = None
    matrix: Optional[Union[str, MatrixId]] = None
    constraints: Optional[Union[str, List[str]]] = empty_list()
    grouping_id_list: Optional[Union[Union[str, GroupingId], List[Union[str, GroupingId]]]] = empty_list()
    relevant_observable_entity_types: Optional[
        Union[Union[str, "ObservableEntityType"], List[Union[str, "ObservableEntityType"]]]
    ] = empty_list()
    biochementity_links: Optional[Union[Union[dict, "BioChemEntityLink"], List[Union[dict, "BioChemEntityLink"]]]] = (
        empty_list()
    )

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, IndicatorId):
            self.id = IndicatorId(self.id)

        if self.indicator_type is not None and not isinstance(self.indicator_type, IndicatorType):
            self.indicator_type = IndicatorType(self.indicator_type)

        if self.varname is not None and not isinstance(self.varname, str):
            self.varname = str(self.varname)

        if self.property is not None and not isinstance(self.property, str):
            self.property = str(self.property)

        if self.quantity_kind is not None and not isinstance(self.quantity_kind, QudtQuantityKind):
            self.quantity_kind = QudtQuantityKind(self.quantity_kind)

        if self.matrix is not None and not isinstance(self.matrix, MatrixId):
            self.matrix = MatrixId(self.matrix)

        if not isinstance(self.constraints, list):
            self.constraints = [self.constraints] if self.constraints is not None else []
        self.constraints = [v if isinstance(v, str) else str(v) for v in self.constraints]

        if not isinstance(self.grouping_id_list, list):
            self.grouping_id_list = [self.grouping_id_list] if self.grouping_id_list is not None else []
        self.grouping_id_list = [v if isinstance(v, GroupingId) else GroupingId(v) for v in self.grouping_id_list]

        if not isinstance(self.relevant_observable_entity_types, list):
            self.relevant_observable_entity_types = (
                [self.relevant_observable_entity_types] if self.relevant_observable_entity_types is not None else []
            )
        self.relevant_observable_entity_types = [
            v if isinstance(v, ObservableEntityType) else ObservableEntityType(v)
            for v in self.relevant_observable_entity_types
        ]

        if not isinstance(self.biochementity_links, list):
            self.biochementity_links = [self.biochementity_links] if self.biochementity_links is not None else []
        self.biochementity_links = [
            v if isinstance(v, BioChemEntityLink) else BioChemEntityLink(**as_dict(v)) for v in self.biochementity_links
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class BioChemEntityLink(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["BioChemEntityLink"]
    class_class_curie: ClassVar[str] = "peh:BioChemEntityLink"
    class_name: ClassVar[str] = "BioChemEntityLink"
    class_model_uri: ClassVar[URIRef] = PEH.BioChemEntityLink

    biochementity_linktype: Optional[Union[str, "BioChemEntityLinkType"]] = None
    biochementity: Optional[Union[str, BioChemEntityId]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.biochementity_linktype is not None and not isinstance(
            self.biochementity_linktype, BioChemEntityLinkType
        ):
            self.biochementity_linktype = BioChemEntityLinkType(self.biochementity_linktype)

        if self.biochementity is not None and not isinstance(self.biochementity, BioChemEntityId):
            self.biochementity = BioChemEntityId(self.biochementity)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class PhysicalEntity(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["PhysicalEntity"]
    class_class_curie: ClassVar[str] = "peh:PhysicalEntity"
    class_name: ClassVar[str] = "PhysicalEntity"
    class_model_uri: ClassVar[URIRef] = PEH.PhysicalEntity

    id: Union[str, PhysicalEntityId] = None
    physical_entity_links: Optional[
        Union[Union[dict, "PhysicalEntityLink"], List[Union[dict, "PhysicalEntityLink"]]]
    ] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if not isinstance(self.physical_entity_links, list):
            self.physical_entity_links = [self.physical_entity_links] if self.physical_entity_links is not None else []
        self.physical_entity_links = [
            v if isinstance(v, PhysicalEntityLink) else PhysicalEntityLink(**as_dict(v))
            for v in self.physical_entity_links
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class PhysicalEntityLink(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["PhysicalEntityLink"]
    class_class_curie: ClassVar[str] = "peh:PhysicalEntityLink"
    class_name: ClassVar[str] = "PhysicalEntityLink"
    class_model_uri: ClassVar[URIRef] = PEH.PhysicalEntityLink

    linktype: Optional[Union[str, "LinkType"]] = None
    physical_entity: Optional[Union[str, PhysicalEntityId]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.linktype is not None and not isinstance(self.linktype, LinkType):
            self.linktype = LinkType(self.linktype)

        if self.physical_entity is not None and not isinstance(self.physical_entity, PhysicalEntityId):
            self.physical_entity = PhysicalEntityId(self.physical_entity)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Sample(PhysicalEntity):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["Sample"]
    class_class_curie: ClassVar[str] = "peh:Sample"
    class_name: ClassVar[str] = "Sample"
    class_model_uri: ClassVar[URIRef] = PEH.Sample

    id: Union[str, SampleId] = None
    matrix: Optional[Union[str, MatrixId]] = None
    constraints: Optional[Union[str, List[str]]] = empty_list()
    sampled_in_project: Optional[Union[str, ProjectId]] = None
    physical_label: Optional[str] = None
    collection_date: Optional[Union[str, XSDDate]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, SampleId):
            self.id = SampleId(self.id)

        if self.matrix is not None and not isinstance(self.matrix, MatrixId):
            self.matrix = MatrixId(self.matrix)

        if not isinstance(self.constraints, list):
            self.constraints = [self.constraints] if self.constraints is not None else []
        self.constraints = [v if isinstance(v, str) else str(v) for v in self.constraints]

        if self.sampled_in_project is not None and not isinstance(self.sampled_in_project, ProjectId):
            self.sampled_in_project = ProjectId(self.sampled_in_project)

        if self.physical_label is not None and not isinstance(self.physical_label, str):
            self.physical_label = str(self.physical_label)

        if self.collection_date is not None and not isinstance(self.collection_date, XSDDate):
            self.collection_date = XSDDate(self.collection_date)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Person(PhysicalEntity):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["Person"]
    class_class_curie: ClassVar[str] = "peh:Person"
    class_name: ClassVar[str] = "Person"
    class_model_uri: ClassVar[URIRef] = PEH.Person

    id: Union[str, PersonId] = None
    recruited_in_project: Optional[Union[str, ProjectId]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, PersonId):
            self.id = PersonId(self.id)

        if self.recruited_in_project is not None and not isinstance(self.recruited_in_project, ProjectId):
            self.recruited_in_project = ProjectId(self.recruited_in_project)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Geolocation(PhysicalEntity):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["Geolocation"]
    class_class_curie: ClassVar[str] = "peh:Geolocation"
    class_name: ClassVar[str] = "Geolocation"
    class_model_uri: ClassVar[URIRef] = PEH.Geolocation

    id: Union[str, GeolocationId] = None
    location: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, GeolocationId):
            self.id = GeolocationId(self.id)

        if self.location is not None and not isinstance(self.location, str):
            self.location = str(self.location)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Environment(PhysicalEntity):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["Environment"]
    class_class_curie: ClassVar[str] = "peh:Environment"
    class_name: ClassVar[str] = "Environment"
    class_model_uri: ClassVar[URIRef] = PEH.Environment

    id: Union[str, EnvironmentId] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, EnvironmentId):
            self.id = EnvironmentId(self.id)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class HomeEnvironment(Environment):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["HomeEnvironment"]
    class_class_curie: ClassVar[str] = "peh:HomeEnvironment"
    class_name: ClassVar[str] = "HomeEnvironment"
    class_model_uri: ClassVar[URIRef] = PEH.HomeEnvironment

    id: Union[str, HomeEnvironmentId] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, HomeEnvironmentId):
            self.id = HomeEnvironmentId(self.id)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class WorkEnvironment(Environment):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["WorkEnvironment"]
    class_class_curie: ClassVar[str] = "peh:WorkEnvironment"
    class_name: ClassVar[str] = "WorkEnvironment"
    class_model_uri: ClassVar[URIRef] = PEH.WorkEnvironment

    id: Union[str, WorkEnvironmentId] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, WorkEnvironmentId):
            self.id = WorkEnvironmentId(self.id)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ObservableProperty(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ObservableProperty"]
    class_class_curie: ClassVar[str] = "peh:ObservableProperty"
    class_name: ClassVar[str] = "ObservableProperty"
    class_model_uri: ClassVar[URIRef] = PEH.ObservableProperty

    id: Union[str, ObservablePropertyId] = None
    value_type: Optional[str] = None
    categorical: Optional[Union[bool, Bool]] = None
    multivalued: Optional[Union[bool, Bool]] = None
    value_options: Optional[
        Union[Union[dict, "ObservablePropertyValueOption"], List[Union[dict, "ObservablePropertyValueOption"]]]
    ] = empty_list()
    value_metadata: Optional[
        Union[Union[dict, "ObservablePropertyMetadataElement"], List[Union[dict, "ObservablePropertyMetadataElement"]]]
    ] = empty_list()
    quantity_kind: Optional[Union[str, "QudtQuantityKind"]] = None
    default_unit: Optional[str] = None
    default_unit_label: Optional[str] = None
    default_required: Optional[Union[bool, Bool]] = None
    default_zeroallowed: Optional[Union[bool, Bool]] = None
    default_significantdecimals: Optional[int] = None
    default_immutable: Optional[Union[bool, Bool]] = None
    grouping_id_list: Optional[Union[Union[str, GroupingId], List[Union[str, GroupingId]]]] = empty_list()
    default_observation_result_type: Optional[Union[str, "ObservationResultType"]] = None
    relevant_observable_entity_types: Optional[
        Union[Union[str, "ObservableEntityType"], List[Union[str, "ObservableEntityType"]]]
    ] = empty_list()
    relevant_observation_types: Optional[Union[Union[str, "ObservationType"], List[Union[str, "ObservationType"]]]] = (
        empty_list()
    )
    indicator: Optional[Union[str, IndicatorId]] = None
    varname: Optional[str] = None
    calculation_designs: Optional[Union[Union[dict, "CalculationDesign"], List[Union[dict, "CalculationDesign"]]]] = (
        empty_list()
    )
    validation_designs: Optional[Union[Union[dict, "ValidationDesign"], List[Union[dict, "ValidationDesign"]]]] = (
        empty_list()
    )
    translations: Optional[Union[Union[dict, Translation], List[Union[dict, Translation]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, ObservablePropertyId):
            self.id = ObservablePropertyId(self.id)

        if self.value_type is not None and not isinstance(self.value_type, str):
            self.value_type = str(self.value_type)

        if self.categorical is not None and not isinstance(self.categorical, Bool):
            self.categorical = Bool(self.categorical)

        if self.multivalued is not None and not isinstance(self.multivalued, Bool):
            self.multivalued = Bool(self.multivalued)

        if not isinstance(self.value_options, list):
            self.value_options = [self.value_options] if self.value_options is not None else []
        self.value_options = [
            v if isinstance(v, ObservablePropertyValueOption) else ObservablePropertyValueOption(**as_dict(v))
            for v in self.value_options
        ]

        if not isinstance(self.value_metadata, list):
            self.value_metadata = [self.value_metadata] if self.value_metadata is not None else []
        self.value_metadata = [
            v if isinstance(v, ObservablePropertyMetadataElement) else ObservablePropertyMetadataElement(**as_dict(v))
            for v in self.value_metadata
        ]

        if self.quantity_kind is not None and not isinstance(self.quantity_kind, QudtQuantityKind):
            self.quantity_kind = QudtQuantityKind(self.quantity_kind)

        if self.default_unit is not None and not isinstance(self.default_unit, str):
            self.default_unit = str(self.default_unit)

        if self.default_unit_label is not None and not isinstance(self.default_unit_label, str):
            self.default_unit_label = str(self.default_unit_label)

        if self.default_required is not None and not isinstance(self.default_required, Bool):
            self.default_required = Bool(self.default_required)

        if self.default_zeroallowed is not None and not isinstance(self.default_zeroallowed, Bool):
            self.default_zeroallowed = Bool(self.default_zeroallowed)

        if self.default_significantdecimals is not None and not isinstance(self.default_significantdecimals, int):
            self.default_significantdecimals = int(self.default_significantdecimals)

        if self.default_immutable is not None and not isinstance(self.default_immutable, Bool):
            self.default_immutable = Bool(self.default_immutable)

        if not isinstance(self.grouping_id_list, list):
            self.grouping_id_list = [self.grouping_id_list] if self.grouping_id_list is not None else []
        self.grouping_id_list = [v if isinstance(v, GroupingId) else GroupingId(v) for v in self.grouping_id_list]

        if self.default_observation_result_type is not None and not isinstance(
            self.default_observation_result_type, ObservationResultType
        ):
            self.default_observation_result_type = ObservationResultType(self.default_observation_result_type)

        if not isinstance(self.relevant_observable_entity_types, list):
            self.relevant_observable_entity_types = (
                [self.relevant_observable_entity_types] if self.relevant_observable_entity_types is not None else []
            )
        self.relevant_observable_entity_types = [
            v if isinstance(v, ObservableEntityType) else ObservableEntityType(v)
            for v in self.relevant_observable_entity_types
        ]

        if not isinstance(self.relevant_observation_types, list):
            self.relevant_observation_types = (
                [self.relevant_observation_types] if self.relevant_observation_types is not None else []
            )
        self.relevant_observation_types = [
            v if isinstance(v, ObservationType) else ObservationType(v) for v in self.relevant_observation_types
        ]

        if self.indicator is not None and not isinstance(self.indicator, IndicatorId):
            self.indicator = IndicatorId(self.indicator)

        if self.varname is not None and not isinstance(self.varname, str):
            self.varname = str(self.varname)

        if not isinstance(self.calculation_designs, list):
            self.calculation_designs = [self.calculation_designs] if self.calculation_designs is not None else []
        self.calculation_designs = [
            v if isinstance(v, CalculationDesign) else CalculationDesign(**as_dict(v)) for v in self.calculation_designs
        ]

        if not isinstance(self.validation_designs, list):
            self.validation_designs = [self.validation_designs] if self.validation_designs is not None else []
        self.validation_designs = [
            v if isinstance(v, ValidationDesign) else ValidationDesign(**as_dict(v)) for v in self.validation_designs
        ]

        if not isinstance(self.translations, list):
            self.translations = [self.translations] if self.translations is not None else []
        self.translations = [v if isinstance(v, Translation) else Translation(**as_dict(v)) for v in self.translations]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ObservablePropertyValueOption(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ObservablePropertyValueOption"]
    class_class_curie: ClassVar[str] = "peh:ObservablePropertyValueOption"
    class_name: ClassVar[str] = "ObservablePropertyValueOption"
    class_model_uri: ClassVar[URIRef] = PEH.ObservablePropertyValueOption

    key: Optional[str] = None
    value: Optional[str] = None
    label: Optional[str] = None
    context_aliases: Optional[Union[Union[dict, ContextAlias], List[Union[dict, ContextAlias]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.key is not None and not isinstance(self.key, str):
            self.key = str(self.key)

        if self.value is not None and not isinstance(self.value, str):
            self.value = str(self.value)

        if self.label is not None and not isinstance(self.label, str):
            self.label = str(self.label)

        if not isinstance(self.context_aliases, list):
            self.context_aliases = [self.context_aliases] if self.context_aliases is not None else []
        self.context_aliases = [
            v if isinstance(v, ContextAlias) else ContextAlias(**as_dict(v)) for v in self.context_aliases
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ObservablePropertyMetadataElement(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ObservablePropertyMetadataElement"]
    class_class_curie: ClassVar[str] = "peh:ObservablePropertyMetadataElement"
    class_name: ClassVar[str] = "ObservablePropertyMetadataElement"
    class_model_uri: ClassVar[URIRef] = PEH.ObservablePropertyMetadataElement

    field: Optional[Union[str, ObservablePropertyMetadataFieldId]] = None
    value: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.field is not None and not isinstance(self.field, ObservablePropertyMetadataFieldId):
            self.field = ObservablePropertyMetadataFieldId(self.field)

        if self.value is not None and not isinstance(self.value, str):
            self.value = str(self.value)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ObservablePropertyMetadataField(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ObservablePropertyMetadataField"]
    class_class_curie: ClassVar[str] = "peh:ObservablePropertyMetadataField"
    class_name: ClassVar[str] = "ObservablePropertyMetadataField"
    class_model_uri: ClassVar[URIRef] = PEH.ObservablePropertyMetadataField

    id: Union[str, ObservablePropertyMetadataFieldId] = None
    value_type: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, ObservablePropertyMetadataFieldId):
            self.id = ObservablePropertyMetadataFieldId(self.id)

        if self.value_type is not None and not isinstance(self.value_type, str):
            self.value_type = str(self.value_type)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class CalculationDesign(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["CalculationDesign"]
    class_class_curie: ClassVar[str] = "peh:CalculationDesign"
    class_name: ClassVar[str] = "CalculationDesign"
    class_model_uri: ClassVar[URIRef] = PEH.CalculationDesign

    calculation_name: Optional[str] = None
    calculation_implementation_as_json: Optional[str] = None
    calculation_implementation: Optional[Union[dict, "CalculationImplementation"]] = None
    conditional: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.calculation_name is not None and not isinstance(self.calculation_name, str):
            self.calculation_name = str(self.calculation_name)

        if self.calculation_implementation_as_json is not None and not isinstance(
            self.calculation_implementation_as_json, str
        ):
            self.calculation_implementation_as_json = str(self.calculation_implementation_as_json)

        if self.calculation_implementation is not None and not isinstance(
            self.calculation_implementation, CalculationImplementation
        ):
            self.calculation_implementation = CalculationImplementation(**as_dict(self.calculation_implementation))

        if self.conditional is not None and not isinstance(self.conditional, str):
            self.conditional = str(self.conditional)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class CalculationImplementation(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["CalculationImplementation"]
    class_class_curie: ClassVar[str] = "peh:CalculationImplementation"
    class_name: ClassVar[str] = "CalculationImplementation"
    class_model_uri: ClassVar[URIRef] = PEH.CalculationImplementation

    function_name: Optional[str] = None
    function_args: Optional[Union[Union[dict, "CalculationArgument"], List[Union[dict, "CalculationArgument"]]]] = (
        empty_list()
    )
    function_kwargs: Optional[
        Union[Union[dict, "CalculationKeywordArgument"], List[Union[dict, "CalculationKeywordArgument"]]]
    ] = empty_list()
    function_results: Optional[Union[Union[dict, "CalculationResult"], List[Union[dict, "CalculationResult"]]]] = (
        empty_list()
    )

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.function_name is not None and not isinstance(self.function_name, str):
            self.function_name = str(self.function_name)

        if not isinstance(self.function_args, list):
            self.function_args = [self.function_args] if self.function_args is not None else []
        self.function_args = [
            v if isinstance(v, CalculationArgument) else CalculationArgument(**as_dict(v)) for v in self.function_args
        ]

        if not isinstance(self.function_kwargs, list):
            self.function_kwargs = [self.function_kwargs] if self.function_kwargs is not None else []
        self.function_kwargs = [
            v if isinstance(v, CalculationKeywordArgument) else CalculationKeywordArgument(**as_dict(v))
            for v in self.function_kwargs
        ]

        if not isinstance(self.function_results, list):
            self.function_results = [self.function_results] if self.function_results is not None else []
        self.function_results = [
            v if isinstance(v, CalculationResult) else CalculationResult(**as_dict(v)) for v in self.function_results
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class CalculationArgument(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["CalculationArgument"]
    class_class_curie: ClassVar[str] = "peh:CalculationArgument"
    class_name: ClassVar[str] = "CalculationArgument"
    class_model_uri: ClassVar[URIRef] = PEH.CalculationArgument

    source_path: Optional[str] = None
    varname: Optional[str] = None
    process_state: Optional[str] = None
    imputation_state: Optional[str] = None
    value_type: Optional[str] = None
    unit: Optional[Union[str, UnitId]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.source_path is not None and not isinstance(self.source_path, str):
            self.source_path = str(self.source_path)

        if self.varname is not None and not isinstance(self.varname, str):
            self.varname = str(self.varname)

        if self.process_state is not None and not isinstance(self.process_state, str):
            self.process_state = str(self.process_state)

        if self.imputation_state is not None and not isinstance(self.imputation_state, str):
            self.imputation_state = str(self.imputation_state)

        if self.value_type is not None and not isinstance(self.value_type, str):
            self.value_type = str(self.value_type)

        if self.unit is not None and not isinstance(self.unit, UnitId):
            self.unit = UnitId(self.unit)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class CalculationKeywordArgument(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["CalculationKeywordArgument"]
    class_class_curie: ClassVar[str] = "peh:CalculationKeywordArgument"
    class_name: ClassVar[str] = "CalculationKeywordArgument"
    class_model_uri: ClassVar[URIRef] = PEH.CalculationKeywordArgument

    mapping_name: Optional[str] = None
    source_path: Optional[str] = None
    varname: Optional[str] = None
    process_state: Optional[str] = None
    imputation_state: Optional[str] = None
    value_type: Optional[str] = None
    unit: Optional[Union[str, UnitId]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.mapping_name is not None and not isinstance(self.mapping_name, str):
            self.mapping_name = str(self.mapping_name)

        if self.source_path is not None and not isinstance(self.source_path, str):
            self.source_path = str(self.source_path)

        if self.varname is not None and not isinstance(self.varname, str):
            self.varname = str(self.varname)

        if self.process_state is not None and not isinstance(self.process_state, str):
            self.process_state = str(self.process_state)

        if self.imputation_state is not None and not isinstance(self.imputation_state, str):
            self.imputation_state = str(self.imputation_state)

        if self.value_type is not None and not isinstance(self.value_type, str):
            self.value_type = str(self.value_type)

        if self.unit is not None and not isinstance(self.unit, UnitId):
            self.unit = UnitId(self.unit)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class CalculationResult(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["CalculationResult"]
    class_class_curie: ClassVar[str] = "peh:CalculationResult"
    class_name: ClassVar[str] = "CalculationResult"
    class_model_uri: ClassVar[URIRef] = PEH.CalculationResult

    mapping_name: Optional[str] = None
    value_type: Optional[str] = None
    unit: Optional[Union[str, UnitId]] = None
    round_decimals: Optional[int] = None
    scale_factor: Optional[Decimal] = None
    destination_path: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.mapping_name is not None and not isinstance(self.mapping_name, str):
            self.mapping_name = str(self.mapping_name)

        if self.value_type is not None and not isinstance(self.value_type, str):
            self.value_type = str(self.value_type)

        if self.unit is not None and not isinstance(self.unit, UnitId):
            self.unit = UnitId(self.unit)

        if self.round_decimals is not None and not isinstance(self.round_decimals, int):
            self.round_decimals = int(self.round_decimals)

        if self.scale_factor is not None and not isinstance(self.scale_factor, Decimal):
            self.scale_factor = Decimal(self.scale_factor)

        if self.destination_path is not None and not isinstance(self.destination_path, str):
            self.destination_path = str(self.destination_path)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ValidationDesign(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ValidationDesign"]
    class_class_curie: ClassVar[str] = "peh:ValidationDesign"
    class_name: ClassVar[str] = "ValidationDesign"
    class_model_uri: ClassVar[URIRef] = PEH.ValidationDesign

    conditional: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.conditional is not None and not isinstance(self.conditional, str):
            self.conditional = str(self.conditional)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Contact(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["Contact"]
    class_class_curie: ClassVar[str] = "peh:Contact"
    class_name: ClassVar[str] = "Contact"
    class_model_uri: ClassVar[URIRef] = PEH.Contact

    name: Optional[str] = None
    orcid: Optional[str] = None
    contact_roles: Optional[Union[Union[str, "ContactRole"], List[Union[str, "ContactRole"]]]] = empty_list()
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    context_aliases: Optional[Union[Union[dict, ContextAlias], List[Union[dict, ContextAlias]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.name is not None and not isinstance(self.name, str):
            self.name = str(self.name)

        if self.orcid is not None and not isinstance(self.orcid, str):
            self.orcid = str(self.orcid)

        if not isinstance(self.contact_roles, list):
            self.contact_roles = [self.contact_roles] if self.contact_roles is not None else []
        self.contact_roles = [v if isinstance(v, ContactRole) else ContactRole(v) for v in self.contact_roles]

        if self.contact_email is not None and not isinstance(self.contact_email, str):
            self.contact_email = str(self.contact_email)

        if self.contact_phone is not None and not isinstance(self.contact_phone, str):
            self.contact_phone = str(self.contact_phone)

        if not isinstance(self.context_aliases, list):
            self.context_aliases = [self.context_aliases] if self.context_aliases is not None else []
        self.context_aliases = [
            v if isinstance(v, ContextAlias) else ContextAlias(**as_dict(v)) for v in self.context_aliases
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Stakeholder(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["Stakeholder"]
    class_class_curie: ClassVar[str] = "peh:Stakeholder"
    class_name: ClassVar[str] = "Stakeholder"
    class_model_uri: ClassVar[URIRef] = PEH.Stakeholder

    id: Union[str, StakeholderId] = None
    rorid: Optional[str] = None
    geographic_scope: Optional[str] = None
    translations: Optional[Union[Union[dict, Translation], List[Union[dict, Translation]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, StakeholderId):
            self.id = StakeholderId(self.id)

        if self.rorid is not None and not isinstance(self.rorid, str):
            self.rorid = str(self.rorid)

        if self.geographic_scope is not None and not isinstance(self.geographic_scope, str):
            self.geographic_scope = str(self.geographic_scope)

        if not isinstance(self.translations, list):
            self.translations = [self.translations] if self.translations is not None else []
        self.translations = [v if isinstance(v, Translation) else Translation(**as_dict(v)) for v in self.translations]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ProjectStakeholder(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ProjectStakeholder"]
    class_class_curie: ClassVar[str] = "peh:ProjectStakeholder"
    class_name: ClassVar[str] = "ProjectStakeholder"
    class_model_uri: ClassVar[URIRef] = PEH.ProjectStakeholder

    stakeholder: Optional[Union[str, StakeholderId]] = None
    project_roles: Optional[Union[Union[str, "ProjectRole"], List[Union[str, "ProjectRole"]]]] = empty_list()
    contacts: Optional[Union[Union[dict, Contact], List[Union[dict, Contact]]]] = empty_list()
    translations: Optional[Union[Union[dict, Translation], List[Union[dict, Translation]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.stakeholder is not None and not isinstance(self.stakeholder, StakeholderId):
            self.stakeholder = StakeholderId(self.stakeholder)

        if not isinstance(self.project_roles, list):
            self.project_roles = [self.project_roles] if self.project_roles is not None else []
        self.project_roles = [v if isinstance(v, ProjectRole) else ProjectRole(v) for v in self.project_roles]

        if not isinstance(self.contacts, list):
            self.contacts = [self.contacts] if self.contacts is not None else []
        self.contacts = [v if isinstance(v, Contact) else Contact(**as_dict(v)) for v in self.contacts]

        if not isinstance(self.translations, list):
            self.translations = [self.translations] if self.translations is not None else []
        self.translations = [v if isinstance(v, Translation) else Translation(**as_dict(v)) for v in self.translations]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class StudyEntity(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["StudyEntity"]
    class_class_curie: ClassVar[str] = "peh:StudyEntity"
    class_name: ClassVar[str] = "StudyEntity"
    class_model_uri: ClassVar[URIRef] = PEH.StudyEntity

    id: Union[str, StudyEntityId] = None
    physical_entity: Optional[Union[str, PhysicalEntityId]] = None
    study_entity_links: Optional[Union[Union[dict, "StudyEntityLink"], List[Union[dict, "StudyEntityLink"]]]] = (
        empty_list()
    )

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.physical_entity is not None and not isinstance(self.physical_entity, PhysicalEntityId):
            self.physical_entity = PhysicalEntityId(self.physical_entity)

        if not isinstance(self.study_entity_links, list):
            self.study_entity_links = [self.study_entity_links] if self.study_entity_links is not None else []
        self.study_entity_links = [
            v if isinstance(v, StudyEntityLink) else StudyEntityLink(**as_dict(v)) for v in self.study_entity_links
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Project(StudyEntity):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["Project"]
    class_class_curie: ClassVar[str] = "peh:Project"
    class_name: ClassVar[str] = "Project"
    class_model_uri: ClassVar[URIRef] = PEH.Project

    id: Union[str, ProjectId] = None
    default_language: Optional[str] = None
    project_stakeholders: Optional[Union[Union[dict, ProjectStakeholder], List[Union[dict, ProjectStakeholder]]]] = (
        empty_list()
    )
    start_date: Optional[Union[str, XSDDate]] = None
    end_date: Optional[Union[str, XSDDate]] = None
    study_id_list: Optional[Union[Union[str, StudyId], List[Union[str, StudyId]]]] = empty_list()
    translations: Optional[Union[Union[dict, Translation], List[Union[dict, Translation]]]] = empty_list()
    context_aliases: Optional[Union[Union[dict, ContextAlias], List[Union[dict, ContextAlias]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, ProjectId):
            self.id = ProjectId(self.id)

        if self.default_language is not None and not isinstance(self.default_language, str):
            self.default_language = str(self.default_language)

        if not isinstance(self.project_stakeholders, list):
            self.project_stakeholders = [self.project_stakeholders] if self.project_stakeholders is not None else []
        self.project_stakeholders = [
            v if isinstance(v, ProjectStakeholder) else ProjectStakeholder(**as_dict(v))
            for v in self.project_stakeholders
        ]

        if self.start_date is not None and not isinstance(self.start_date, XSDDate):
            self.start_date = XSDDate(self.start_date)

        if self.end_date is not None and not isinstance(self.end_date, XSDDate):
            self.end_date = XSDDate(self.end_date)

        if not isinstance(self.study_id_list, list):
            self.study_id_list = [self.study_id_list] if self.study_id_list is not None else []
        self.study_id_list = [v if isinstance(v, StudyId) else StudyId(v) for v in self.study_id_list]

        if not isinstance(self.translations, list):
            self.translations = [self.translations] if self.translations is not None else []
        self.translations = [v if isinstance(v, Translation) else Translation(**as_dict(v)) for v in self.translations]

        if not isinstance(self.context_aliases, list):
            self.context_aliases = [self.context_aliases] if self.context_aliases is not None else []
        self.context_aliases = [
            v if isinstance(v, ContextAlias) else ContextAlias(**as_dict(v)) for v in self.context_aliases
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class StudyEntityLink(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["StudyEntityLink"]
    class_class_curie: ClassVar[str] = "peh:StudyEntityLink"
    class_name: ClassVar[str] = "StudyEntityLink"
    class_model_uri: ClassVar[URIRef] = PEH.StudyEntityLink

    linktype: Optional[Union[str, "LinkType"]] = None
    study_entity: Optional[Union[str, StudyEntityId]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.linktype is not None and not isinstance(self.linktype, LinkType):
            self.linktype = LinkType(self.linktype)

        if self.study_entity is not None and not isinstance(self.study_entity, StudyEntityId):
            self.study_entity = StudyEntityId(self.study_entity)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Study(StudyEntity):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["Study"]
    class_class_curie: ClassVar[str] = "peh:Study"
    class_name: ClassVar[str] = "Study"
    class_model_uri: ClassVar[URIRef] = PEH.Study

    id: Union[str, StudyId] = None
    default_language: Optional[str] = None
    study_stakeholders: Optional[Union[Union[dict, "StudyStakeholder"], List[Union[dict, "StudyStakeholder"]]]] = (
        empty_list()
    )
    start_date: Optional[Union[str, XSDDate]] = None
    end_date: Optional[Union[str, XSDDate]] = None
    observation_group_id_list: Optional[Union[Union[str, ObservationGroupId], List[Union[str, ObservationGroupId]]]] = (
        empty_list()
    )
    study_entity_id_list: Optional[Union[Union[str, StudyEntityId], List[Union[str, StudyEntityId]]]] = empty_list()
    project_id_list: Optional[Union[Union[str, ProjectId], List[Union[str, ProjectId]]]] = empty_list()
    translations: Optional[Union[Union[dict, Translation], List[Union[dict, Translation]]]] = empty_list()
    context_aliases: Optional[Union[Union[dict, ContextAlias], List[Union[dict, ContextAlias]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, StudyId):
            self.id = StudyId(self.id)

        if self.default_language is not None and not isinstance(self.default_language, str):
            self.default_language = str(self.default_language)

        if not isinstance(self.study_stakeholders, list):
            self.study_stakeholders = [self.study_stakeholders] if self.study_stakeholders is not None else []
        self.study_stakeholders = [
            v if isinstance(v, StudyStakeholder) else StudyStakeholder(**as_dict(v)) for v in self.study_stakeholders
        ]

        if self.start_date is not None and not isinstance(self.start_date, XSDDate):
            self.start_date = XSDDate(self.start_date)

        if self.end_date is not None and not isinstance(self.end_date, XSDDate):
            self.end_date = XSDDate(self.end_date)

        if not isinstance(self.observation_group_id_list, list):
            self.observation_group_id_list = (
                [self.observation_group_id_list] if self.observation_group_id_list is not None else []
            )
        self.observation_group_id_list = [
            v if isinstance(v, ObservationGroupId) else ObservationGroupId(v) for v in self.observation_group_id_list
        ]

        if not isinstance(self.study_entity_id_list, list):
            self.study_entity_id_list = [self.study_entity_id_list] if self.study_entity_id_list is not None else []
        self.study_entity_id_list = [
            v if isinstance(v, StudyEntityId) else StudyEntityId(v) for v in self.study_entity_id_list
        ]

        if not isinstance(self.project_id_list, list):
            self.project_id_list = [self.project_id_list] if self.project_id_list is not None else []
        self.project_id_list = [v if isinstance(v, ProjectId) else ProjectId(v) for v in self.project_id_list]

        if not isinstance(self.translations, list):
            self.translations = [self.translations] if self.translations is not None else []
        self.translations = [v if isinstance(v, Translation) else Translation(**as_dict(v)) for v in self.translations]

        if not isinstance(self.context_aliases, list):
            self.context_aliases = [self.context_aliases] if self.context_aliases is not None else []
        self.context_aliases = [
            v if isinstance(v, ContextAlias) else ContextAlias(**as_dict(v)) for v in self.context_aliases
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class StudyStakeholder(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["StudyStakeholder"]
    class_class_curie: ClassVar[str] = "peh:StudyStakeholder"
    class_name: ClassVar[str] = "StudyStakeholder"
    class_model_uri: ClassVar[URIRef] = PEH.StudyStakeholder

    stakeholder: Optional[Union[str, StakeholderId]] = None
    study_roles: Optional[Union[Union[str, "StudyRole"], List[Union[str, "StudyRole"]]]] = empty_list()
    contacts: Optional[Union[Union[dict, Contact], List[Union[dict, Contact]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.stakeholder is not None and not isinstance(self.stakeholder, StakeholderId):
            self.stakeholder = StakeholderId(self.stakeholder)

        if not isinstance(self.study_roles, list):
            self.study_roles = [self.study_roles] if self.study_roles is not None else []
        self.study_roles = [v if isinstance(v, StudyRole) else StudyRole(v) for v in self.study_roles]

        if not isinstance(self.contacts, list):
            self.contacts = [self.contacts] if self.contacts is not None else []
        self.contacts = [v if isinstance(v, Contact) else Contact(**as_dict(v)) for v in self.contacts]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ObservationGroup(StudyEntity):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ObservationGroup"]
    class_class_curie: ClassVar[str] = "peh:ObservationGroup"
    class_name: ClassVar[str] = "ObservationGroup"
    class_model_uri: ClassVar[URIRef] = PEH.ObservationGroup

    id: Union[str, ObservationGroupId] = None
    sort_order: Optional[Decimal] = None
    start_date: Optional[Union[str, XSDDate]] = None
    end_date: Optional[Union[str, XSDDate]] = None
    observation_id_list: Optional[Union[Union[str, ObservationId], List[Union[str, ObservationId]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, ObservationGroupId):
            self.id = ObservationGroupId(self.id)

        if self.sort_order is not None and not isinstance(self.sort_order, Decimal):
            self.sort_order = Decimal(self.sort_order)

        if self.start_date is not None and not isinstance(self.start_date, XSDDate):
            self.start_date = XSDDate(self.start_date)

        if self.end_date is not None and not isinstance(self.end_date, XSDDate):
            self.end_date = XSDDate(self.end_date)

        if not isinstance(self.observation_id_list, list):
            self.observation_id_list = [self.observation_id_list] if self.observation_id_list is not None else []
        self.observation_id_list = [
            v if isinstance(v, ObservationId) else ObservationId(v) for v in self.observation_id_list
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class StudyPopulation(StudyEntity):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["StudyPopulation"]
    class_class_curie: ClassVar[str] = "peh:StudyPopulation"
    class_name: ClassVar[str] = "StudyPopulation"
    class_model_uri: ClassVar[URIRef] = PEH.StudyPopulation

    id: Union[str, StudyPopulationId] = None
    research_population_type: Optional[Union[str, "ResearchPopulationType"]] = None
    member_id_list: Optional[Union[Union[str, StudyEntityId], List[Union[str, StudyEntityId]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, StudyPopulationId):
            self.id = StudyPopulationId(self.id)

        if self.research_population_type is not None and not isinstance(
            self.research_population_type, ResearchPopulationType
        ):
            self.research_population_type = ResearchPopulationType(self.research_population_type)

        if not isinstance(self.member_id_list, list):
            self.member_id_list = [self.member_id_list] if self.member_id_list is not None else []
        self.member_id_list = [v if isinstance(v, StudyEntityId) else StudyEntityId(v) for v in self.member_id_list]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class SampleCollection(StudyEntity):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["SampleCollection"]
    class_class_curie: ClassVar[str] = "peh:SampleCollection"
    class_name: ClassVar[str] = "SampleCollection"
    class_model_uri: ClassVar[URIRef] = PEH.SampleCollection

    id: Union[str, SampleCollectionId] = None
    matrix: Optional[Union[str, MatrixId]] = None
    constraints: Optional[Union[str, List[str]]] = empty_list()
    sample_id_list: Optional[Union[Union[str, SampleId], List[Union[str, SampleId]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, SampleCollectionId):
            self.id = SampleCollectionId(self.id)

        if self.matrix is not None and not isinstance(self.matrix, MatrixId):
            self.matrix = MatrixId(self.matrix)

        if not isinstance(self.constraints, list):
            self.constraints = [self.constraints] if self.constraints is not None else []
        self.constraints = [v if isinstance(v, str) else str(v) for v in self.constraints]

        if not isinstance(self.sample_id_list, list):
            self.sample_id_list = [self.sample_id_list] if self.sample_id_list is not None else []
        self.sample_id_list = [v if isinstance(v, SampleId) else SampleId(v) for v in self.sample_id_list]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class StudySubject(StudyEntity):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["StudySubject"]
    class_class_curie: ClassVar[str] = "peh:StudySubject"
    class_name: ClassVar[str] = "StudySubject"
    class_model_uri: ClassVar[URIRef] = PEH.StudySubject

    id: Union[str, StudySubjectId] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, StudySubjectId):
            self.id = StudySubjectId(self.id)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class StudySubjectGroup(StudyEntity):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["StudySubjectGroup"]
    class_class_curie: ClassVar[str] = "peh:StudySubjectGroup"
    class_name: ClassVar[str] = "StudySubjectGroup"
    class_model_uri: ClassVar[URIRef] = PEH.StudySubjectGroup

    id: Union[str, StudySubjectGroupId] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, StudySubjectGroupId):
            self.id = StudySubjectGroupId(self.id)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Observation(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["Observation"]
    class_class_curie: ClassVar[str] = "peh:Observation"
    class_name: ClassVar[str] = "Observation"
    class_model_uri: ClassVar[URIRef] = PEH.Observation

    id: Union[str, ObservationId] = None
    observation_type: Optional[Union[str, "ObservationType"]] = None
    observation_design: Optional[Union[dict, "ObservationDesign"]] = None
    observation_result_id_list: Optional[
        Union[Union[str, ObservationResultId], List[Union[str, ObservationResultId]]]
    ] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.observation_type is not None and not isinstance(self.observation_type, ObservationType):
            self.observation_type = ObservationType(self.observation_type)

        if self.observation_design is not None and not isinstance(self.observation_design, ObservationDesign):
            self.observation_design = ObservationDesign(**as_dict(self.observation_design))

        if not isinstance(self.observation_result_id_list, list):
            self.observation_result_id_list = (
                [self.observation_result_id_list] if self.observation_result_id_list is not None else []
            )
        self.observation_result_id_list = [
            v if isinstance(v, ObservationResultId) else ObservationResultId(v) for v in self.observation_result_id_list
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class MetadataObservation(Observation):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["MetadataObservation"]
    class_class_curie: ClassVar[str] = "peh:MetadataObservation"
    class_name: ClassVar[str] = "MetadataObservation"
    class_model_uri: ClassVar[URIRef] = PEH.MetadataObservation

    id: Union[str, MetadataObservationId] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, MetadataObservationId):
            self.id = MetadataObservationId(self.id)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class QuestionnaireObservation(Observation):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["QuestionnaireObservation"]
    class_class_curie: ClassVar[str] = "peh:QuestionnaireObservation"
    class_name: ClassVar[str] = "QuestionnaireObservation"
    class_model_uri: ClassVar[URIRef] = PEH.QuestionnaireObservation

    id: Union[str, QuestionnaireObservationId] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, QuestionnaireObservationId):
            self.id = QuestionnaireObservationId(self.id)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class SamplingObservation(Observation):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["SamplingObservation"]
    class_class_curie: ClassVar[str] = "peh:SamplingObservation"
    class_name: ClassVar[str] = "SamplingObservation"
    class_model_uri: ClassVar[URIRef] = PEH.SamplingObservation

    id: Union[str, SamplingObservationId] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, SamplingObservationId):
            self.id = SamplingObservationId(self.id)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class GeospatialObservation(Observation):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["GeospatialObservation"]
    class_class_curie: ClassVar[str] = "peh:GeospatialObservation"
    class_name: ClassVar[str] = "GeospatialObservation"
    class_model_uri: ClassVar[URIRef] = PEH.GeospatialObservation

    id: Union[str, GeospatialObservationId] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, GeospatialObservationId):
            self.id = GeospatialObservationId(self.id)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ObservationDesign(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ObservationDesign"]
    class_class_curie: ClassVar[str] = "peh:ObservationDesign"
    class_name: ClassVar[str] = "ObservationDesign"
    class_model_uri: ClassVar[URIRef] = PEH.ObservationDesign

    observable_entity_property_sets: Optional[
        Union[Union[dict, "ObservableEntityPropertySet"], List[Union[dict, "ObservableEntityPropertySet"]]]
    ] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if not isinstance(self.observable_entity_property_sets, list):
            self.observable_entity_property_sets = (
                [self.observable_entity_property_sets] if self.observable_entity_property_sets is not None else []
            )
        self.observable_entity_property_sets = [
            v if isinstance(v, ObservableEntityPropertySet) else ObservableEntityPropertySet(**as_dict(v))
            for v in self.observable_entity_property_sets
        ]

        super().__post_init__(**kwargs)


class MetadataDesign(ObservationDesign):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["MetadataDesign"]
    class_class_curie: ClassVar[str] = "peh:MetadataDesign"
    class_name: ClassVar[str] = "MetadataDesign"
    class_model_uri: ClassVar[URIRef] = PEH.MetadataDesign


class QuestionnaireDesign(ObservationDesign):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["QuestionnaireDesign"]
    class_class_curie: ClassVar[str] = "peh:QuestionnaireDesign"
    class_name: ClassVar[str] = "QuestionnaireDesign"
    class_model_uri: ClassVar[URIRef] = PEH.QuestionnaireDesign


class SamplingDesign(ObservationDesign):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["SamplingDesign"]
    class_class_curie: ClassVar[str] = "peh:SamplingDesign"
    class_name: ClassVar[str] = "SamplingDesign"
    class_model_uri: ClassVar[URIRef] = PEH.SamplingDesign


class GeospatialDesign(ObservationDesign):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["GeospatialDesign"]
    class_class_curie: ClassVar[str] = "peh:GeospatialDesign"
    class_name: ClassVar[str] = "GeospatialDesign"
    class_model_uri: ClassVar[URIRef] = PEH.GeospatialDesign


@dataclass(repr=False)
class ObservableEntityPropertySet(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ObservableEntityPropertySet"]
    class_class_curie: ClassVar[str] = "peh:ObservableEntityPropertySet"
    class_name: ClassVar[str] = "ObservableEntityPropertySet"
    class_model_uri: ClassVar[URIRef] = PEH.ObservableEntityPropertySet

    observation_result_type: Optional[Union[str, "ObservationResultType"]] = None
    observable_entity_type: Optional[Union[str, "ObservableEntityType"]] = None
    observable_entity_id_list: Optional[Union[Union[str, StudyEntityId], List[Union[str, StudyEntityId]]]] = (
        empty_list()
    )
    observable_property_id_list: Optional[
        Union[Union[str, ObservablePropertyId], List[Union[str, ObservablePropertyId]]]
    ] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.observation_result_type is not None and not isinstance(
            self.observation_result_type, ObservationResultType
        ):
            self.observation_result_type = ObservationResultType(self.observation_result_type)

        if self.observable_entity_type is not None and not isinstance(
            self.observable_entity_type, ObservableEntityType
        ):
            self.observable_entity_type = ObservableEntityType(self.observable_entity_type)

        if not isinstance(self.observable_entity_id_list, list):
            self.observable_entity_id_list = (
                [self.observable_entity_id_list] if self.observable_entity_id_list is not None else []
            )
        self.observable_entity_id_list = [
            v if isinstance(v, StudyEntityId) else StudyEntityId(v) for v in self.observable_entity_id_list
        ]

        if not isinstance(self.observable_property_id_list, list):
            self.observable_property_id_list = (
                [self.observable_property_id_list] if self.observable_property_id_list is not None else []
            )
        self.observable_property_id_list = [
            v if isinstance(v, ObservablePropertyId) else ObservablePropertyId(v)
            for v in self.observable_property_id_list
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ObservationResult(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ObservationResult"]
    class_class_curie: ClassVar[str] = "peh:ObservationResult"
    class_name: ClassVar[str] = "ObservationResult"
    class_model_uri: ClassVar[URIRef] = PEH.ObservationResult

    id: Union[str, ObservationResultId] = None
    observation_result_type: Optional[Union[str, "ObservationResultType"]] = None
    observation_start_date: Optional[Union[str, XSDDate]] = None
    observation_end_date: Optional[Union[str, XSDDate]] = None
    observed_values: Optional[Union[Union[dict, "ObservedValue"], List[Union[dict, "ObservedValue"]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.observation_result_type is not None and not isinstance(
            self.observation_result_type, ObservationResultType
        ):
            self.observation_result_type = ObservationResultType(self.observation_result_type)

        if self.observation_start_date is not None and not isinstance(self.observation_start_date, XSDDate):
            self.observation_start_date = XSDDate(self.observation_start_date)

        if self.observation_end_date is not None and not isinstance(self.observation_end_date, XSDDate):
            self.observation_end_date = XSDDate(self.observation_end_date)

        if not isinstance(self.observed_values, list):
            self.observed_values = [self.observed_values] if self.observed_values is not None else []
        self.observed_values = [
            v if isinstance(v, ObservedValue) else ObservedValue(**as_dict(v)) for v in self.observed_values
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class MetadataResult(ObservationResult):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["MetadataResult"]
    class_class_curie: ClassVar[str] = "peh:MetadataResult"
    class_name: ClassVar[str] = "MetadataResult"
    class_model_uri: ClassVar[URIRef] = PEH.MetadataResult

    id: Union[str, MetadataResultId] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, MetadataResultId):
            self.id = MetadataResultId(self.id)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class QuestionnaireResult(ObservationResult):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["QuestionnaireResult"]
    class_class_curie: ClassVar[str] = "peh:QuestionnaireResult"
    class_name: ClassVar[str] = "QuestionnaireResult"
    class_model_uri: ClassVar[URIRef] = PEH.QuestionnaireResult

    id: Union[str, QuestionnaireResultId] = None
    registration_start_date: Optional[Union[str, XSDDate]] = None
    registration_end_date: Optional[Union[str, XSDDate]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, QuestionnaireResultId):
            self.id = QuestionnaireResultId(self.id)

        if self.registration_start_date is not None and not isinstance(self.registration_start_date, XSDDate):
            self.registration_start_date = XSDDate(self.registration_start_date)

        if self.registration_end_date is not None and not isinstance(self.registration_end_date, XSDDate):
            self.registration_end_date = XSDDate(self.registration_end_date)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class SamplingResult(ObservationResult):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["SamplingResult"]
    class_class_curie: ClassVar[str] = "peh:SamplingResult"
    class_name: ClassVar[str] = "SamplingResult"
    class_model_uri: ClassVar[URIRef] = PEH.SamplingResult

    id: Union[str, SamplingResultId] = None
    collection_start_date: Optional[Union[str, XSDDate]] = None
    collection_end_date: Optional[Union[str, XSDDate]] = None
    analysis_start_date: Optional[Union[str, XSDDate]] = None
    analysis_end_date: Optional[Union[str, XSDDate]] = None
    analysis_lab: Optional[Union[str, StakeholderId]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, SamplingResultId):
            self.id = SamplingResultId(self.id)

        if self.collection_start_date is not None and not isinstance(self.collection_start_date, XSDDate):
            self.collection_start_date = XSDDate(self.collection_start_date)

        if self.collection_end_date is not None and not isinstance(self.collection_end_date, XSDDate):
            self.collection_end_date = XSDDate(self.collection_end_date)

        if self.analysis_start_date is not None and not isinstance(self.analysis_start_date, XSDDate):
            self.analysis_start_date = XSDDate(self.analysis_start_date)

        if self.analysis_end_date is not None and not isinstance(self.analysis_end_date, XSDDate):
            self.analysis_end_date = XSDDate(self.analysis_end_date)

        if self.analysis_lab is not None and not isinstance(self.analysis_lab, StakeholderId):
            self.analysis_lab = StakeholderId(self.analysis_lab)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class GeospatialResult(ObservationResult):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["GeospatialResult"]
    class_class_curie: ClassVar[str] = "peh:GeospatialResult"
    class_name: ClassVar[str] = "GeospatialResult"
    class_model_uri: ClassVar[URIRef] = PEH.GeospatialResult

    id: Union[str, GeospatialResultId] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, GeospatialResultId):
            self.id = GeospatialResultId(self.id)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ObservedValue(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ObservedValue"]
    class_class_curie: ClassVar[str] = "peh:ObservedValue"
    class_name: ClassVar[str] = "ObservedValue"
    class_model_uri: ClassVar[URIRef] = PEH.ObservedValue

    observable_entity: Optional[Union[str, StudyEntityId]] = None
    observable_property: Optional[Union[str, ObservablePropertyId]] = None
    default_unit: Optional[str] = None
    raw_value: Optional[str] = None
    raw_unit: Optional[str] = None
    imputed_value: Optional[str] = None
    imputed_unit: Optional[str] = None
    normalised_value: Optional[str] = None
    normalised_unit: Optional[str] = None
    value: Optional[str] = None
    unit: Optional[Union[str, UnitId]] = None
    value_as_string: Optional[str] = None
    quality_data: Optional[Union[Union[dict, "QualityData"], List[Union[dict, "QualityData"]]]] = empty_list()
    provenance_data: Optional[Union[Union[dict, "ProvenanceData"], List[Union[dict, "ProvenanceData"]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.observable_entity is not None and not isinstance(self.observable_entity, StudyEntityId):
            self.observable_entity = StudyEntityId(self.observable_entity)

        if self.observable_property is not None and not isinstance(self.observable_property, ObservablePropertyId):
            self.observable_property = ObservablePropertyId(self.observable_property)

        if self.default_unit is not None and not isinstance(self.default_unit, str):
            self.default_unit = str(self.default_unit)

        if self.raw_value is not None and not isinstance(self.raw_value, str):
            self.raw_value = str(self.raw_value)

        if self.raw_unit is not None and not isinstance(self.raw_unit, str):
            self.raw_unit = str(self.raw_unit)

        if self.imputed_value is not None and not isinstance(self.imputed_value, str):
            self.imputed_value = str(self.imputed_value)

        if self.imputed_unit is not None and not isinstance(self.imputed_unit, str):
            self.imputed_unit = str(self.imputed_unit)

        if self.normalised_value is not None and not isinstance(self.normalised_value, str):
            self.normalised_value = str(self.normalised_value)

        if self.normalised_unit is not None and not isinstance(self.normalised_unit, str):
            self.normalised_unit = str(self.normalised_unit)

        if self.value is not None and not isinstance(self.value, str):
            self.value = str(self.value)

        if self.unit is not None and not isinstance(self.unit, UnitId):
            self.unit = UnitId(self.unit)

        if self.value_as_string is not None and not isinstance(self.value_as_string, str):
            self.value_as_string = str(self.value_as_string)

        if not isinstance(self.quality_data, list):
            self.quality_data = [self.quality_data] if self.quality_data is not None else []
        self.quality_data = [v if isinstance(v, QualityData) else QualityData(**as_dict(v)) for v in self.quality_data]

        if not isinstance(self.provenance_data, list):
            self.provenance_data = [self.provenance_data] if self.provenance_data is not None else []
        self.provenance_data = [
            v if isinstance(v, ProvenanceData) else ProvenanceData(**as_dict(v)) for v in self.provenance_data
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class QualityData(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["QualityData"]
    class_class_curie: ClassVar[str] = "peh:QualityData"
    class_name: ClassVar[str] = "QualityData"
    class_model_uri: ClassVar[URIRef] = PEH.QualityData

    quality_context_key: Optional[str] = None
    quality_value: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.quality_context_key is not None and not isinstance(self.quality_context_key, str):
            self.quality_context_key = str(self.quality_context_key)

        if self.quality_value is not None and not isinstance(self.quality_value, str):
            self.quality_value = str(self.quality_value)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ProvenanceData(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ProvenanceData"]
    class_class_curie: ClassVar[str] = "peh:ProvenanceData"
    class_name: ClassVar[str] = "ProvenanceData"
    class_model_uri: ClassVar[URIRef] = PEH.ProvenanceData

    provenance_context_key: Optional[str] = None
    provenance_value: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.provenance_context_key is not None and not isinstance(self.provenance_context_key, str):
            self.provenance_context_key = str(self.provenance_context_key)

        if self.provenance_value is not None and not isinstance(self.provenance_value, str):
            self.provenance_value = str(self.provenance_value)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class DataLayout(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["DataLayout"]
    class_class_curie: ClassVar[str] = "peh:DataLayout"
    class_name: ClassVar[str] = "DataLayout"
    class_model_uri: ClassVar[URIRef] = PEH.DataLayout

    id: Union[str, DataLayoutId] = None
    sections: Optional[
        Union[
            Dict[Union[str, DataLayoutSectionId], Union[dict, "DataLayoutSection"]],
            List[Union[dict, "DataLayoutSection"]],
        ]
    ] = empty_dict()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, DataLayoutId):
            self.id = DataLayoutId(self.id)

        self._normalize_inlined_as_list(slot_name="sections", slot_type=DataLayoutSection, key_name="id", keyed=True)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class DataLayoutSection(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["DataLayoutSection"]
    class_class_curie: ClassVar[str] = "peh:DataLayoutSection"
    class_name: ClassVar[str] = "DataLayoutSection"
    class_model_uri: ClassVar[URIRef] = PEH.DataLayoutSection

    id: Union[str, DataLayoutSectionId] = None
    section_type: Optional[Union[str, "DataLayoutSectionType"]] = None
    observable_entity_types: Optional[
        Union[Union[str, "ObservableEntityType"], List[Union[str, "ObservableEntityType"]]]
    ] = empty_list()
    observable_entity_grouping_id_list: Optional[Union[Union[str, StudyEntityId], List[Union[str, StudyEntityId]]]] = (
        empty_list()
    )
    elements: Optional[Union[Union[dict, "DataLayoutElement"], List[Union[dict, "DataLayoutElement"]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, DataLayoutSectionId):
            self.id = DataLayoutSectionId(self.id)

        if self.section_type is not None and not isinstance(self.section_type, DataLayoutSectionType):
            self.section_type = DataLayoutSectionType(self.section_type)

        if not isinstance(self.observable_entity_types, list):
            self.observable_entity_types = (
                [self.observable_entity_types] if self.observable_entity_types is not None else []
            )
        self.observable_entity_types = [
            v if isinstance(v, ObservableEntityType) else ObservableEntityType(v) for v in self.observable_entity_types
        ]

        if not isinstance(self.observable_entity_grouping_id_list, list):
            self.observable_entity_grouping_id_list = (
                [self.observable_entity_grouping_id_list] if self.observable_entity_grouping_id_list is not None else []
            )
        self.observable_entity_grouping_id_list = [
            v if isinstance(v, StudyEntityId) else StudyEntityId(v) for v in self.observable_entity_grouping_id_list
        ]

        if not isinstance(self.elements, list):
            self.elements = [self.elements] if self.elements is not None else []
        self.elements = [
            v if isinstance(v, DataLayoutElement) else DataLayoutElement(**as_dict(v)) for v in self.elements
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class DataLayoutElement(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["DataLayoutElement"]
    class_class_curie: ClassVar[str] = "peh:DataLayoutElement"
    class_name: ClassVar[str] = "DataLayoutElement"
    class_model_uri: ClassVar[URIRef] = PEH.DataLayoutElement

    label: Optional[str] = None
    element_type: Optional[Union[str, "DataLayoutElementType"]] = None
    element_style: Optional[Union[str, "DataLayoutElementStyle"]] = None
    varname: Optional[str] = None
    observable_property: Optional[Union[str, ObservablePropertyId]] = None
    is_observable_entity_key: Optional[Union[bool, Bool]] = None
    is_foreign_key: Optional[Union[bool, Bool]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.label is not None and not isinstance(self.label, str):
            self.label = str(self.label)

        if self.element_type is not None and not isinstance(self.element_type, DataLayoutElementType):
            self.element_type = DataLayoutElementType(self.element_type)

        if self.element_style is not None and not isinstance(self.element_style, DataLayoutElementStyle):
            self.element_style = DataLayoutElementStyle(self.element_style)

        if self.varname is not None and not isinstance(self.varname, str):
            self.varname = str(self.varname)

        if self.observable_property is not None and not isinstance(self.observable_property, ObservablePropertyId):
            self.observable_property = ObservablePropertyId(self.observable_property)

        if self.is_observable_entity_key is not None and not isinstance(self.is_observable_entity_key, Bool):
            self.is_observable_entity_key = Bool(self.is_observable_entity_key)

        if self.is_foreign_key is not None and not isinstance(self.is_foreign_key, Bool):
            self.is_foreign_key = Bool(self.is_foreign_key)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class DataRequest(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["DataRequest"]
    class_class_curie: ClassVar[str] = "peh:DataRequest"
    class_name: ClassVar[str] = "DataRequest"
    class_model_uri: ClassVar[URIRef] = PEH.DataRequest

    id: Union[str, DataRequestId] = None
    contacts: Optional[Union[Union[dict, Contact], List[Union[dict, Contact]]]] = empty_list()
    request_properties: Optional[str] = None
    data_stakeholders: Optional[Union[Union[str, DataStakeholderId], List[Union[str, DataStakeholderId]]]] = (
        empty_list()
    )
    research_objectives: Optional[Union[Union[str, ResearchObjectiveId], List[Union[str, ResearchObjectiveId]]]] = (
        empty_list()
    )
    processing_actions: Optional[Union[Union[str, ProcessingActionId], List[Union[str, ProcessingActionId]]]] = (
        empty_list()
    )
    processing_steps: Optional[Union[Union[str, ProcessingStepId], List[Union[str, ProcessingStepId]]]] = empty_list()
    remark_on_content: Optional[str] = None
    remark_on_methodology: Optional[str] = None
    observed_entity_properties: Optional[
        Union[Union[dict, "ObservedEntityProperty"], List[Union[dict, "ObservedEntityProperty"]]]
    ] = empty_list()
    observable_entity_property_sets: Optional[
        Union[Union[dict, ObservableEntityPropertySet], List[Union[dict, ObservableEntityPropertySet]]]
    ] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, DataRequestId):
            self.id = DataRequestId(self.id)

        if not isinstance(self.contacts, list):
            self.contacts = [self.contacts] if self.contacts is not None else []
        self.contacts = [v if isinstance(v, Contact) else Contact(**as_dict(v)) for v in self.contacts]

        if self.request_properties is not None and not isinstance(self.request_properties, str):
            self.request_properties = str(self.request_properties)

        if not isinstance(self.data_stakeholders, list):
            self.data_stakeholders = [self.data_stakeholders] if self.data_stakeholders is not None else []
        self.data_stakeholders = [
            v if isinstance(v, DataStakeholderId) else DataStakeholderId(v) for v in self.data_stakeholders
        ]

        if not isinstance(self.research_objectives, list):
            self.research_objectives = [self.research_objectives] if self.research_objectives is not None else []
        self.research_objectives = [
            v if isinstance(v, ResearchObjectiveId) else ResearchObjectiveId(v) for v in self.research_objectives
        ]

        if not isinstance(self.processing_actions, list):
            self.processing_actions = [self.processing_actions] if self.processing_actions is not None else []
        self.processing_actions = [
            v if isinstance(v, ProcessingActionId) else ProcessingActionId(v) for v in self.processing_actions
        ]

        if not isinstance(self.processing_steps, list):
            self.processing_steps = [self.processing_steps] if self.processing_steps is not None else []
        self.processing_steps = [
            v if isinstance(v, ProcessingStepId) else ProcessingStepId(v) for v in self.processing_steps
        ]

        if self.remark_on_content is not None and not isinstance(self.remark_on_content, str):
            self.remark_on_content = str(self.remark_on_content)

        if self.remark_on_methodology is not None and not isinstance(self.remark_on_methodology, str):
            self.remark_on_methodology = str(self.remark_on_methodology)

        if not isinstance(self.observed_entity_properties, list):
            self.observed_entity_properties = (
                [self.observed_entity_properties] if self.observed_entity_properties is not None else []
            )
        self.observed_entity_properties = [
            v if isinstance(v, ObservedEntityProperty) else ObservedEntityProperty(**as_dict(v))
            for v in self.observed_entity_properties
        ]

        if not isinstance(self.observable_entity_property_sets, list):
            self.observable_entity_property_sets = (
                [self.observable_entity_property_sets] if self.observable_entity_property_sets is not None else []
            )
        self.observable_entity_property_sets = [
            v if isinstance(v, ObservableEntityPropertySet) else ObservableEntityPropertySet(**as_dict(v))
            for v in self.observable_entity_property_sets
        ]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ObservedEntityProperty(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ObservedEntityProperty"]
    class_class_curie: ClassVar[str] = "peh:ObservedEntityProperty"
    class_name: ClassVar[str] = "ObservedEntityProperty"
    class_model_uri: ClassVar[URIRef] = PEH.ObservedEntityProperty

    observable_entity: Optional[Union[str, StudyEntityId]] = None
    observable_property: Optional[Union[str, ObservablePropertyId]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self.observable_entity is not None and not isinstance(self.observable_entity, StudyEntityId):
            self.observable_entity = StudyEntityId(self.observable_entity)

        if self.observable_property is not None and not isinstance(self.observable_property, ObservablePropertyId):
            self.observable_property = ObservablePropertyId(self.observable_property)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class DataStakeholder(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["DataStakeholder"]
    class_class_curie: ClassVar[str] = "peh:DataStakeholder"
    class_name: ClassVar[str] = "DataStakeholder"
    class_model_uri: ClassVar[URIRef] = PEH.DataStakeholder

    id: Union[str, DataStakeholderId] = None
    stakeholder: Optional[Union[str, StakeholderId]] = None
    data_roles: Optional[Union[Union[str, "DataRole"], List[Union[str, "DataRole"]]]] = empty_list()
    contacts: Optional[Union[Union[dict, Contact], List[Union[dict, Contact]]]] = empty_list()
    processing_description: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, DataStakeholderId):
            self.id = DataStakeholderId(self.id)

        if self.stakeholder is not None and not isinstance(self.stakeholder, StakeholderId):
            self.stakeholder = StakeholderId(self.stakeholder)

        if not isinstance(self.data_roles, list):
            self.data_roles = [self.data_roles] if self.data_roles is not None else []
        self.data_roles = [v if isinstance(v, DataRole) else DataRole(v) for v in self.data_roles]

        if not isinstance(self.contacts, list):
            self.contacts = [self.contacts] if self.contacts is not None else []
        self.contacts = [v if isinstance(v, Contact) else Contact(**as_dict(v)) for v in self.contacts]

        if self.processing_description is not None and not isinstance(self.processing_description, str):
            self.processing_description = str(self.processing_description)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ResearchObjective(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ResearchObjective"]
    class_class_curie: ClassVar[str] = "peh:ResearchObjective"
    class_name: ClassVar[str] = "ResearchObjective"
    class_model_uri: ClassVar[URIRef] = PEH.ResearchObjective

    id: Union[str, ResearchObjectiveId] = None
    objective_type: Optional[Union[str, "ObjectiveType"]] = None
    authors: Optional[Union[str, List[str]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, ResearchObjectiveId):
            self.id = ResearchObjectiveId(self.id)

        if self.objective_type is not None and not isinstance(self.objective_type, ObjectiveType):
            self.objective_type = ObjectiveType(self.objective_type)

        if not isinstance(self.authors, list):
            self.authors = [self.authors] if self.authors is not None else []
        self.authors = [v if isinstance(v, str) else str(v) for v in self.authors]

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ProcessingAction(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ProcessingAction"]
    class_class_curie: ClassVar[str] = "peh:ProcessingAction"
    class_name: ClassVar[str] = "ProcessingAction"
    class_model_uri: ClassVar[URIRef] = PEH.ProcessingAction

    id: Union[str, ProcessingActionId] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, ProcessingActionId):
            self.id = ProcessingActionId(self.id)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class ProcessingStep(NamedThing):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["ProcessingStep"]
    class_class_curie: ClassVar[str] = "peh:ProcessingStep"
    class_name: ClassVar[str] = "ProcessingStep"
    class_model_uri: ClassVar[URIRef] = PEH.ProcessingStep

    id: Union[str, ProcessingStepId] = None
    start_date: Optional[Union[str, XSDDate]] = None
    delivery_date: Optional[Union[str, XSDDate]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, ProcessingStepId):
            self.id = ProcessingStepId(self.id)

        if self.start_date is not None and not isinstance(self.start_date, XSDDate):
            self.start_date = XSDDate(self.start_date)

        if self.delivery_date is not None and not isinstance(self.delivery_date, XSDDate):
            self.delivery_date = XSDDate(self.delivery_date)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class DataExtract(YAMLRoot):
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = PEH["DataExtract"]
    class_class_curie: ClassVar[str] = "peh:DataExtract"
    class_name: ClassVar[str] = "DataExtract"
    class_model_uri: ClassVar[URIRef] = PEH.DataExtract

    observed_values: Optional[Union[Union[dict, ObservedValue], List[Union[dict, ObservedValue]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if not isinstance(self.observed_values, list):
            self.observed_values = [self.observed_values] if self.observed_values is not None else []
        self.observed_values = [
            v if isinstance(v, ObservedValue) else ObservedValue(**as_dict(v)) for v in self.observed_values
        ]

        super().__post_init__(**kwargs)


# Enumerations
class ValidationStatus(EnumDefinitionImpl):
    unvalidated = PermissibleValue(text="unvalidated")
    in_progress = PermissibleValue(text="in_progress")
    validated = PermissibleValue(text="validated")

    _defn = EnumDefinition(
        name="ValidationStatus",
    )


class IndicatorType(EnumDefinitionImpl):
    effectmarker = PermissibleValue(text="effectmarker")
    exposuremarker = PermissibleValue(text="exposuremarker")
    geomarker = PermissibleValue(text="geomarker")
    observation = PermissibleValue(text="observation")

    _defn = EnumDefinition(
        name="IndicatorType",
    )


class BioChemEntityLinkType(EnumDefinitionImpl):
    exact_match = PermissibleValue(text="exact_match")
    close_match = PermissibleValue(text="close_match")
    broader = PermissibleValue(text="broader")
    part_of = PermissibleValue(text="part_of")
    group_contains = PermissibleValue(text="group_contains")
    has_parent_compound = PermissibleValue(text="has_parent_compound")
    branched_version_of = PermissibleValue(text="branched_version_of")

    _defn = EnumDefinition(
        name="BioChemEntityLinkType",
    )


class ResearchPopulationType(EnumDefinitionImpl):
    general_population = PermissibleValue(text="general_population")
    person = PermissibleValue(text="person")
    newborn = PermissibleValue(text="newborn")
    adolescent = PermissibleValue(text="adolescent")
    mother = PermissibleValue(text="mother")
    parent = PermissibleValue(text="parent")
    pregnant_person = PermissibleValue(text="pregnant_person")
    household = PermissibleValue(text="household")

    _defn = EnumDefinition(
        name="ResearchPopulationType",
    )


class ObservableEntityType(EnumDefinitionImpl):
    person = PermissibleValue(text="person")
    persongroup = PermissibleValue(text="persongroup")
    environment = PermissibleValue(text="environment")
    location = PermissibleValue(text="location")
    study = PermissibleValue(text="study")
    dataset = PermissibleValue(text="dataset")
    sample = PermissibleValue(text="sample")

    _defn = EnumDefinition(
        name="ObservableEntityType",
    )


class ObservationType(EnumDefinitionImpl):
    sampling = PermissibleValue(text="sampling")
    questionnaire = PermissibleValue(text="questionnaire")
    geospatial = PermissibleValue(text="geospatial")
    metadata = PermissibleValue(text="metadata")

    _defn = EnumDefinition(
        name="ObservationType",
    )


class ObservationResultType(EnumDefinitionImpl):
    measurement = PermissibleValue(text="measurement")
    control = PermissibleValue(text="control")
    calculation = PermissibleValue(text="calculation")
    simulation = PermissibleValue(text="simulation")

    _defn = EnumDefinition(
        name="ObservationResultType",
    )


class DataLayoutSectionType(EnumDefinitionImpl):
    data_form = PermissibleValue(text="data_form")
    data_table = PermissibleValue(text="data_table")
    property_table = PermissibleValue(text="property_table")

    _defn = EnumDefinition(
        name="DataLayoutSectionType",
    )


class DataLayoutElementType(EnumDefinitionImpl):
    text = PermissibleValue(text="text")
    spacer = PermissibleValue(text="spacer")
    data_field = PermissibleValue(text="data_field")

    _defn = EnumDefinition(
        name="DataLayoutElementType",
    )


class DataLayoutElementStyle(EnumDefinitionImpl):
    standard = PermissibleValue(text="standard")
    main_title = PermissibleValue(text="main_title")
    section_title = PermissibleValue(text="section_title")
    sub_title = PermissibleValue(text="sub_title")
    comment = PermissibleValue(text="comment")
    warning = PermissibleValue(text="warning")
    alert = PermissibleValue(text="alert")

    _defn = EnumDefinition(
        name="DataLayoutElementStyle",
    )


class ObjectiveType(EnumDefinitionImpl):
    research_objective = PermissibleValue(text="research_objective")
    project_result = PermissibleValue(text="project_result")
    publication = PermissibleValue(text="publication")

    _defn = EnumDefinition(
        name="ObjectiveType",
    )


class LinkType(EnumDefinitionImpl):
    is_about = PermissibleValue(text="is_about")
    is_same_as = PermissibleValue(text="is_same_as")
    is_part_of = PermissibleValue(text="is_part_of")
    is_located_at = PermissibleValue(text="is_located_at")

    _defn = EnumDefinition(
        name="LinkType",
    )


class ContactRole(EnumDefinitionImpl):
    administrative = PermissibleValue(text="administrative")
    data = PermissibleValue(text="data")
    general = PermissibleValue(text="general")
    lead = PermissibleValue(text="lead")
    legal = PermissibleValue(text="legal")
    technical = PermissibleValue(text="technical")

    _defn = EnumDefinition(
        name="ContactRole",
    )


class ProjectRole(EnumDefinitionImpl):
    member = PermissibleValue(text="member")
    partner = PermissibleValue(text="partner")
    funding_partner = PermissibleValue(text="funding_partner")
    principal_investigator = PermissibleValue(text="principal_investigator")
    data_governance = PermissibleValue(text="data_governance")
    data_controller = PermissibleValue(text="data_controller")
    data_processor = PermissibleValue(text="data_processor")
    data_user = PermissibleValue(text="data_user")
    lab = PermissibleValue(text="lab")

    _defn = EnumDefinition(
        name="ProjectRole",
    )


class StudyRole(EnumDefinitionImpl):
    funding_partner = PermissibleValue(text="funding_partner")
    principal_investigator = PermissibleValue(text="principal_investigator")
    data_controller = PermissibleValue(text="data_controller")
    data_processor = PermissibleValue(text="data_processor")
    data_user = PermissibleValue(text="data_user")
    lab = PermissibleValue(text="lab")

    _defn = EnumDefinition(
        name="StudyRole",
    )


class DataRole(EnumDefinitionImpl):
    main_stakeholder = PermissibleValue(text="main_stakeholder")
    supplying_data_controller = PermissibleValue(text="supplying_data_controller")
    receiving_data_controller = PermissibleValue(text="receiving_data_controller")
    external_data_controller = PermissibleValue(text="external_data_controller")

    _defn = EnumDefinition(
        name="DataRole",
    )


class QudtUnit(EnumDefinitionImpl):
    _defn = EnumDefinition(
        name="QudtUnit",
    )


class QudtQuantityKind(EnumDefinitionImpl):
    AmountOfSubstanceConcentration = PermissibleValue(
        text="AmountOfSubstanceConcentration", meaning=QUDTQK["AmountOfSubstanceConcentration"]
    )
    AmountOfSubstancePerUnitMass = PermissibleValue(
        text="AmountOfSubstancePerUnitMass", meaning=QUDTQK["AmountOfSubstancePerUnitMass"]
    )
    Dimensionless = PermissibleValue(text="Dimensionless", meaning=QUDTQK["Dimensionless"])
    DimensionlessRatio = PermissibleValue(text="DimensionlessRatio", meaning=QUDTQK["DimensionlessRatio"])
    Time = PermissibleValue(text="Time", meaning=QUDTQK["Time"])
    Frequency = PermissibleValue(text="Frequency", meaning=QUDTQK["Frequency"])
    Length = PermissibleValue(text="Length", meaning=QUDTQK["Length"])
    Mass = PermissibleValue(text="Mass", meaning=QUDTQK["Mass"])
    MassPerArea = PermissibleValue(text="MassPerArea", meaning=QUDTQK["MassPerArea"])
    MassConcentration = PermissibleValue(text="MassConcentration", meaning=QUDTQK["MassConcentration"])
    NumberDensity = PermissibleValue(text="NumberDensity", meaning=QUDTQK["NumberDensity"])
    SerumOrPlasmaLevel = PermissibleValue(text="SerumOrPlasmaLevel", meaning=QUDTQK["SerumOrPlasmaLevel"])
    Volume = PermissibleValue(text="Volume", meaning=QUDTQK["Volume"])

    _defn = EnumDefinition(
        name="QudtQuantityKind",
    )


# Slots
class slots:
    pass


slots.id = Slot(
    uri=SCHEMA.identifier, name="id", curie=SCHEMA.curie("identifier"), model_uri=PEH.id, domain=None, range=URIRef
)

slots.unique_name = Slot(
    uri=PEH.unique_name,
    name="unique_name",
    curie=PEH.curie("unique_name"),
    model_uri=PEH.unique_name,
    domain=None,
    range=Optional[str],
)

slots.name = Slot(
    uri=SCHEMA.name, name="name", curie=SCHEMA.curie("name"), model_uri=PEH.name, domain=None, range=Optional[str]
)

slots.description = Slot(
    uri=SCHEMA.description,
    name="description",
    curie=SCHEMA.curie("description"),
    model_uri=PEH.description,
    domain=None,
    range=Optional[str],
)

slots.label = Slot(
    uri=RDFS.label, name="label", curie=RDFS.curie("label"), model_uri=PEH.label, domain=None, range=Optional[str]
)

slots.remark = Slot(
    uri=PEH.remark, name="remark", curie=PEH.curie("remark"), model_uri=PEH.remark, domain=None, range=Optional[str]
)

slots.orcid = Slot(
    uri=PEH.orcid, name="orcid", curie=PEH.curie("orcid"), model_uri=PEH.orcid, domain=None, range=Optional[str]
)

slots.rorid = Slot(
    uri=PEH.rorid, name="rorid", curie=PEH.curie("rorid"), model_uri=PEH.rorid, domain=None, range=Optional[str]
)

slots.alias = Slot(
    uri=PEH.alias, name="alias", curie=PEH.curie("alias"), model_uri=PEH.alias, domain=None, range=Optional[str]
)

slots.aliases = Slot(
    uri=PEH.aliases,
    name="aliases",
    curie=PEH.curie("aliases"),
    model_uri=PEH.aliases,
    domain=None,
    range=Optional[Union[str, List[str]]],
)

slots.context_aliases = Slot(
    uri=PEH.context_aliases,
    name="context_aliases",
    curie=PEH.curie("context_aliases"),
    model_uri=PEH.context_aliases,
    domain=None,
    range=Optional[Union[Union[dict, ContextAlias], List[Union[dict, ContextAlias]]]],
)

slots.context = Slot(
    uri=PEH.context,
    name="context",
    curie=PEH.curie("context"),
    model_uri=PEH.context,
    domain=None,
    range=Optional[Union[str, NamedThingId]],
)

slots.translations = Slot(
    uri=PEH.translations,
    name="translations",
    curie=PEH.curie("translations"),
    model_uri=PEH.translations,
    domain=None,
    range=Optional[Union[Union[dict, Translation], List[Union[dict, Translation]]]],
)

slots.property_name = Slot(
    uri=PEH.property_name,
    name="property_name",
    curie=PEH.curie("property_name"),
    model_uri=PEH.property_name,
    domain=None,
    range=Optional[str],
)

slots.language = Slot(
    uri=PEH.language,
    name="language",
    curie=PEH.curie("language"),
    model_uri=PEH.language,
    domain=None,
    range=Optional[str],
)

slots.translated_value = Slot(
    uri=PEH.translated_value,
    name="translated_value",
    curie=PEH.curie("translated_value"),
    model_uri=PEH.translated_value,
    domain=None,
    range=Optional[str],
)

slots.validation_history = Slot(
    uri=PEH.validation_history,
    name="validation_history",
    curie=PEH.curie("validation_history"),
    model_uri=PEH.validation_history,
    domain=None,
    range=Optional[Union[Union[dict, ValidationHistoryRecord], List[Union[dict, ValidationHistoryRecord]]]],
)

slots.units = Slot(
    uri=PEH.units,
    name="units",
    curie=PEH.curie("units"),
    model_uri=PEH.units,
    domain=None,
    range=Optional[Union[Dict[Union[str, UnitId], Union[dict, Unit]], List[Union[dict, Unit]]]],
)

slots.same_unit_as = Slot(
    uri=PEH.same_unit_as,
    name="same_unit_as",
    curie=PEH.curie("same_unit_as"),
    model_uri=PEH.same_unit_as,
    domain=None,
    range=Optional[Union[str, "QudtUnit"]],
)

slots.quantity_kind = Slot(
    uri=PEH.quantity_kind,
    name="quantity_kind",
    curie=PEH.curie("quantity_kind"),
    model_uri=PEH.quantity_kind,
    domain=None,
    range=Optional[Union[str, "QudtQuantityKind"]],
)

slots.groupings = Slot(
    uri=PEH.groupings,
    name="groupings",
    curie=PEH.curie("groupings"),
    model_uri=PEH.groupings,
    domain=None,
    range=Optional[Union[Dict[Union[str, GroupingId], Union[dict, Grouping]], List[Union[dict, Grouping]]]],
)

slots.grouping_id_list = Slot(
    uri=PEH.grouping_id_list,
    name="grouping_id_list",
    curie=PEH.curie("grouping_id_list"),
    model_uri=PEH.grouping_id_list,
    domain=None,
    range=Optional[Union[Union[str, GroupingId], List[Union[str, GroupingId]]]],
)

slots.parent_grouping_id_list = Slot(
    uri=PEH.parent_grouping_id_list,
    name="parent_grouping_id_list",
    curie=PEH.curie("parent_grouping_id_list"),
    model_uri=PEH.parent_grouping_id_list,
    domain=None,
    range=Optional[Union[Union[str, GroupingId], List[Union[str, GroupingId]]]],
)

slots.biochemidentifiers = Slot(
    uri=PEH.biochemidentifiers,
    name="biochemidentifiers",
    curie=PEH.curie("biochemidentifiers"),
    model_uri=PEH.biochemidentifiers,
    domain=None,
    range=Optional[Union[Union[dict, BioChemIdentifier], List[Union[dict, BioChemIdentifier]]]],
)

slots.biochementities = Slot(
    uri=PEH.biochementities,
    name="biochementities",
    curie=PEH.curie("biochementities"),
    model_uri=PEH.biochementities,
    domain=None,
    range=Optional[
        Union[Dict[Union[str, BioChemEntityId], Union[dict, BioChemEntity]], List[Union[dict, BioChemEntity]]]
    ],
)

slots.indicators = Slot(
    uri=PEH.indicators,
    name="indicators",
    curie=PEH.curie("indicators"),
    model_uri=PEH.indicators,
    domain=None,
    range=Optional[Union[Dict[Union[str, IndicatorId], Union[dict, Indicator]], List[Union[dict, Indicator]]]],
)

slots.web_uri = Slot(
    uri=PEH.web_uri, name="web_uri", curie=PEH.curie("web_uri"), model_uri=PEH.web_uri, domain=None, range=Optional[str]
)

slots.identifier_schema = Slot(
    uri=PEH.identifier_schema,
    name="identifier_schema",
    curie=PEH.curie("identifier_schema"),
    model_uri=PEH.identifier_schema,
    domain=None,
    range=Optional[Union[str, BioChemIdentifierSchemaId]],
)

slots.identifier_code = Slot(
    uri=PEH.identifier_code,
    name="identifier_code",
    curie=PEH.curie("identifier_code"),
    model_uri=PEH.identifier_code,
    domain=None,
    range=Optional[str],
)

slots.current_validation_status = Slot(
    uri=PEH.current_validation_status,
    name="current_validation_status",
    curie=PEH.curie("current_validation_status"),
    model_uri=PEH.current_validation_status,
    domain=None,
    range=Optional[Union[str, "ValidationStatus"]],
)

slots.validation_datetime = Slot(
    uri=PEH.validation_datetime,
    name="validation_datetime",
    curie=PEH.curie("validation_datetime"),
    model_uri=PEH.validation_datetime,
    domain=None,
    range=Optional[Union[str, XSDDateTime]],
)

slots.validation_status = Slot(
    uri=PEH.validation_status,
    name="validation_status",
    curie=PEH.curie("validation_status"),
    model_uri=PEH.validation_status,
    domain=None,
    range=Optional[Union[str, "ValidationStatus"]],
)

slots.validation_actor = Slot(
    uri=PEH.validation_actor,
    name="validation_actor",
    curie=PEH.curie("validation_actor"),
    model_uri=PEH.validation_actor,
    domain=None,
    range=Optional[str],
)

slots.validation_institute = Slot(
    uri=PEH.validation_institute,
    name="validation_institute",
    curie=PEH.curie("validation_institute"),
    model_uri=PEH.validation_institute,
    domain=None,
    range=Optional[str],
)

slots.validation_remark = Slot(
    uri=PEH.validation_remark,
    name="validation_remark",
    curie=PEH.curie("validation_remark"),
    model_uri=PEH.validation_remark,
    domain=None,
    range=Optional[str],
)

slots.sort_order = Slot(
    uri=PEH.sort_order,
    name="sort_order",
    curie=PEH.curie("sort_order"),
    model_uri=PEH.sort_order,
    domain=None,
    range=Optional[Decimal],
)

slots.aggregation_target = Slot(
    uri=PEH.aggregation_target,
    name="aggregation_target",
    curie=PEH.curie("aggregation_target"),
    model_uri=PEH.aggregation_target,
    domain=None,
    range=Optional[Union[bool, Bool]],
)

slots.parent_matrix = Slot(
    uri=PEH.parent_matrix,
    name="parent_matrix",
    curie=PEH.curie("parent_matrix"),
    model_uri=PEH.parent_matrix,
    domain=None,
    range=Optional[Union[str, MatrixId]],
)

slots.secondary_parent_matrix_id_list = Slot(
    uri=PEH.secondary_parent_matrix_id_list,
    name="secondary_parent_matrix_id_list",
    curie=PEH.curie("secondary_parent_matrix_id_list"),
    model_uri=PEH.secondary_parent_matrix_id_list,
    domain=None,
    range=Optional[Union[Union[str, MatrixId], List[Union[str, MatrixId]]]],
)

slots.indicator_type = Slot(
    uri=PEH.indicator_type,
    name="indicator_type",
    curie=PEH.curie("indicator_type"),
    model_uri=PEH.indicator_type,
    domain=None,
    range=Optional[Union[str, "IndicatorType"]],
)

slots.varname = Slot(
    uri=PEH.varname, name="varname", curie=PEH.curie("varname"), model_uri=PEH.varname, domain=None, range=Optional[str]
)

slots.property = Slot(
    uri=PEH.property,
    name="property",
    curie=PEH.curie("property"),
    model_uri=PEH.property,
    domain=None,
    range=Optional[str],
)

slots.matrices = Slot(
    uri=PEH.matrices,
    name="matrices",
    curie=PEH.curie("matrices"),
    model_uri=PEH.matrices,
    domain=None,
    range=Optional[Union[Dict[Union[str, MatrixId], Union[dict, Matrix]], List[Union[dict, Matrix]]]],
)

slots.matrix = Slot(
    uri=PEH.matrix,
    name="matrix",
    curie=PEH.curie("matrix"),
    model_uri=PEH.matrix,
    domain=None,
    range=Optional[Union[str, MatrixId]],
)

slots.constraints = Slot(
    uri=PEH.constraints,
    name="constraints",
    curie=PEH.curie("constraints"),
    model_uri=PEH.constraints,
    domain=None,
    range=Optional[Union[str, List[str]]],
)

slots.default_observation_result_type = Slot(
    uri=PEH.default_observation_result_type,
    name="default_observation_result_type",
    curie=PEH.curie("default_observation_result_type"),
    model_uri=PEH.default_observation_result_type,
    domain=None,
    range=Optional[Union[str, "ObservationResultType"]],
)

slots.relevant_observable_entity_types = Slot(
    uri=PEH.relevant_observable_entity_types,
    name="relevant_observable_entity_types",
    curie=PEH.curie("relevant_observable_entity_types"),
    model_uri=PEH.relevant_observable_entity_types,
    domain=None,
    range=Optional[Union[Union[str, "ObservableEntityType"], List[Union[str, "ObservableEntityType"]]]],
)

slots.molweight_grampermol = Slot(
    uri=PEH.molweight_grampermol,
    name="molweight_grampermol",
    curie=PEH.curie("molweight_grampermol"),
    model_uri=PEH.molweight_grampermol,
    domain=None,
    range=Optional[Decimal],
)

slots.biochementity_links = Slot(
    uri=PEH.biochementity_links,
    name="biochementity_links",
    curie=PEH.curie("biochementity_links"),
    model_uri=PEH.biochementity_links,
    domain=None,
    range=Optional[Union[Union[dict, BioChemEntityLink], List[Union[dict, BioChemEntityLink]]]],
)

slots.biochementity_linktype = Slot(
    uri=PEH.biochementity_linktype,
    name="biochementity_linktype",
    curie=PEH.curie("biochementity_linktype"),
    model_uri=PEH.biochementity_linktype,
    domain=None,
    range=Optional[Union[str, "BioChemEntityLinkType"]],
)

slots.biochementity = Slot(
    uri=PEH.biochementity,
    name="biochementity",
    curie=PEH.curie("biochementity"),
    model_uri=PEH.biochementity,
    domain=None,
    range=Optional[Union[str, BioChemEntityId]],
)

slots.categorical = Slot(
    uri=PEH.categorical,
    name="categorical",
    curie=PEH.curie("categorical"),
    model_uri=PEH.categorical,
    domain=None,
    range=Optional[Union[bool, Bool]],
)

slots.multivalued = Slot(
    uri=PEH.multivalued,
    name="multivalued",
    curie=PEH.curie("multivalued"),
    model_uri=PEH.multivalued,
    domain=None,
    range=Optional[Union[bool, Bool]],
)

slots.abstract = Slot(
    uri=PEH.abstract,
    name="abstract",
    curie=PEH.curie("abstract"),
    model_uri=PEH.abstract,
    domain=None,
    range=Optional[Union[bool, Bool]],
)

slots.value_type = Slot(
    uri=PEH.value_type,
    name="value_type",
    curie=PEH.curie("value_type"),
    model_uri=PEH.value_type,
    domain=None,
    range=Optional[str],
)

slots.value_metadata = Slot(
    uri=PEH.value_metadata,
    name="value_metadata",
    curie=PEH.curie("value_metadata"),
    model_uri=PEH.value_metadata,
    domain=None,
    range=Optional[
        Union[Union[dict, ObservablePropertyMetadataElement], List[Union[dict, ObservablePropertyMetadataElement]]]
    ],
)

slots.value_options = Slot(
    uri=PEH.value_options,
    name="value_options",
    curie=PEH.curie("value_options"),
    model_uri=PEH.value_options,
    domain=None,
    range=Optional[Union[Union[dict, ObservablePropertyValueOption], List[Union[dict, ObservablePropertyValueOption]]]],
)

slots.default_required = Slot(
    uri=PEH.default_required,
    name="default_required",
    curie=PEH.curie("default_required"),
    model_uri=PEH.default_required,
    domain=None,
    range=Optional[Union[bool, Bool]],
)

slots.default_zeroallowed = Slot(
    uri=PEH.default_zeroallowed,
    name="default_zeroallowed",
    curie=PEH.curie("default_zeroallowed"),
    model_uri=PEH.default_zeroallowed,
    domain=None,
    range=Optional[Union[bool, Bool]],
)

slots.default_significantdecimals = Slot(
    uri=PEH.default_significantdecimals,
    name="default_significantdecimals",
    curie=PEH.curie("default_significantdecimals"),
    model_uri=PEH.default_significantdecimals,
    domain=None,
    range=Optional[int],
)

slots.default_unit = Slot(
    uri=PEH.default_unit,
    name="default_unit",
    curie=PEH.curie("default_unit"),
    model_uri=PEH.default_unit,
    domain=None,
    range=Optional[str],
)

slots.default_unit_label = Slot(
    uri=PEH.default_unit_label,
    name="default_unit_label",
    curie=PEH.curie("default_unit_label"),
    model_uri=PEH.default_unit_label,
    domain=None,
    range=Optional[str],
)

slots.default_immutable = Slot(
    uri=PEH.default_immutable,
    name="default_immutable",
    curie=PEH.curie("default_immutable"),
    model_uri=PEH.default_immutable,
    domain=None,
    range=Optional[Union[bool, Bool]],
)

slots.relevant_observation_types = Slot(
    uri=PEH.relevant_observation_types,
    name="relevant_observation_types",
    curie=PEH.curie("relevant_observation_types"),
    model_uri=PEH.relevant_observation_types,
    domain=None,
    range=Optional[Union[Union[str, "ObservationType"], List[Union[str, "ObservationType"]]]],
)

slots.indicator = Slot(
    uri=PEH.indicator,
    name="indicator",
    curie=PEH.curie("indicator"),
    model_uri=PEH.indicator,
    domain=None,
    range=Optional[Union[str, IndicatorId]],
)

slots.calculation_designs = Slot(
    uri=PEH.calculation_designs,
    name="calculation_designs",
    curie=PEH.curie("calculation_designs"),
    model_uri=PEH.calculation_designs,
    domain=None,
    range=Optional[Union[Union[dict, CalculationDesign], List[Union[dict, CalculationDesign]]]],
)

slots.calculation_name = Slot(
    uri=PEH.calculation_name,
    name="calculation_name",
    curie=PEH.curie("calculation_name"),
    model_uri=PEH.calculation_name,
    domain=None,
    range=Optional[str],
)

slots.calculation_implementation_as_json = Slot(
    uri=PEH.calculation_implementation_as_json,
    name="calculation_implementation_as_json",
    curie=PEH.curie("calculation_implementation_as_json"),
    model_uri=PEH.calculation_implementation_as_json,
    domain=None,
    range=Optional[str],
)

slots.calculation_implementation = Slot(
    uri=PEH.calculation_implementation,
    name="calculation_implementation",
    curie=PEH.curie("calculation_implementation"),
    model_uri=PEH.calculation_implementation,
    domain=None,
    range=Optional[Union[dict, CalculationImplementation]],
)

slots.function_name = Slot(
    uri=PEH.function_name,
    name="function_name",
    curie=PEH.curie("function_name"),
    model_uri=PEH.function_name,
    domain=None,
    range=Optional[str],
)

slots.function_args = Slot(
    uri=PEH.function_args,
    name="function_args",
    curie=PEH.curie("function_args"),
    model_uri=PEH.function_args,
    domain=None,
    range=Optional[Union[Union[dict, CalculationArgument], List[Union[dict, CalculationArgument]]]],
)

slots.function_kwargs = Slot(
    uri=PEH.function_kwargs,
    name="function_kwargs",
    curie=PEH.curie("function_kwargs"),
    model_uri=PEH.function_kwargs,
    domain=None,
    range=Optional[Union[Union[dict, CalculationKeywordArgument], List[Union[dict, CalculationKeywordArgument]]]],
)

slots.function_results = Slot(
    uri=PEH.function_results,
    name="function_results",
    curie=PEH.curie("function_results"),
    model_uri=PEH.function_results,
    domain=None,
    range=Optional[Union[Union[dict, CalculationResult], List[Union[dict, CalculationResult]]]],
)

slots.process_state = Slot(
    uri=PEH.process_state,
    name="process_state",
    curie=PEH.curie("process_state"),
    model_uri=PEH.process_state,
    domain=None,
    range=Optional[str],
)

slots.imputation_state = Slot(
    uri=PEH.imputation_state,
    name="imputation_state",
    curie=PEH.curie("imputation_state"),
    model_uri=PEH.imputation_state,
    domain=None,
    range=Optional[str],
)

slots.mapping_name = Slot(
    uri=PEH.mapping_name,
    name="mapping_name",
    curie=PEH.curie("mapping_name"),
    model_uri=PEH.mapping_name,
    domain=None,
    range=Optional[str],
)

slots.source_path = Slot(
    uri=PEH.source_path,
    name="source_path",
    curie=PEH.curie("source_path"),
    model_uri=PEH.source_path,
    domain=None,
    range=Optional[str],
)

slots.destination_path = Slot(
    uri=PEH.destination_path,
    name="destination_path",
    curie=PEH.curie("destination_path"),
    model_uri=PEH.destination_path,
    domain=None,
    range=Optional[str],
)

slots.round_decimals = Slot(
    uri=PEH.round_decimals,
    name="round_decimals",
    curie=PEH.curie("round_decimals"),
    model_uri=PEH.round_decimals,
    domain=None,
    range=Optional[int],
)

slots.scale_factor = Slot(
    uri=PEH.scale_factor,
    name="scale_factor",
    curie=PEH.curie("scale_factor"),
    model_uri=PEH.scale_factor,
    domain=None,
    range=Optional[Decimal],
)

slots.validation_designs = Slot(
    uri=PEH.validation_designs,
    name="validation_designs",
    curie=PEH.curie("validation_designs"),
    model_uri=PEH.validation_designs,
    domain=None,
    range=Optional[Union[Union[dict, ValidationDesign], List[Union[dict, ValidationDesign]]]],
)

slots.conditional = Slot(
    uri=PEH.conditional,
    name="conditional",
    curie=PEH.curie("conditional"),
    model_uri=PEH.conditional,
    domain=None,
    range=Optional[str],
)

slots.field = Slot(
    uri=PEH.field,
    name="field",
    curie=PEH.curie("field"),
    model_uri=PEH.field,
    domain=None,
    range=Optional[Union[str, ObservablePropertyMetadataFieldId]],
)

slots.key = Slot(uri=PEH.key, name="key", curie=PEH.curie("key"), model_uri=PEH.key, domain=None, range=Optional[str])

slots.value = Slot(
    uri=PEH.value, name="value", curie=PEH.curie("value"), model_uri=PEH.value, domain=None, range=Optional[str]
)

slots.metadata_fields = Slot(
    uri=PEH.metadata_fields,
    name="metadata_fields",
    curie=PEH.curie("metadata_fields"),
    model_uri=PEH.metadata_fields,
    domain=None,
    range=Optional[
        Union[
            Dict[Union[str, ObservablePropertyMetadataFieldId], Union[dict, ObservablePropertyMetadataField]],
            List[Union[dict, ObservablePropertyMetadataField]],
        ]
    ],
)

slots.stakeholders = Slot(
    uri=PEH.stakeholders,
    name="stakeholders",
    curie=PEH.curie("stakeholders"),
    model_uri=PEH.stakeholders,
    domain=None,
    range=Optional[Union[Dict[Union[str, StakeholderId], Union[dict, Stakeholder]], List[Union[dict, Stakeholder]]]],
)

slots.project_id_list = Slot(
    uri=PEH.project_id_list,
    name="project_id_list",
    curie=PEH.curie("project_id_list"),
    model_uri=PEH.project_id_list,
    domain=None,
    range=Optional[Union[Union[str, ProjectId], List[Union[str, ProjectId]]]],
)

slots.study_id_list = Slot(
    uri=PEH.study_id_list,
    name="study_id_list",
    curie=PEH.curie("study_id_list"),
    model_uri=PEH.study_id_list,
    domain=None,
    range=Optional[Union[Union[str, StudyId], List[Union[str, StudyId]]]],
)

slots.observation_group_id_list = Slot(
    uri=PEH.observation_group_id_list,
    name="observation_group_id_list",
    curie=PEH.curie("observation_group_id_list"),
    model_uri=PEH.observation_group_id_list,
    domain=None,
    range=Optional[Union[Union[str, ObservationGroupId], List[Union[str, ObservationGroupId]]]],
)

slots.observation_id_list = Slot(
    uri=PEH.observation_id_list,
    name="observation_id_list",
    curie=PEH.curie("observation_id_list"),
    model_uri=PEH.observation_id_list,
    domain=None,
    range=Optional[Union[Union[str, ObservationId], List[Union[str, ObservationId]]]],
)

slots.member_id_list = Slot(
    uri=PEH.member_id_list,
    name="member_id_list",
    curie=PEH.curie("member_id_list"),
    model_uri=PEH.member_id_list,
    domain=None,
    range=Optional[Union[Union[str, StudyEntityId], List[Union[str, StudyEntityId]]]],
)

slots.sample_id_list = Slot(
    uri=PEH.sample_id_list,
    name="sample_id_list",
    curie=PEH.curie("sample_id_list"),
    model_uri=PEH.sample_id_list,
    domain=None,
    range=Optional[Union[Union[str, SampleId], List[Union[str, SampleId]]]],
)

slots.projects = Slot(
    uri=PEH.projects,
    name="projects",
    curie=PEH.curie("projects"),
    model_uri=PEH.projects,
    domain=None,
    range=Optional[Union[Dict[Union[str, ProjectId], Union[dict, Project]], List[Union[dict, Project]]]],
)

slots.geographic_scope = Slot(
    uri=PEH.geographic_scope,
    name="geographic_scope",
    curie=PEH.curie("geographic_scope"),
    model_uri=PEH.geographic_scope,
    domain=None,
    range=Optional[str],
)

slots.project = Slot(
    uri=SCHEMA.ResearchProject,
    name="project",
    curie=SCHEMA.curie("ResearchProject"),
    model_uri=PEH.project,
    domain=None,
    range=Optional[str],
)

slots.default_language = Slot(
    uri=PEH.default_language,
    name="default_language",
    curie=PEH.curie("default_language"),
    model_uri=PEH.default_language,
    domain=None,
    range=Optional[str],
)

slots.stakeholder = Slot(
    uri=PEH.stakeholder,
    name="stakeholder",
    curie=PEH.curie("stakeholder"),
    model_uri=PEH.stakeholder,
    domain=None,
    range=Optional[Union[str, StakeholderId]],
)

slots.project_stakeholders = Slot(
    uri=PEH.project_stakeholders,
    name="project_stakeholders",
    curie=PEH.curie("project_stakeholders"),
    model_uri=PEH.project_stakeholders,
    domain=None,
    range=Optional[Union[Union[dict, ProjectStakeholder], List[Union[dict, ProjectStakeholder]]]],
)

slots.studies = Slot(
    uri=PEH.studies,
    name="studies",
    curie=PEH.curie("studies"),
    model_uri=PEH.studies,
    domain=None,
    range=Optional[Union[Dict[Union[str, StudyId], Union[dict, Study]], List[Union[dict, Study]]]],
)

slots.project_roles = Slot(
    uri=PEH.project_roles,
    name="project_roles",
    curie=PEH.curie("project_roles"),
    model_uri=PEH.project_roles,
    domain=None,
    range=Optional[Union[Union[str, "ProjectRole"], List[Union[str, "ProjectRole"]]]],
)

slots.study_stakeholders = Slot(
    uri=PEH.study_stakeholders,
    name="study_stakeholders",
    curie=PEH.curie("study_stakeholders"),
    model_uri=PEH.study_stakeholders,
    domain=None,
    range=Optional[Union[Union[dict, StudyStakeholder], List[Union[dict, StudyStakeholder]]]],
)

slots.research_population_type = Slot(
    uri=PEH.research_population_type,
    name="research_population_type",
    curie=PEH.curie("research_population_type"),
    model_uri=PEH.research_population_type,
    domain=None,
    range=Optional[Union[str, "ResearchPopulationType"]],
)

slots.study_roles = Slot(
    uri=PEH.study_roles,
    name="study_roles",
    curie=PEH.curie("study_roles"),
    model_uri=PEH.study_roles,
    domain=None,
    range=Optional[Union[Union[str, "StudyRole"], List[Union[str, "StudyRole"]]]],
)

slots.observation_groups = Slot(
    uri=PEH.observation_groups,
    name="observation_groups",
    curie=PEH.curie("observation_groups"),
    model_uri=PEH.observation_groups,
    domain=None,
    range=Optional[
        Union[Dict[Union[str, ObservationGroupId], Union[dict, ObservationGroup]], List[Union[dict, ObservationGroup]]]
    ],
)

slots.observations = Slot(
    uri=PEH.observations,
    name="observations",
    curie=PEH.curie("observations"),
    model_uri=PEH.observations,
    domain=None,
    range=Optional[Union[Dict[Union[str, ObservationId], Union[dict, Observation]], List[Union[dict, Observation]]]],
)

slots.linktype = Slot(
    uri=PEH.linktype,
    name="linktype",
    curie=PEH.curie("linktype"),
    model_uri=PEH.linktype,
    domain=None,
    range=Optional[Union[str, "LinkType"]],
)

slots.physical_entities = Slot(
    uri=PEH.physical_entities,
    name="physical_entities",
    curie=PEH.curie("physical_entities"),
    model_uri=PEH.physical_entities,
    domain=None,
    range=Optional[
        Union[Dict[Union[str, PhysicalEntityId], Union[dict, PhysicalEntity]], List[Union[dict, PhysicalEntity]]]
    ],
)

slots.physical_entity_links = Slot(
    uri=PEH.physical_entity_links,
    name="physical_entity_links",
    curie=PEH.curie("physical_entity_links"),
    model_uri=PEH.physical_entity_links,
    domain=None,
    range=Optional[Union[Union[dict, PhysicalEntityLink], List[Union[dict, PhysicalEntityLink]]]],
)

slots.physical_entity = Slot(
    uri=PEH.physical_entity,
    name="physical_entity",
    curie=PEH.curie("physical_entity"),
    model_uri=PEH.physical_entity,
    domain=None,
    range=Optional[Union[str, PhysicalEntityId]],
)

slots.study_entities = Slot(
    uri=PEH.study_entities,
    name="study_entities",
    curie=PEH.curie("study_entities"),
    model_uri=PEH.study_entities,
    domain=None,
    range=Optional[Union[Dict[Union[str, StudyEntityId], Union[dict, StudyEntity]], List[Union[dict, StudyEntity]]]],
)

slots.study_entity_id_list = Slot(
    uri=PEH.study_entity_id_list,
    name="study_entity_id_list",
    curie=PEH.curie("study_entity_id_list"),
    model_uri=PEH.study_entity_id_list,
    domain=None,
    range=Optional[Union[Union[str, StudyEntityId], List[Union[str, StudyEntityId]]]],
)

slots.study_entity_links = Slot(
    uri=PEH.study_entity_links,
    name="study_entity_links",
    curie=PEH.curie("study_entity_links"),
    model_uri=PEH.study_entity_links,
    domain=None,
    range=Optional[Union[Union[dict, StudyEntityLink], List[Union[dict, StudyEntityLink]]]],
)

slots.study_entity = Slot(
    uri=PEH.study_entity,
    name="study_entity",
    curie=PEH.curie("study_entity"),
    model_uri=PEH.study_entity,
    domain=None,
    range=Optional[Union[str, StudyEntityId]],
)

slots.recruited_in_project = Slot(
    uri=PEH.recruited_in_project,
    name="recruited_in_project",
    curie=PEH.curie("recruited_in_project"),
    model_uri=PEH.recruited_in_project,
    domain=None,
    range=Optional[Union[str, ProjectId]],
)

slots.sampled_in_project = Slot(
    uri=PEH.sampled_in_project,
    name="sampled_in_project",
    curie=PEH.curie("sampled_in_project"),
    model_uri=PEH.sampled_in_project,
    domain=None,
    range=Optional[Union[str, ProjectId]],
)

slots.physical_label = Slot(
    uri=PEH.physical_label,
    name="physical_label",
    curie=PEH.curie("physical_label"),
    model_uri=PEH.physical_label,
    domain=None,
    range=Optional[str],
)

slots.location = Slot(
    uri=PEH.location,
    name="location",
    curie=PEH.curie("location"),
    model_uri=PEH.location,
    domain=None,
    range=Optional[str],
)

slots.observation_type = Slot(
    uri=PEH.observation_type,
    name="observation_type",
    curie=PEH.curie("observation_type"),
    model_uri=PEH.observation_type,
    domain=None,
    range=Optional[Union[str, "ObservationType"]],
)

slots.observation_design = Slot(
    uri=PEH.observation_design,
    name="observation_design",
    curie=PEH.curie("observation_design"),
    model_uri=PEH.observation_design,
    domain=None,
    range=Optional[Union[dict, ObservationDesign]],
)

slots.observable_entity_property_sets = Slot(
    uri=PEH.observable_entity_property_sets,
    name="observable_entity_property_sets",
    curie=PEH.curie("observable_entity_property_sets"),
    model_uri=PEH.observable_entity_property_sets,
    domain=None,
    range=Optional[Union[Union[dict, ObservableEntityPropertySet], List[Union[dict, ObservableEntityPropertySet]]]],
)

slots.observation_result_type = Slot(
    uri=PEH.observation_result_type,
    name="observation_result_type",
    curie=PEH.curie("observation_result_type"),
    model_uri=PEH.observation_result_type,
    domain=None,
    range=Optional[Union[str, "ObservationResultType"]],
)

slots.observation_results = Slot(
    uri=PEH.observation_results,
    name="observation_results",
    curie=PEH.curie("observation_results"),
    model_uri=PEH.observation_results,
    domain=None,
    range=Optional[
        Union[
            Dict[Union[str, ObservationResultId], Union[dict, ObservationResult]], List[Union[dict, ObservationResult]]
        ]
    ],
)

slots.observation_result_id_list = Slot(
    uri=PEH.observation_result_id_list,
    name="observation_result_id_list",
    curie=PEH.curie("observation_result_id_list"),
    model_uri=PEH.observation_result_id_list,
    domain=None,
    range=Optional[Union[Union[str, ObservationResultId], List[Union[str, ObservationResultId]]]],
)

slots.observable_entity_type = Slot(
    uri=PEH.observable_entity_type,
    name="observable_entity_type",
    curie=PEH.curie("observable_entity_type"),
    model_uri=PEH.observable_entity_type,
    domain=None,
    range=Optional[Union[str, "ObservableEntityType"]],
)

slots.observable_entity_types = Slot(
    uri=PEH.observable_entity_types,
    name="observable_entity_types",
    curie=PEH.curie("observable_entity_types"),
    model_uri=PEH.observable_entity_types,
    domain=None,
    range=Optional[Union[Union[str, "ObservableEntityType"], List[Union[str, "ObservableEntityType"]]]],
)

slots.observable_entity_id_list = Slot(
    uri=PEH.observable_entity_id_list,
    name="observable_entity_id_list",
    curie=PEH.curie("observable_entity_id_list"),
    model_uri=PEH.observable_entity_id_list,
    domain=None,
    range=Optional[Union[Union[str, StudyEntityId], List[Union[str, StudyEntityId]]]],
)

slots.observable_entities = Slot(
    uri=PEH.observable_entities,
    name="observable_entities",
    curie=PEH.curie("observable_entities"),
    model_uri=PEH.observable_entities,
    domain=None,
    range=Optional[Union[Dict[Union[str, StudyEntityId], Union[dict, StudyEntity]], List[Union[dict, StudyEntity]]]],
)

slots.observable_entity = Slot(
    uri=PEH.observable_entity,
    name="observable_entity",
    curie=PEH.curie("observable_entity"),
    model_uri=PEH.observable_entity,
    domain=None,
    range=Optional[Union[str, StudyEntityId]],
)

slots.observable_property_id_list = Slot(
    uri=PEH.observable_property_id_list,
    name="observable_property_id_list",
    curie=PEH.curie("observable_property_id_list"),
    model_uri=PEH.observable_property_id_list,
    domain=None,
    range=Optional[Union[Union[str, ObservablePropertyId], List[Union[str, ObservablePropertyId]]]],
)

slots.observable_properties = Slot(
    uri=PEH.observable_properties,
    name="observable_properties",
    curie=PEH.curie("observable_properties"),
    model_uri=PEH.observable_properties,
    domain=None,
    range=Optional[
        Union[
            Dict[Union[str, ObservablePropertyId], Union[dict, ObservableProperty]],
            List[Union[dict, ObservableProperty]],
        ]
    ],
)

slots.observable_property = Slot(
    uri=PEH.observable_property,
    name="observable_property",
    curie=PEH.curie("observable_property"),
    model_uri=PEH.observable_property,
    domain=None,
    range=Optional[Union[str, ObservablePropertyId]],
)

slots.observed_values = Slot(
    uri=PEH.observed_values,
    name="observed_values",
    curie=PEH.curie("observed_values"),
    model_uri=PEH.observed_values,
    domain=None,
    range=Optional[Union[Union[dict, ObservedValue], List[Union[dict, ObservedValue]]]],
)

slots.unit = Slot(
    uri=PEH.unit,
    name="unit",
    curie=PEH.curie("unit"),
    model_uri=PEH.unit,
    domain=None,
    range=Optional[Union[str, UnitId]],
)

slots.raw_value = Slot(
    uri=PEH.raw_value,
    name="raw_value",
    curie=PEH.curie("raw_value"),
    model_uri=PEH.raw_value,
    domain=None,
    range=Optional[str],
)

slots.raw_unit = Slot(
    uri=PEH.raw_unit,
    name="raw_unit",
    curie=PEH.curie("raw_unit"),
    model_uri=PEH.raw_unit,
    domain=None,
    range=Optional[str],
)

slots.imputed_value = Slot(
    uri=PEH.imputed_value,
    name="imputed_value",
    curie=PEH.curie("imputed_value"),
    model_uri=PEH.imputed_value,
    domain=None,
    range=Optional[str],
)

slots.imputed_unit = Slot(
    uri=PEH.imputed_unit,
    name="imputed_unit",
    curie=PEH.curie("imputed_unit"),
    model_uri=PEH.imputed_unit,
    domain=None,
    range=Optional[str],
)

slots.normalised_value = Slot(
    uri=PEH.normalised_value,
    name="normalised_value",
    curie=PEH.curie("normalised_value"),
    model_uri=PEH.normalised_value,
    domain=None,
    range=Optional[str],
)

slots.normalised_unit = Slot(
    uri=PEH.normalised_unit,
    name="normalised_unit",
    curie=PEH.curie("normalised_unit"),
    model_uri=PEH.normalised_unit,
    domain=None,
    range=Optional[str],
)

slots.value_as_string = Slot(
    uri=PEH.value_as_string,
    name="value_as_string",
    curie=PEH.curie("value_as_string"),
    model_uri=PEH.value_as_string,
    domain=None,
    range=Optional[str],
)

slots.quality_data = Slot(
    uri=PEH.quality_data,
    name="quality_data",
    curie=PEH.curie("quality_data"),
    model_uri=PEH.quality_data,
    domain=None,
    range=Optional[Union[Union[dict, QualityData], List[Union[dict, QualityData]]]],
)

slots.quality_context_key = Slot(
    uri=PEH.quality_context_key,
    name="quality_context_key",
    curie=PEH.curie("quality_context_key"),
    model_uri=PEH.quality_context_key,
    domain=None,
    range=Optional[str],
)

slots.quality_value = Slot(
    uri=PEH.quality_value,
    name="quality_value",
    curie=PEH.curie("quality_value"),
    model_uri=PEH.quality_value,
    domain=None,
    range=Optional[str],
)

slots.provenance_data = Slot(
    uri=PEH.provenance_data,
    name="provenance_data",
    curie=PEH.curie("provenance_data"),
    model_uri=PEH.provenance_data,
    domain=None,
    range=Optional[Union[Union[dict, ProvenanceData], List[Union[dict, ProvenanceData]]]],
)

slots.provenance_context_key = Slot(
    uri=PEH.provenance_context_key,
    name="provenance_context_key",
    curie=PEH.curie("provenance_context_key"),
    model_uri=PEH.provenance_context_key,
    domain=None,
    range=Optional[str],
)

slots.provenance_value = Slot(
    uri=PEH.provenance_value,
    name="provenance_value",
    curie=PEH.curie("provenance_value"),
    model_uri=PEH.provenance_value,
    domain=None,
    range=Optional[str],
)

slots.data_requests = Slot(
    uri=PEH.data_requests,
    name="data_requests",
    curie=PEH.curie("data_requests"),
    model_uri=PEH.data_requests,
    domain=None,
    range=Optional[Union[Dict[Union[str, DataRequestId], Union[dict, DataRequest]], List[Union[dict, DataRequest]]]],
)

slots.layouts = Slot(
    uri=PEH.layouts,
    name="layouts",
    curie=PEH.curie("layouts"),
    model_uri=PEH.layouts,
    domain=None,
    range=Optional[Union[Dict[Union[str, DataLayoutId], Union[dict, DataLayout]], List[Union[dict, DataLayout]]]],
)

slots.sections = Slot(
    uri=PEH.sections,
    name="sections",
    curie=PEH.curie("sections"),
    model_uri=PEH.sections,
    domain=None,
    range=Optional[
        Union[
            Dict[Union[str, DataLayoutSectionId], Union[dict, DataLayoutSection]], List[Union[dict, DataLayoutSection]]
        ]
    ],
)

slots.section_type = Slot(
    uri=PEH.section_type,
    name="section_type",
    curie=PEH.curie("section_type"),
    model_uri=PEH.section_type,
    domain=None,
    range=Optional[Union[str, "DataLayoutSectionType"]],
)

slots.observable_entity_grouping_id_list = Slot(
    uri=PEH.observable_entity_grouping_id_list,
    name="observable_entity_grouping_id_list",
    curie=PEH.curie("observable_entity_grouping_id_list"),
    model_uri=PEH.observable_entity_grouping_id_list,
    domain=None,
    range=Optional[Union[Union[str, StudyEntityId], List[Union[str, StudyEntityId]]]],
)

slots.elements = Slot(
    uri=PEH.elements,
    name="elements",
    curie=PEH.curie("elements"),
    model_uri=PEH.elements,
    domain=None,
    range=Optional[Union[Union[dict, DataLayoutElement], List[Union[dict, DataLayoutElement]]]],
)

slots.element_type = Slot(
    uri=PEH.element_type,
    name="element_type",
    curie=PEH.curie("element_type"),
    model_uri=PEH.element_type,
    domain=None,
    range=Optional[Union[str, "DataLayoutElementType"]],
)

slots.element_style = Slot(
    uri=PEH.element_style,
    name="element_style",
    curie=PEH.curie("element_style"),
    model_uri=PEH.element_style,
    domain=None,
    range=Optional[Union[str, "DataLayoutElementStyle"]],
)

slots.is_observable_entity_key = Slot(
    uri=PEH.is_observable_entity_key,
    name="is_observable_entity_key",
    curie=PEH.curie("is_observable_entity_key"),
    model_uri=PEH.is_observable_entity_key,
    domain=None,
    range=Optional[Union[bool, Bool]],
)

slots.is_foreign_key = Slot(
    uri=PEH.is_foreign_key,
    name="is_foreign_key",
    curie=PEH.curie("is_foreign_key"),
    model_uri=PEH.is_foreign_key,
    domain=None,
    range=Optional[Union[bool, Bool]],
)

slots.data_roles = Slot(
    uri=PEH.data_roles,
    name="data_roles",
    curie=PEH.curie("data_roles"),
    model_uri=PEH.data_roles,
    domain=None,
    range=Optional[Union[Union[str, "DataRole"], List[Union[str, "DataRole"]]]],
)

slots.contacts = Slot(
    uri=PEH.contacts,
    name="contacts",
    curie=PEH.curie("contacts"),
    model_uri=PEH.contacts,
    domain=None,
    range=Optional[Union[Union[dict, Contact], List[Union[dict, Contact]]]],
)

slots.contact_roles = Slot(
    uri=PEH.contact_roles,
    name="contact_roles",
    curie=PEH.curie("contact_roles"),
    model_uri=PEH.contact_roles,
    domain=None,
    range=Optional[Union[Union[str, "ContactRole"], List[Union[str, "ContactRole"]]]],
)

slots.contact_email = Slot(
    uri=PEH.contact_email,
    name="contact_email",
    curie=PEH.curie("contact_email"),
    model_uri=PEH.contact_email,
    domain=None,
    range=Optional[str],
)

slots.contact_phone = Slot(
    uri=PEH.contact_phone,
    name="contact_phone",
    curie=PEH.curie("contact_phone"),
    model_uri=PEH.contact_phone,
    domain=None,
    range=Optional[str],
)

slots.request_properties = Slot(
    uri=PEH.request_properties,
    name="request_properties",
    curie=PEH.curie("request_properties"),
    model_uri=PEH.request_properties,
    domain=None,
    range=Optional[str],
)

slots.data_stakeholders = Slot(
    uri=PEH.data_stakeholders,
    name="data_stakeholders",
    curie=PEH.curie("data_stakeholders"),
    model_uri=PEH.data_stakeholders,
    domain=None,
    range=Optional[Union[Union[str, DataStakeholderId], List[Union[str, DataStakeholderId]]]],
)

slots.research_objectives = Slot(
    uri=PEH.research_objectives,
    name="research_objectives",
    curie=PEH.curie("research_objectives"),
    model_uri=PEH.research_objectives,
    domain=None,
    range=Optional[Union[Union[str, ResearchObjectiveId], List[Union[str, ResearchObjectiveId]]]],
)

slots.processing_actions = Slot(
    uri=PEH.processing_actions,
    name="processing_actions",
    curie=PEH.curie("processing_actions"),
    model_uri=PEH.processing_actions,
    domain=None,
    range=Optional[Union[Union[str, ProcessingActionId], List[Union[str, ProcessingActionId]]]],
)

slots.processing_steps = Slot(
    uri=PEH.processing_steps,
    name="processing_steps",
    curie=PEH.curie("processing_steps"),
    model_uri=PEH.processing_steps,
    domain=None,
    range=Optional[Union[Union[str, ProcessingStepId], List[Union[str, ProcessingStepId]]]],
)

slots.remark_on_content = Slot(
    uri=PEH.remark_on_content,
    name="remark_on_content",
    curie=PEH.curie("remark_on_content"),
    model_uri=PEH.remark_on_content,
    domain=None,
    range=Optional[str],
)

slots.remark_on_methodology = Slot(
    uri=PEH.remark_on_methodology,
    name="remark_on_methodology",
    curie=PEH.curie("remark_on_methodology"),
    model_uri=PEH.remark_on_methodology,
    domain=None,
    range=Optional[str],
)

slots.observed_entity_properties = Slot(
    uri=PEH.observed_entity_properties,
    name="observed_entity_properties",
    curie=PEH.curie("observed_entity_properties"),
    model_uri=PEH.observed_entity_properties,
    domain=None,
    range=Optional[Union[Union[dict, ObservedEntityProperty], List[Union[dict, ObservedEntityProperty]]]],
)

slots.processing_description = Slot(
    uri=PEH.processing_description,
    name="processing_description",
    curie=PEH.curie("processing_description"),
    model_uri=PEH.processing_description,
    domain=None,
    range=Optional[str],
)

slots.objective_type = Slot(
    uri=PEH.objective_type,
    name="objective_type",
    curie=PEH.curie("objective_type"),
    model_uri=PEH.objective_type,
    domain=None,
    range=Optional[Union[str, "ObjectiveType"]],
)

slots.authors = Slot(
    uri=PEH.authors,
    name="authors",
    curie=PEH.curie("authors"),
    model_uri=PEH.authors,
    domain=None,
    range=Optional[Union[str, List[str]]],
)

slots.start_date = Slot(
    uri=PEH.start_date,
    name="start_date",
    curie=PEH.curie("start_date"),
    model_uri=PEH.start_date,
    domain=None,
    range=Optional[Union[str, XSDDate]],
)

slots.end_date = Slot(
    uri=PEH.end_date,
    name="end_date",
    curie=PEH.curie("end_date"),
    model_uri=PEH.end_date,
    domain=None,
    range=Optional[Union[str, XSDDate]],
)

slots.delivery_date = Slot(
    uri=PEH.delivery_date,
    name="delivery_date",
    curie=PEH.curie("delivery_date"),
    model_uri=PEH.delivery_date,
    domain=None,
    range=Optional[Union[str, XSDDate]],
)

slots.observation_start_date = Slot(
    uri=PEH.observation_start_date,
    name="observation_start_date",
    curie=PEH.curie("observation_start_date"),
    model_uri=PEH.observation_start_date,
    domain=None,
    range=Optional[Union[str, XSDDate]],
)

slots.observation_end_date = Slot(
    uri=PEH.observation_end_date,
    name="observation_end_date",
    curie=PEH.curie("observation_end_date"),
    model_uri=PEH.observation_end_date,
    domain=None,
    range=Optional[Union[str, XSDDate]],
)

slots.registration_start_date = Slot(
    uri=PEH.registration_start_date,
    name="registration_start_date",
    curie=PEH.curie("registration_start_date"),
    model_uri=PEH.registration_start_date,
    domain=None,
    range=Optional[Union[str, XSDDate]],
)

slots.registration_end_date = Slot(
    uri=PEH.registration_end_date,
    name="registration_end_date",
    curie=PEH.curie("registration_end_date"),
    model_uri=PEH.registration_end_date,
    domain=None,
    range=Optional[Union[str, XSDDate]],
)

slots.collection_date = Slot(
    uri=PEH.collection_date,
    name="collection_date",
    curie=PEH.curie("collection_date"),
    model_uri=PEH.collection_date,
    domain=None,
    range=Optional[Union[str, XSDDate]],
)

slots.collection_start_date = Slot(
    uri=PEH.collection_start_date,
    name="collection_start_date",
    curie=PEH.curie("collection_start_date"),
    model_uri=PEH.collection_start_date,
    domain=None,
    range=Optional[Union[str, XSDDate]],
)

slots.collection_end_date = Slot(
    uri=PEH.collection_end_date,
    name="collection_end_date",
    curie=PEH.curie("collection_end_date"),
    model_uri=PEH.collection_end_date,
    domain=None,
    range=Optional[Union[str, XSDDate]],
)

slots.analysis_start_date = Slot(
    uri=PEH.analysis_start_date,
    name="analysis_start_date",
    curie=PEH.curie("analysis_start_date"),
    model_uri=PEH.analysis_start_date,
    domain=None,
    range=Optional[Union[str, XSDDate]],
)

slots.analysis_end_date = Slot(
    uri=PEH.analysis_end_date,
    name="analysis_end_date",
    curie=PEH.curie("analysis_end_date"),
    model_uri=PEH.analysis_end_date,
    domain=None,
    range=Optional[Union[str, XSDDate]],
)

slots.analysis_lab = Slot(
    uri=PEH.analysis_lab,
    name="analysis_lab",
    curie=PEH.curie("analysis_lab"),
    model_uri=PEH.analysis_lab,
    domain=None,
    range=Optional[Union[str, StakeholderId]],
)
