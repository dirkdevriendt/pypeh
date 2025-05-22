import logging

from peh_model import peh
from typing import Type, Callable


logger = logging.getLogger(__name__)


# Create a dictionary with the required pattern
CLASS_REFERENCES = {
    # NamedThingId
    "NamedThingId": peh.NamedThing,
    "GroupingId": peh.Grouping,
    "UnitId": peh.Unit,
    "BioChemEntityId": peh.BioChemEntity,
    "BioChemIdentifierSchemaId": peh.BioChemIdentifierSchema,
    "MatrixId": peh.Matrix,
    "IndicatorId": peh.Indicator,
    "ObservablePropertyId": peh.ObservableProperty,
    "ObservablePropertyMetadataFieldId": peh.ObservablePropertyMetadataField,
    "StakeholderId": peh.Stakeholder,
    "StudyEntityId": peh.StudyEntity,
    "ObservationId": peh.Observation,
    "ObservationResultId": peh.ObservationResult,
    "DataLayoutId": peh.DataLayout,
    "DataLayoutSectionId": peh.DataLayoutSection,
    "DataRequestId": peh.DataRequest,
    "DataStakeholderId": peh.DataStakeholder,
    "ResearchObjectiveId": peh.ResearchObjective,
    "ProcessingActionId": peh.ProcessingAction,
    "ProcessingStepId": peh.ProcessingStep,
    # PhysicalEntityId
    "PhysicalEntityId": peh.PhysicalEntity,
    "SampleId": peh.Sample,
    "PersonId": peh.Person,
    "GeolocationId": peh.Geolocation,
    "EnvironmentId": peh.Environment,
    # EnvironmentId
    "HomeEnvironmentId": peh.HomeEnvironment,
    "WorkEnvironmentId": peh.WorkEnvironment,
    # StudyEntityId
    "ProjectId": peh.Project,
    "StudyId": peh.Study,
    "ObservationGroupId": peh.ObservationGroup,
    "StudyPopulationId": peh.StudyPopulation,
    "SampleCollectionId": peh.SampleCollection,
    "StudySubjectId": peh.StudySubject,
    "StudySubjectGroupId": peh.StudySubjectGroup,
}


class TypedLazyProxy:
    def __init__(self, identifier: str, expected_type: Type[peh.NamedThing], loader: Callable):
        self._id: str = identifier
        self._expected_type = expected_type
        self._loader = loader
        self._target = None

    @property
    def id(self):
        return self._id

    @property
    def expected_type(self):
        return self._expected_type

    def set_loader(self, loader: Callable) -> bool:
        self._loader = loader
        return True

    def _ensure_loaded(self):
        if self._target is None:
            self._target = self._loader()
            if self._expected_type and not isinstance(self._target, self._expected_type):
                raise TypeError(f"Loaded object is not of expected type {self._expected_type}")

    def __getattr__(self, name):
        self._ensure_loaded()
        return getattr(self._target, name)

    def __repr__(self):
        if self._target is None:
            return f"TypedLazyProxy: type: {self._expected_type}, id: {self._id}"
        else:
            return repr(self._target)

    # Add more special methods as needed
    def __eq__(self, other):
        if isinstance(other, TypedLazyProxy):
            return self.id == other.id
        elif isinstance(other, peh.NamedThing):
            return self.id == other.id
