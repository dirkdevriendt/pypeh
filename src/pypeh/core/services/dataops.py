from __future__ import annotations

import logging

from typing import TYPE_CHECKING, Sequence
from peh_model import peh

from pypeh.core.interfaces.inbound.dataops import InDataOpsInterface
from pypeh.core.interfaces.outbound.dataops import OutDataOpsInterface, ValidationInterface
from pypeh.core.models.validation_dto import ValidationConfig
from pypeh.core.cache.containers import CacheContainer, CacheContainerFactory


if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class DataOpsService:
    def __init__(
        self,
        inbound_adapter: InDataOpsInterface,
        outbound_adapter: OutDataOpsInterface,
        cache: CacheContainer = CacheContainerFactory.new(),
    ):
        self.inbound_adapter = inbound_adapter
        self.outbound_adapter = outbound_adapter
        self.cache = cache


class ValidationService(DataOpsService):
    def __init__(
        self,
        inbound_adapter: InDataOpsInterface,
        outbound_adapter: ValidationInterface,
        cache: CacheContainer = CacheContainerFactory.new(),
    ):
        super().__init__(inbound_adapter, outbound_adapter, cache)
        self.outbound_adapter: ValidationInterface = outbound_adapter

    def validate_data(
        self,
        data: dict[str, Sequence],
        observation: peh.Observation,
        observable_property_dict: dict[str, peh.ObservableProperty],
    ) -> dict:
        result_dict = {}
        observation_design = observation.observation_design
        observable_entity_property_sets = getattr(observation_design, "observable_entity_property_sets", None)
        if observable_entity_property_sets is None:
            raise AttributeError
        for cnt, oep_set in enumerate(observable_entity_property_sets):
            oep_set_name = (
                f"{oep_set}_{cnt:0>2}"  # TODO: document why an observable_entity_property_set gets a label like this
            )
            validation_config = ValidationConfig.from_peh(
                oep_set,
                oep_set_name,
                observable_property_dict,
            )
            result_dict[oep_set_name] = self.outbound_adapter.validate(data, validation_config)

        return result_dict
