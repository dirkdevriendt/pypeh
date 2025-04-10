from __future__ import annotations

import logging

from typing import TYPE_CHECKING


logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Callable


class FieldMapping:
    """Handles mapping between source and target data models."""

    def __init__(
        self,
        source_to_target: Optional[Dict[str, str]] = None,
        target_to_source: Optional[Dict[str, str]] = None,
        default_values: Optional[Dict[str, Any]] = None,
        transformers: Optional[Dict[str, Callable]] = None,
        include_unmapped_fields: bool = True,
    ):
        """
        Configure field mapping between source and target schemas.

        Args:
            source_to_target: Maps source field names to target field names
            target_to_source: Reverse mapping (for writing back to source)
            default_values: Default values for target fields not in source
            transformers: Functions to transform values (source value -> target value)
            include_unmapped_fields: Whether to include fields not in mapping
        """
        self.source_to_target = source_to_target or {}
        self.target_to_source = target_to_source or {}
        self.default_values = default_values or {}
        self.transformers = transformers or {}
        self.include_unmapped_fields = include_unmapped_fields

        # Auto-populate reverse mapping if not provided
        if not self.target_to_source and self.source_to_target:
            self.target_to_source = {v: k for k, v in self.source_to_target.items()}

    def transform_to_target(self, source_data: Dict[str, Any]) -> Dict[str, Any]:
        return self._transform(source_data, self.source_to_target, self.transformers, self.include_unmapped_fields)

    def transform_to_source(self, target_data: Dict[str, Any]) -> Dict[str, Any]:
        return self._transform(
            target_data,
            self.target_to_source,
            {},  # No transformers for reverse direction yet
            self.include_unmapped_fields,
        )

    def _transform(
        self, data: Dict[str, Any], mapping: Dict[str, str], transformers: Dict[str, Callable], include_unmapped: bool
    ) -> Dict[str, Any]:
        result = {}

        # Apply explicit mappings with transformations
        for source_field, target_field in mapping.items():
            if source_field in data:
                value = data[source_field]
                # Apply transformer if specified
                if source_field in transformers:
                    value = transformers[source_field](value)
                result[target_field] = value

        # Include unmapped fields from source if specified
        if include_unmapped:
            for field, value in data.items():
                if field not in mapping:
                    result[field] = value

        # Add default values for missing target fields
        for field, default_value in self.default_values.items():
            if field not in result:
                result[field] = default_value

        return result
