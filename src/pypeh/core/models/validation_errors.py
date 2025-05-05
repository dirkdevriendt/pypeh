from typing import Any, Dict, List, Literal, Optional, Tuple
from pydantic import BaseModel, Field

from pypeh.core.models.constants import ValidationErrorLevel


class ValidationErrorLocation(BaseModel):
    """Base class for specifying where an error occurred"""

    location_type: str


class DataFrameLocation(ValidationErrorLocation):
    """Location information for DataFrame-based validation errors"""

    location_type: Literal["dataframe"] = "dataframe"
    key_columns: List[str]  # List of column names that jointly identifies a dataframe entry.
    column_name: Optional[str] = None
    row_ids: List[Tuple[str]] = []


class ValidationErrorContext(BaseModel):  # TODO: adapt, what type of context are we expecting
    """Context information about a validation error"""

    failing_values: Optional[List[Any]] = None
    additional_info: Optional[List[str]] = None


class ValidationError(BaseModel):
    """Base validation error model"""

    message: str = Field(description="Human-readable error message")
    type: str = Field(description="Machine-readable error code")
    level: ValidationErrorLevel

    locations: Optional[List[ValidationErrorLocation]] = Field(
        default_factory=list, description="Where the error occurred"
    )
    context: Optional[ValidationErrorContext] = None
    check_name: Optional[str] = None
    traceback: Optional[str] = None
    source: Optional[str] = None


class ValidationGroup(BaseModel):
    """Group of related validation errors"""

    group_id: str
    group_type: str
    name: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    errors: List[ValidationError] = Field(default_factory=list)


class ValidationReport(BaseModel):
    """Complete validation report"""

    timestamp: str
    total_errors: int
    error_counts: Dict[ValidationErrorLevel, int] = Field(default_factory=dict)
    groups: List[ValidationGroup] = Field(default_factory=list)
    unexpected_errors: ValidationError
