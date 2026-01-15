from enum import Enum, auto

PEH_MODEL_LATEST = "0.0.1"


class FolderEnum(Enum):
    REFERENCELISTS = 1
    PROJECTCONFIG = 2
    DATA = 3


class LocationEnum(Enum):
    LOCAL = "LOCAL"
    URI = "URI"
    CURIE = "CURIE"
    PID = "PID"


class DomainNameEnum(Enum):
    TYPE_API_SCHEMAS = "https://typeapi.lab.pidconsortium.net/v1/types/schema"
    TYPE_REGISTRY_OBJECTS = "https://typeregistry.lab.pidconsortium.net/objects"
    RESOLVE_PID = "https://hdl.handle.net/api/handles"


class TaskStatusEnum(Enum):
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()


class ResponseStatusEnum(Enum):
    COMPLETED = auto()
    FAILED = auto()


class FileTypeEnum(Enum):
    JSON = "application/json"
    YAML = "application/yaml"
    CSV = "text/csv"
    EXCEL = "application/vnd.ms-excel"


class AdapterEnum(Enum):
    DATAFRAME = "dataframe"


class ValidationErrorLevel(Enum):
    INFO = auto()
    WARNING = auto()
    ERROR = auto()
    FATAL = auto()


_ALIASES: dict[str, str] = {
    "decimal": "float",
}


class ObservablePropertyValueType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    FLOAT = "float"
    DECIMAL = "decimal"
    CATEGORICAL = "categorical"
    DATE = "date"
    DATETIME = "datetime"

    def __new__(cls, value):
        # remap before Enum processes it
        normalized = value.lower()
        if normalized in _ALIASES:
            value = _ALIASES[normalized]
        obj = str.__new__(cls, value)
        obj._value_ = value
        return obj

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            normalized = value.lower()
            if normalized in _ALIASES:
                return cls(_ALIASES[normalized])
        return super()._missing_(value)
