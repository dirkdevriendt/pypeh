from typing import TypeVar, Union, Sequence, List, Dict, Any, Mapping, TextIO
from pydantic import BaseModel
from peh_model.peh import EntityList, YAMLRoot, NamedThingId, NamedThing

from pypeh.core.models.proxy import TypedLazyProxy


# IO Types

T_Dataclass = TypeVar("T_Dataclass", bound=Union[EntityList, BaseModel])
T_Root = Union[YAMLRoot, NamedThingId]
T_RootStream = Union[T_Root, Mapping[Any, T_Root], Sequence[T_Root]]
IOLike = Union[str, List, List[Dict], TextIO]

# Data model types
T_NamedThingLike = Union[NamedThing, TypedLazyProxy]
