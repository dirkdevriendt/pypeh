from typing import TypeVar, Union, Sequence, List, Dict, Any, Mapping, TextIO
from pydantic import BaseModel

from pypeh.core.models.peh import EntityList, YAMLRoot, NamedThingId


T_Dataclass = TypeVar("T_Dataclass", bound=Union[EntityList, BaseModel])
T_Root = Union[YAMLRoot, NamedThingId]
T_RootStream = Union[T_Root, Mapping[Any, T_Root], Sequence[T_Root]]
IOLike = Union[str, List, List[Dict], TextIO]
