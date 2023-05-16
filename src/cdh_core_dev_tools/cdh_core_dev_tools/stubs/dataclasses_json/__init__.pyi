import abc
from enum import Enum
from typing import Callable, Optional, Union, Dict, Any, Tuple, Type, TypeVar

Json = Union[dict, list, str, int, float, bool, None]
A = TypeVar("A")
JsonData = Union[str, bytes, bytearray]

class Undefined(Enum): ...

class LetterCase(Enum):
    CAMEL: str

class Exclude:
    ALWAYS: Callable[[Any], bool]

def config(
    metadata: Optional[Dict[Any, Any]] = None,
    *,
    encoder: Optional[Callable[..., Any]] = None,
    decoder: Optional[Callable[..., Any]] = None,
    mm_field: Optional[Any] = None,
    letter_case: Optional[Callable[[str], str]] = None,
    undefined: Optional[Union[str, Undefined]] = None,
    field_name: Optional[str] = None,
    exclude: Optional[Callable[[str], bool]] = None,
) -> Dict[str, Dict[Any, Any]]: ...

class DataClassJsonMixin(abc.ABC):
    def to_json(
        self,
        *,
        skipkeys: Optional[bool] = False,
        ensure_ascii: Optional[bool] = True,
        check_circular: Optional[bool] = True,
        allow_nan: Optional[bool] = True,
        indent: Optional[Union[int, str]] = None,
        separators: Optional[Tuple[str, str]] = None,
        default: Optional[Callable[..., Any]] = None,
        sort_keys: Optional[bool] = False,
        **kw: Any,
    ) -> str: ...
    def to_dict(self, encode_json: Optional[bool] = False) -> Dict[str, Json]: ...
    @classmethod
    def from_json(
        cls: Type[A],
        s: JsonData,
        *,
        parse_float: Any = None,
        parse_int: Any = None,
        parse_constant: Any = None,
        infer_missing: Optional[bool] = False,
        **kw: Any,
    ) -> A: ...
    @classmethod
    def from_dict(cls: Type[A], kvs: Json, *, infer_missing: bool = False) -> A: ...

def dataclass_json(
    _cls: Type[A], *, letter_case: Optional[LetterCase] = None, undefined: Optional[Union[str, Undefined]] = None
) -> Type[A]: ...
