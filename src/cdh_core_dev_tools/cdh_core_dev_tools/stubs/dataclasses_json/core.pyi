from typing import Union, List, NamedTuple, Callable, Any

Json = Union[dict, list, str, int, float, bool, None]
confs: List[str]

class FieldOverride(NamedTuple):
    exclude: Callable[[str], bool]
    encoder: Callable[[Any], Any]
    letter_case: Callable[[str], str]
