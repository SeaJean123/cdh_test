from typing import Callable, Dict, Type, Any

from marshmallow.validate import Validator

FIELD_VALIDATORS: Dict[Type[Validator], Callable[..., Any]]
