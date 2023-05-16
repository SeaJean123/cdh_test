from typing import Dict, Any

from marshmallow import Schema

class JSONSchema:
    def dump(self, data: Schema) -> Dict[str, Any]: ...
