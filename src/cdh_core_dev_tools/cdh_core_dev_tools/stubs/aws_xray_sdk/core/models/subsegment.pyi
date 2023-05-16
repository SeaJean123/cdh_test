from typing import Any, Optional

class Subsegment:
    def put_http_meta(self, key: str, value: Any) -> None: ...
    def put_metadata(self, key: str, value: Any, namespace: Optional[str]) -> None: ...
    def save_origin_trace_header(self, value: Any) -> None: ...
    def get_origin_trace_header(self) -> Any: ...
    def add_exception(self, exception: Any, stack: Any) -> None: ...
