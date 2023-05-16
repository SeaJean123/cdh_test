from typing import Any

log: Any
LAMBDA_TRACE_HEADER_KEY: str
LAMBDA_TASK_ROOT_KEY: str
TOUCH_FILE_DIR: str
TOUCH_FILE_PATH: str

def check_in_lambda() -> bool: ...

class LambdaContext:
    def __init__(self) -> None: ...
