from typing import Any

class AWSRequestsAuth:
    aws_access_key: str
    aws_secret_access_key: str
    aws_token: str
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
