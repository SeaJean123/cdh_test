from typing import Optional, Union, Collection, Tuple, NamedTuple, Set

class RequestHistory(NamedTuple):
    method: Optional[str]
    url: Optional[str]
    error: Optional[Exception]
    status: Optional[int]
    redirect_location: Optional[str]

class Retry:
    DEFAULT_METHOD_WHITELIST: Collection[str]
    DEFAULT_REDIRECT_HEADERS_BLACKLIST: Collection[str]
    DEFAULT_ALLOWED_METHODS: Set[str]
    RETRY_AFTER_STATUS_CODES: Collection[int]
    BACKOFF_MAX: int
    total: Optional[Union[bool, int]]
    connect: Optional[int]
    read: Optional[int]
    redirect: Optional[int]
    status: Optional[int]
    other: Optional[int]
    allowed_methods: Optional[Collection[str]]
    status_forcelist: Collection[int]
    method_whitelist: Optional[Collection[str]]
    backoff_factor: float
    raise_on_redirect: bool
    raise_on_status: bool
    respect_retry_after_header: bool
    remove_headers_on_redirect: Collection[str]
    def __init__(
        self,
        total: Optional[Union[bool, int]] = ...,
        connect: Optional[int] = ...,
        read: Optional[int] = ...,
        redirect: Optional[Union[bool, int]] = ...,
        status: Optional[int] = ...,
        other: Optional[int] = ...,
        allowed_methods: Optional[Collection[str]] = ...,
        status_forcelist: Optional[Collection[int]] = ...,
        backoff_factor: float = ...,
        raise_on_redirect: bool = ...,
        raise_on_status: bool = ...,
        history: Optional[Tuple[RequestHistory, ...]] = ...,
        respect_retry_after_header: bool = ...,
        remove_headers_on_redirect: Collection[str] = ...,
        method_whitelist: Optional[Collection[str]] = ...,
    ) -> None: ...
