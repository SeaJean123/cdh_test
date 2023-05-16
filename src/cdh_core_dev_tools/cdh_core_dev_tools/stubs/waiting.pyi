from typing import Callable, Any, Union, Tuple, Type

def wait(
    predicate: Callable[[], Any],
    timeout_seconds: float = ...,
    sleep_seconds: float = ...,
    on_poll: Callable[[], Any] = ...,
    waiting_for: Any = ...,
    expected_exceptions: Union[Tuple[Exception], Exception, Type[Exception]] = ...,
) -> Any: ...

class TimeoutExpired(Exception): ...
