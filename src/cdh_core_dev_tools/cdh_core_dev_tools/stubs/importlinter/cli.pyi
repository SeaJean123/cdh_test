from typing import Optional, Tuple

def lint_imports(
    config_filename: Optional[str] = None,
    limit_to_contracts: Tuple[str, ...] = (),
    cache_dir: Optional[str] = None,
    no_cache: bool = False,
    is_debug_mode: bool = False,
    show_timings: bool = False,
    verbose: bool = False,
) -> int: ...
