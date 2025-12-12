# Copyright 2025 Daytona Platforms Inc.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import functools
import inspect
import signal
import sys
import threading
from typing import Callable, Optional, TypeVar

try:
    import stopit
    HAS_STOPIT = True
except ImportError:
    HAS_STOPIT = False

from ..common.errors import DaytonaError, DaytonaTimeoutError

if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

P = ParamSpec("P")
T = TypeVar("T")


def with_timeout() -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator to add timeout mechanism that executes finally blocks properly.
    
    This decorator ensures that finally blocks and context managers execute properly
    when a timeout occurs, allowing for proper resource cleanup. The TimeoutError is
    raised as if it originated from within the decorated function's workflow.
    
    Platform Support:
        - **Async functions**: All platforms (uses asyncio task cancellation)
        - **Sync functions (Unix/Linux, main thread)**: Uses SIGALRM signal
        - **Sync functions (Windows or threads)**: Uses stopit.ThreadingTimeout
    
    Behavior:
        - **Finally blocks**: Execute properly on timeout for both sync and async
        - **Context managers**: __exit__ methods are called with the timeout exception
        - **Resource cleanup**: Guaranteed to execute cleanup code
    
    Limitations:
        - **Async with blocking code**: Cannot interrupt blocking operations like time.sleep().
          Use proper async code (await asyncio.sleep()) instead.
        - **Nested timeouts (Unix)**: SIGALRM is process-wide, may conflict with nested timeouts
    
    Returns:
        Decorated function with timeout enforcement.
    
    Raises:
        DaytonaTimeoutError: When the function exceeds the specified timeout.
        DaytonaError: If timeout is negative or stopit is not installed (Windows/threads).
    
    Example:
        ```python
        @with_timeout()
        async def create_resource(self, timeout=60):
            resource = None
            try:
                resource = await allocate_resource()
                await resource.initialize()
                return resource
            finally:
                # This cleanup ALWAYS executes, even on timeout
                if resource and not resource.initialized:
                    await resource.cleanup()
        ```
    """

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        # Extract timeout from args/kwargs
        def _extract_timeout(args: tuple, kwargs: dict) -> Optional[float]:
            names = func.__code__.co_varnames[: func.__code__.co_argcount]
            bound = dict(zip(names, args))
            return kwargs.get("timeout", bound.get("timeout", None))

        if inspect.iscoroutinefunction(func):
            # Async function: Use asyncio.wait_for (works on all platforms)
            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                timeout = _extract_timeout(args, kwargs)
                if timeout is None or timeout == 0:
                    return await func(*args, **kwargs)
                if timeout < 0:
                    raise DaytonaError("Timeout must be a non-negative number or None.")

                # Use asyncio.wait_for with task cancellation
                # This executes finally blocks via CancelledError propagation
                task = asyncio.create_task(func(*args, **kwargs))
                
                try:
                    return await asyncio.wait_for(task, timeout=timeout)
                except asyncio.TimeoutError:
                    # wait_for already cancelled the task
                    # Wait briefly for cleanup to complete
                    try:
                        await asyncio.wait_for(task, timeout=0.1)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass
                    except Exception:
                        pass
                    
                    raise DaytonaTimeoutError(
                        f"Function '{func.__name__}' exceeded timeout of {timeout} seconds."
                    )  # pylint: disable=raise-missing-from
                except asyncio.CancelledError:
                    raise DaytonaTimeoutError(
                        f"Function '{func.__name__}' exceeded timeout of {timeout} seconds."
                    )  # pylint: disable=raise-missing-from

            return async_wrapper

        # Sync function: Use best available method
        @functools.wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            timeout = _extract_timeout(args, kwargs)
            if timeout is None or timeout == 0:
                return func(*args, **kwargs)
            if timeout < 0:
                raise DaytonaError("Timeout must be a non-negative number or None.")

            # Strategy 1: Unix/Linux main thread - use signals (fastest, most efficient)
            if hasattr(signal, "SIGALRM") and threading.current_thread() is threading.main_thread():
                def _timeout_handler(signum, frame):
                    raise DaytonaTimeoutError(f"Function '{func.__name__}' exceeded timeout of {timeout} seconds.")

                old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
                signal.alarm(int(timeout) + (1 if timeout % 1 > 0 else 0))

                try:
                    result = func(*args, **kwargs)
                    signal.alarm(0)
                    return result
                finally:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, old_handler)
            
            # Strategy 2: Windows or non-main thread - use stopit (cross-platform)
            else:
                if not HAS_STOPIT:
                    raise DaytonaError(
                        f"Timeout for synchronous function '{func.__name__}' on Windows or in "
                        "background threads requires the 'stopit' package. "
                        "Install it with: pip install stopit"
                    )
                
                # Use stopit's threading timeout
                @stopit.threading_timeoutable()
                def _wrapped():
                    return func(*args, **kwargs)
                
                result = _wrapped(timeout=timeout)
                
                # stopit returns TimeoutException as the result when timeout occurs
                if isinstance(result, type) and issubclass(result, BaseException):
                    raise DaytonaTimeoutError(
                        f"Function '{func.__name__}' exceeded timeout of {timeout} seconds."
                    )
                
                return result

        return sync_wrapper

    return decorator
