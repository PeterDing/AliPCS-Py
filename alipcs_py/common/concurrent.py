from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Callable, Any
from functools import wraps
from threading import Semaphore


def sure_release(semaphore: Semaphore, func, *args, **kwargs):
    """Release semaphore after func is done."""

    try:
        return func(*args, **kwargs)
    finally:
        semaphore.release()


def retry(times: int, except_callback: Optional[Callable[[Exception, int], Any]] = None):
    """Retry times when func fails"""

    def wrap(func):
        @wraps(func)
        def retry_it(*args, **kwargs):
            nonlocal times
            if times < 0:  # forever
                times = 1 << 32

            for i in range(1, times + 1):
                try:
                    r = func(*args, **kwargs)
                    return r
                except Exception as err:
                    if except_callback is not None:
                        except_callback(err, i)

                    if i == times:
                        raise err

        return retry_it

    return wrap


class Executor:
    """
    Executor is a ThreadPoolExecutor when max_workers > 1, else a single thread executor.
    """

    def __init__(self, max_workers: int = 1):
        self._max_workers = max_workers
        self._pool = ThreadPoolExecutor(max_workers=max_workers) if max_workers > 1 else None
        self._semaphore = Semaphore(max_workers)
        self._futures = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._pool is not None:
            as_completed(self._futures)
            self._pool.shutdown()
            self._futures.clear()

    def submit(self, func, *args, **kwargs):
        if self._pool is not None:
            self._semaphore.acquire()
            fut = self._pool.submit(sure_release, self._semaphore, func, *args, **kwargs)
            self._futures.append(fut)
            return fut
        else:
            return func(*args, **kwargs)
