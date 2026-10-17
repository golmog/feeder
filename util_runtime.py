"""Cooperative checkpoints for long scans without changing FF's worker setup."""
import time
from contextvars import ContextVar


_LAST_YIELD = ContextVar('feeder_last_yield', default=None)
_YIELD_INTERVAL = 0.02
_YIELD_DELAY = 0.001


def pause(seconds=_YIELD_DELAY):
    try:
        from gevent import monkey, sleep
    except ImportError:
        time.sleep(seconds)
        return
    if monkey.is_module_patched('socket') or monkey.is_module_patched('time'):
        sleep(seconds)
    else:
        time.sleep(seconds)


def cooperate():
    """Yield at most once per interval, independently in each execution context."""
    now = time.monotonic()
    last = _LAST_YIELD.get()
    if last is None or now - last >= _YIELD_INTERVAL:
        pause()
        _LAST_YIELD.set(time.monotonic())
