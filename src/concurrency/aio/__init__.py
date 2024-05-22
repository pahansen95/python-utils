from __future__ import annotations
import enum, os, sys, time, asyncio
from dataclasses import dataclass, field, KW_ONLY
from typing import Callable, Iterable, Generic, TypeVar, AsyncGenerator, Literal
from collections import deque
from contextlib import asynccontextmanager
from loguru import logger
from .. import ItemLog, ConcurrencyBackend, active_backend

AIOBackend = ConcurrencyBackend
"""The Asynchronous IO Backend to use."""
aio_backend = active_backend
"""The Active Asynchronous IO Backend."""

CHUNK_SIZE = int(16*1024)
"""The Chunk Size for IO Operations: 16k"""

class AIOError(RuntimeError):
  """An Error occurred during Asynchronous IO."""
  def __init__(self, reason: IOErrorReason, msg: str):
    self.reason = reason
    self.msg = msg

  def __str__(self) -> str:
    return f"{self.reason.name}: {self.msg}"  

class IOErrorReason(enum.IntEnum):
  NONE = 0
  """No Error occurred."""
  BLOCK = 1
  """The operation would block."""
  EOF = 2
  """The End of the File was reached."""
  CLOSED = 3
  """The File Descriptor was closed."""
  ERROR = 4
  """An IO Error occurred in the Kernel."""
  INTERRUPT = 5
  """The operation was interrupted by a signal before any I/O actions were taken."""
  UNHANDLED = 255
  """An unhandled error occurred. Should only be used when raising exceptions"""

class IOCondition(enum.IntEnum):
  """A subset of IOErrorReason that indicate common conditions"""
  BLOCK = IOErrorReason.BLOCK
  EOF = IOErrorReason.EOF
  CLOSED = IOErrorReason.CLOSED
  ERROR = IOErrorReason.ERROR
  INTERRUPT = IOErrorReason.INTERRUPT

class IOEvent(enum.IntFlag):
  UNDEFINED = 0
  """A Runtime Error"""
  READ = enum.auto()
  """The File Descriptor can be read from"""
  WRITE = enum.auto()
  """The File Descriptor can be written to"""
  ERROR = enum.auto()
  """The File Descriptor encountered an error"""
  CLOSE = enum.auto()
  """The File Descriptor (or Peer) was closed"""

def _calculate_backoff_curve(fn: Callable[[float], float], steps: int) -> tuple[float]:
  """Calculate a Backoff Curve of N steps between [0, 1]; the curve function should return values in [0, 1] given the steps relative position in [0, 1]"""
  if steps < 1: raise ValueError("steps must be at least 1")
  _steps: tuple[float] = tuple((1 / (steps - 1)) * step for step in range(steps))
  _curve: tuple[float] = tuple(fn(y) for y in _steps)
  # Scale Curve to fit in [0, 1]
  _min, _max = min(_curve), max(_curve)
  if _min < 0 or _max > 1: # Apply Min-Max Normalization
    if _min == _max: _curve = tuple(0.5 for _ in _curve) # Handle Divide by Zero (ie. constant function)
    else: _curve = tuple((y - _min) / (_max - _min) for y in _curve)
  return _curve

@dataclass
class _CurveCtx:
  linrange: tuple[float] = None
  """The Computed Linear Range of the Curve over the declared steps."""

@dataclass
class BackoffCurve:
  """A Curve to calculate the time to wait for a backoff."""

  steps: int
  """How many discrete steps the curve has."""
  max_backoff_ns: int
  """The maximum backoff wait time in nanoseconds."""
  curve: Callable[[float], float]
  """The Curve to use for calculating the backoff wait time."""
  _: KW_ONLY
  _ctx: _CurveCtx = field(default_factory=_CurveCtx)

  def __post_init__(self):
    self._ctx.linrange = tuple(round(y * self.max_backoff_ns) for y in _calculate_backoff_curve(self.curve, self.steps))
    logger.trace(f"Backoff Curve (s): {[x / 1E9 for x in self._ctx.linrange]}")
    # raise NotImplementedError

  def remaining_wait_time(self, datum: int, idx: int) -> int:
    """How much longer in nanoseconds to wait for the next backoff; 0 if the backoff is complete."""
    # _idx = min(idx, self.steps - 1); logger.trace(f"Backoff Curve Index: {_idx}")
    # _raw_wait = self._ctx.linrange[_idx]; logger.trace(f"Backoff Curve Wait (ns): {_raw_wait}")
    # _wait = _raw_wait - (time.monotonic_ns() - datum); logger.trace(f"Backoff Curve Remaining Wait (ns): {_wait}")
    # _clamped_wait = max(0, _wait); logger.trace(f"Backoff Curve Clamped Wait (ns): {_clamped_wait}")
    # return _clamped_wait
    return max(0, self._ctx.linrange[min(idx, self.steps - 1)] - (time.monotonic_ns() - datum))

def common_backoff_curves(steps = 10, max_backoff_ns = int((100/1e3) * 1e9), choice: Literal['cubic'] = 'cubic') -> BackoffCurve:
  """Create Common Backoff Curves; by default, backoff up to 100ms following over 10 steps.
  
  Curves Available:

    - Cubic: https://www.desmos.com/calculator/2oxeytilfm
  
  """
  # Backoff up to 100ms following a cubic curve over 10 steps"""
  if choice.lower() == "cubic": _fn = lambda y: y ** 3
  else: raise ValueError(f"invalid choice '{choice}'")
  return BackoffCurve(steps, max_backoff_ns, _fn)


# # Let's import everything else

# from .watch import (
#   WatchBackend, HIGHEST_PRIORITY_WATCH_BACKEND,
#   IOWatcher, io_watcher,
# )
# from .fd import (
#   AsyncFileDescriptor
# )