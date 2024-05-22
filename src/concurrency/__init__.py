from typing import Generic, TypeVar, AsyncGenerator, Iterable, AsyncIterable, Protocol
from abc import abstractmethod
from collections.abc import Coroutine, Callable
import asyncio
from dataclasses import dataclass, field
from collections import deque
from contextlib import asynccontextmanager
from loguru import logger
import enum
import sys

class ConcurrencyBackend(enum.Enum):
  """The Backend to use for Concurrency."""
  PYTHON = "python"
  """Use the Python Backend."""
  LINUX = "linux"
  """Use the Linux Backend."""
  WINDOWS = "windows"
  """Use the Windows Backend."""
  MACOS = "macos"
  """Use the MacOS Backend."""
active_backend: ConcurrencyBackend
if sys.platform.lower().startswith("linux"): active_backend = ConcurrencyBackend.LINUX
elif sys.platform.lower().startswith("win"): active_backend = ConcurrencyBackend.WINDOWS
elif sys.platform.lower().startswith("darwin"): active_backend = ConcurrencyBackend.MACOS
else: active_backend = ConcurrencyBackend.PYTHON

def _log_trapper(coro: Callable[..., Coroutine]) -> Callable[..., Coroutine]:
  async def _log_trap(*args, **kwargs) -> None:
    try:
      return await coro(*args, **kwargs)
    except:
      logger.opt(exception=True).debug(f"Trapped an Exception raised from {coro.__name__}")
      raise
  return _log_trap

I = TypeVar("I")

class Log(Generic[I], Protocol):
  """An Asynchronous Log of Items"""

  log: deque[I]
  """The Item Log"""
  mutex: asyncio.Lock
  """The Async Lock for Mutually Exclusive Access to the Log"""
  item_added: asyncio.Event
  """The Async Event that tracks when Items are added to the Item Log; It's the caller's responsibility to clear the flag."""
  item_removed: asyncio.Event
  """The Async Event that tracks when Items are removed from the Item Log; It's the caller's responsibility to clear the flag."""

  @abstractmethod
  def __len__(self) -> int:
    """Get the length of the Log."""
    ...

  @abstractmethod
  def __iter__(self) -> Iterable[I]:
    """Iterate over the Log."""
    ...

  @abstractmethod
  def __next__(self) -> I:
    """Get the next Item in the Log."""
    ...

  @abstractmethod
  def __aiter__(self) -> AsyncIterable[I]:
    """Iterate over the Log asynchronously."""
    ...

  @abstractmethod
  async def __anext__(self) -> I:
    """Get the next Item in the Log asynchronously."""
    ...

  @abstractmethod
  def __contains__(self, item: I) -> bool:
    """Check if the Log contains an Item."""
    ...

  @abstractmethod
  def __getitem__(self, idx: int) -> I:
    """Get an Item from the Log."""
    ...

  @abstractmethod
  def __setitem__(self, idx: int, item: I) -> None:
    """Set an Item in the Log."""
    ...

  @abstractmethod
  def __delitem__(self, idx: int) -> None:
    """Delete an Item from the Log."""
    ...

  @abstractmethod
  def __reversed__(self) -> Iterable[I]:
    """Get the Log in reverse order."""
    ...

  @property
  @abstractmethod
  def empty(self) -> bool:
    """Check if the Log is empty."""
    ...

  @property
  @abstractmethod
  def full(self) -> bool:
    """Check if the Log is full."""
    ...

  @property
  @abstractmethod
  def max_size(self) -> int | None:
    """Get the maximum size of the Log."""
    ...

  @property
  @abstractmethod
  def locked(self) -> bool:
    """Check if the Log is currently read locked."""
    ...

  @asynccontextmanager
  @abstractmethod
  async def lock(self) -> AsyncGenerator[None, None]:
    """Lock the Log for reading and writing."""
    ...

  @abstractmethod
  async def clear(self) -> None:
    """Clear the Log; Caller must lock the log for Read/Writes first."""
    ...

  @abstractmethod
  async def peek(self, block: bool = True) -> I | None:
    """Peek at the head of the Log (or None if non-blocking & log is empty); does not remove the Item. Caller does not need to lock the log."""
    ...

  @abstractmethod
  async def pop(self, block: bool = True) -> I:
    """Pop the head of the Log (or None if non-blocking & log is empty). Caller does not need to lock the log."""
    ...

  @abstractmethod
  async def push(self, item: I, block: bool = True) -> None | I:
    """Push an Item onto the Log at the tail (or returns the item if non-blocking & log is full). Caller does not need to lock the log."""
    ...

@dataclass
class _ItemLogCtx:
  push_log: asyncio.Queue = field(default_factory=asyncio.Queue)
  """The Async Queue that logs all push events that occur in the Item Log."""
  pop_log: asyncio.Queue = field(default_factory=asyncio.Queue)
  """The Async Queue that logs all pop events that occur in the Item Log."""
  iter_idx: int = 0
  """The current index of the Item Log when iterating over it."""
  aiter_idx: int = 0
  """The current index of the Item Log when asynchronously iterating over it."""

@dataclass
class ItemLog(Log[I]):
  """An Async Log of Items"""

  log: deque[I] = field(default_factory=deque)
  """The Item Log"""
  mutex: asyncio.Lock = field(default_factory=asyncio.Lock)
  """The Async Lock for Mutually Exclusive Access to the Log"""
  item_added: asyncio.Event = field(default_factory=asyncio.Event)
  """The Async Event that tracks when Items are added to the Item Log; It's the caller's responsibility to clear the flag."""
  item_removed: asyncio.Event = field(default_factory=asyncio.Event)
  """The Async Event that tracks when Items are removed from the Item Log; It's the caller's responsibility to clear the flag."""
  _ctx: _ItemLogCtx = field(default_factory=_ItemLogCtx)

  def __len__(self) -> int:
    """Get the length of the Log."""
    return len(self.log)

  def __iter__(self) -> Iterable[I]:
    """Iterate over the Log."""
    self._ctx.iter_idx = 0
    return self
  
  def __next__(self) -> I:
    """Get the next Item in the Log."""
    if self._ctx.iter_idx >= len(self.log): raise StopIteration
    self._ctx.iter_idx += 1
    return self.log[self._ctx.iter_idx - 1]

  def __aiter__(self) -> AsyncIterable[I]:
    """Iterate over the Log asynchronously."""
    self._ctx.aiter_idx = 0
    return self

  async def __anext__(self) -> I:
    """Get the next Item in the Log asynchronously."""
    if self._ctx.aiter_idx >= len(self.log): raise StopAsyncIteration
    self._ctx.aiter_idx += 1
    return self.log[self._ctx.aiter_idx - 1]

  def __contains__(self, item: I) -> bool:
    """Check if the Log contains an Item."""
    return item in self.log
  
  def __getitem__(self, idx: int) -> I:
    """Get an Item from the Log."""
    return self.log[idx]
  
  def __setitem__(self, idx: int, item: I) -> None:
    """Set an Item in the Log."""
    self.log[idx] = item
  
  def __delitem__(self, idx: int) -> None:
    """Delete an Item from the Log."""
    del self.log[idx]
  
  def __reversed__(self) -> Iterable[I]:
    """Get the Log in reverse order."""
    return reversed(self.log)

  @property
  def empty(self) -> bool:
    """Check if the Log is empty."""
    logger.trace(f"Log {id(self)} - Len is {len(self.log)}")
    if len(self.log) > 0: logger.trace(f"Log {id(self)} - Contents {list(self.log)}")
    return len(self.log) <= 0

  @property
  def full(self) -> bool:
    """Check if the Log is full."""
    logger.trace(f"Log {id(self)} - Len is {len(self.log)}")
    logger.trace(f"Log {id(self)} - MaxLen is {'UNBOUND' if self.log.maxlen is None else self.log.maxlen}")
    if self.log.maxlen is None: return False
    else: return len(self.log) >= self.log.maxlen

  @property
  def max_size(self) -> int | None:
    """Get the maximum size of the Log."""
    return self.log.maxlen
  
  @property
  def locked(self) -> bool:
    """Check if the Log is currently read locked."""
    return self.mutex.locked()  
  
  @asynccontextmanager
  async def lock(self) -> AsyncGenerator[None, None]:
    """Lock the Log for reading and writing."""
    async with self.mutex:
      yield

  async def clear(self) -> None:
    """Clear the Log; Caller must lock the log for Read/Writes first."""
    if self.empty: return
    elif not self.locked: raise RuntimeError("cannot clear log while unlocked")
    self.log.clear()
    self.item_added.clear()
    self.item_removed.clear()
    self._ctx.pop_log = asyncio.Queue()
    self._ctx.push_log = asyncio.Queue()

  async def peek(self, block: bool = True) -> I | None:
    """Peek at the head of the Log (or None if non-blocking & log is empty); does not remove the Item. Caller does not need to lock the log."""
    logger.trace(f"Log {id(self)} - Peeking at the log")
    if self.empty and not block: return None
    while True:
      logger.trace(f"Peek Log {id(self)} - Waiting for push log")
      # A Push Event MUST have occured in order to peek an event.
      await self._ctx.push_log.get()
      logger.trace(f"Peek Log {id(self)} - Push log event received")
      self._ctx.push_log.task_done()
      logger.trace(f"Peek Log {id(self)} - Acquiring lock")
      async with self.mutex:
        logger.trace(f"Peek Log {id(self)} - Lock acquired")
        if self.empty: logger.trace(f"Peek Log {id(self)} - Log is Empty!"); continue # TODO (Should I do this?): There's nothing in the Queue so drop the push event
        logger.trace(f"Peek Log {id(self)} - Replace Push Event")
        await self._ctx.push_log.put(None) # We aren't actually consuming an item so make sure to keep the log in-sync
        logger.trace(f"Peek Log {id(self)} - Returning item")
        return self.log[0]
  
  async def pop(self, block: bool = True) -> I:
    """Pop the head of the Log (or None if non-blocking & log is empty). Caller does not need to lock the log."""
    logger.trace(f"Log {id(self)} - Popping an item from the log")
    if self.empty and not block: return None
    while True:
      logger.trace("Pop Loop Iteration")
      # To Pop an item, a push event MUST have occured
      logger.trace(f"Pop Log {id(self)} - Consuming a push event")
      await self._ctx.push_log.get()
      self._ctx.push_log.task_done()
      logger.trace(f"Pop Log {id(self)} - Acquiring lock")
      async with self.mutex:
        logger.trace(f"Pop Log {id(self)} - Lock acquired")
        if self.empty: logger.trace(f"Pop Log {id(self)} - Log is Empty!"); continue # Prevent Race Conditions
        logger.trace(f"Pop Log {id(self)} - Put Pop Event")
        await self._ctx.pop_log.put(None)
        self.item_removed.set()
        logger.trace(f"Pop Log {id(self)} - Returning item")
        return self.log.popleft()
  
  async def push(self, item: I, block: bool = True) -> None | I:
    """Push an Item onto the Log at the tail (or returns the item if non-blocking & log is full). Caller does not need to lock the log."""
    logger.trace(f"Log {id(self)} - Pushing an item on the log")
    if self.full and not block: return item
    while True:
      if not self.empty: # Don't block if the log is empty; otherwise consume a pop event
        logger.trace(f"Push Log {id(self)} - Consuming a pop event")
        await self._ctx.pop_log.get()
        self._ctx.pop_log.task_done()
      logger.trace(f"Push Log {id(self)} - Acquiring lock")
      async with self.mutex:
        logger.trace(f"Push Log {id(self)} - Lock acquired")
        if self.full: logger.trace(f"Push Log {id(self)} - Log is Empty!"); continue # Prevent Race Conditions
        logger.trace(f"Push Log {id(self)} - Put Push Event")
        await self._ctx.push_log.put(None)
        self.log.append(item)
        self.item_added.set()
        logger.trace(f"Push Log {id(self)} - Item pushed")
        return
