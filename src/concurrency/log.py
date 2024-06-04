"""

Implements an Asynchronous Log of Items for tracking events over time

"""
from __future__ import annotations
from typing import Generic, TypeVar, AsyncGenerator, Iterable, AsyncIterable, Protocol
from abc import abstractmethod
from dataclasses import dataclass, field
from collections import deque
from contextlib import asynccontextmanager
from loguru import logger
import asyncio

__all__ = [
  'Log',
  'ItemLog'
]

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

def _create_event(state: bool) -> asyncio.Event:
  event = asyncio.Event()
  if state: event.set()
  return event

@dataclass
class _ItemLogCtx:
  empty: asyncio.Event = field(default_factory=lambda: _create_event(True))
  """Is the Item Log Empty"""
  not_empty: asyncio.Event = field(default_factory=lambda: _create_event(False))
  """Is the Item Log not empty"""
  full: asyncio.Event = field(default_factory=lambda: _create_event(False))
  """Is the Item Log Full"""
  not_full: asyncio.Event = field(default_factory=lambda: _create_event(True))
  """Is the Item Log not Full"""
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
    return len(self.log) <= 0

  @property
  def full(self) -> bool:
    """Check if the Log is full."""
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
    if not self.locked: raise RuntimeError("cannot clear log while unlocked")
    self.log.clear()
    self._ctx.empty.set()
    self._ctx.not_empty.clear()
    self._ctx.full.clear()
    self._ctx.not_full.set()

  async def peek(self, block: bool = True) -> I | None:
    """Peek at the head of the Log (or None if non-blocking & log is empty); does not remove the Item. Caller does not need to lock the log."""
    while True:
      # Wait for an Item or short circuit
      if block: await self._ctx.not_empty.wait()
      elif not self._ctx.not_empty.is_set(): return None # Short Circuit if non-blocking
      # Return the head of the log
      async with self.mutex:
        if not self._ctx.not_empty.is_set(): continue # Protect against Race Conditions
        return self.log[0]
  
  async def pop(self, block: bool = True) -> I | None:
    """Pop the head of the Log (or None if non-blocking & log is empty). Caller does not need to lock the log."""
    while True:
      # Wait for an Item or Short Circuit
      if block: await self._ctx.not_empty.wait() # Wait for an item to be pushed
      elif not self._ctx.not_empty.is_set(): return None # Short Circuit if non-blocking
      # Pop the head of the log
      async with self.mutex:
        if self._ctx.empty.is_set(): continue # Protect against Race Conditions
        item = self.log.popleft()
        self._ctx.full.clear()
        self._ctx.not_full.set()
        if len(self.log) == 0: # We popped the last item in the log
          self._ctx.empty.set()
          self._ctx.not_empty.clear()
        return item
  
  async def push(self, item: I, block: bool = True) -> None | I:
    """Push an Item onto the Log at the tail (or returns the item if non-blocking & log is full). Caller does not need to lock the log."""
    while True:
      # Wait for a slot or shortcircuit
      if not block and self._ctx.full.is_set(): return item # Short Circuit
      elif block and self._ctx.full.is_set(): await self._ctx.not_full.wait() # Wait until the log has a slot

      # Pop the head of the log
      async with self.mutex:
        if self._ctx.full.is_set(): continue # Protect against Race Conditions
        self.log.append(item)
        self._ctx.not_empty.set()
        self._ctx.empty.clear()
        if self.log.maxlen is not None and len(self.log) >= self.log.maxlen: # We pushed into the last avialable slot on a bounded queue
          self._ctx.full.set()
          self._ctx.not_full.clear()
        return None
