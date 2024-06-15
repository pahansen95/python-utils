"""

# Gates

A Gate is a concurrency primitive that is used to manage access between
a pool of resources and a pool of workers. The Gate is used to control
the flow of access to the resources.

"""
from __future__ import annotations
from typing import Protocol, Any, runtime_checkable
from abc import abstractmethod

@runtime_checkable
class Gate(Protocol):
  """A Gate to manage concurrent access to some singular or pool of resources"""

  async def __aenter__(self) -> Gate:
    """Acquire the Gate"""
    await self.acquire()
    return self

  async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
    """Release the Gate"""
    await self.release()

  @abstractmethod
  async def acquire(self) -> None: ...

  @abstractmethod
  async def release(self) -> None: ...

### Concrete Implementations ###

class DummyGate(Gate):
  """A Dummy Gate that does nothing"""
  async def acquire(self) -> None: pass
  async def release(self) -> None: pass