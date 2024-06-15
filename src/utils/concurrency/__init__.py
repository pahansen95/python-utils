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

### Library Imports
from .log import *
###

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

