"""

# The Task Utility

Spawn & Manage new Tasks

"""

from __future__ import annotations
from typing import Protocol, runtime_checkable, Any
import enum
from . import ConcurrencyBackend, active_backend
from ..channels import Channel
from ..messages import Message, DataclassJSONMessageMixin
from abc import abstractmethod
import uuid
from dataclasses import dataclass, field, KW_ONLY, asdict

class TaskBackend(enum.Enum):
  OS_EXEC = "OS-Exec"
  """Spawn a new OS Process using the `exec` system calls"""

class TaskState(enum.Enum):
  UNDEFINED = "undefined"
  """The Task is in an undefined State"""
  INIT = "initaliatizing"
  """The Task has been spawned but isn't running yet"""
  RUNNING = "running"
  """The Task is actively running"""
  COMPLETED = "completed"
  """The Task completed on it's own; irrespective of success or failure"""
  CANCELLED = "cancelled"
  """The Task was cancelled before it could complete on it's own"""
  ERROR = "error"
  """The Task encountered an error during execution (success or fail wasn't captured)"""  

class TaskCompletionStatus(enum.Enum):
  SUCCESS = "success"
  """The Task completed successfully"""
  FAILURE = "failure"
  """The Task completed with a failure"""
  CANCELLED = "cancelled"
  """The Task was cancelled before it could complete"""

@runtime_checkable
class Task(Protocol):

  ### Config Properties ### 

  id: str
  """A unique identifier for the Task"""
  backend: TaskBackend
  """The Backend used to spawn the Task"""
  cmd: Any
  """The Command to execute (or equivalent)"""
  argv: list[Any]
  """The Command Arguments (or equivalent); Value Types are Backend Dependent"""
  env: dict[str, Any]
  """The Environment Variables (or equivalent); Value Types are Backend Dependent"""
  mgmt_chan: Channel[Any]
  """The Management Channel for the Task (RW); implementation is backend dependent"""
  data_chan: Channel[Any]
  """The Data Channel for the Task (RW); implementation is backend dependent"""
  error_chan: Channel[Any]
  """The Error Sink for the Task (RO); implementation is backend dependent"""

  ### Runtime Properties ###

  state: TaskState | None
  """The current State of the Task (None if it hasn't been spawned yet)"""
  complete_status: TaskCompletionStatus | None
  """The Completion Status of the Task (None if it hasn't completed yet)"""
  error: Exception | str | None
  """The Error that occurred during the Task (None if the state is not ERROR)"""

  ### Task Interface ###

  @abstractmethod
  async def spawn(self) -> None:
    """Spawn the Task"""
    ...
  
  @abstractmethod
  async def cancel(self) -> None:
    """Cancel execution of the Task"""
    ...
  
  @abstractmethod
  async def wait(self) -> TaskState:
    """Wait for the Task to complete, cancel or error out. Returns the final state of the Task"""
    ...
    
  