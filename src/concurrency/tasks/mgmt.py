"""

# The Task Mgmt Module

Datastructures & functionality related to the Management of Tasks.

"""

from __future__ import annotations
from typing import Any
import enum
from ..messages import Message, DataclassJSONMessageMixin
import uuid
from dataclasses import dataclass, field, KW_ONLY, asdict


class MgmtMsgKind(enum.Enum):
  """The Kind of Management Message"""
  ### Controller Messages ###
  CANCEL = "cancel"
  """Cancel the Task"""

  ### Task Messages ###
  READY = "ready"
  """The Task is ready"""
  COMPLETE = "complete"
  """The Task has completed"""

@dataclass
class MgmtMsgPayload:
  """A Payload for a Management Message"""
  ...

  @classmethod
  def from_map(cls, m: dict[str, Any]) -> MgmtMsgPayload:
    raise NotImplementedError()

@dataclass
class MgmtMsg(DataclassJSONMessageMixin, Message):
  """A Message for Task Management"""

  kind: MgmtMsgKind
  """The Kind of Message being spent"""
  payload: MgmtMsgPayload | None
  """A Message Payload (or None for thin messages)"""

  _ = KW_ONLY

  id: str = field(default_factory=lambda: str(uuid.uuid4()))
  """A unique identifier for the Message"""

  @classmethod
  def from_map(cls, m: dict[str, Any]) -> MgmtMsg:
    return cls(
      kind=MgmtMsgKind(m["kind"]),
      id=m["id"],
      payload=MgmtMsgPayload.from_map(m["payload"]),
    )
  
  @classmethod
  def unmarshal(cls, buffer: bytes | bytearray | memoryview) -> MgmtMsg:
    return cls.from_map(cls.__load_json(buffer))
