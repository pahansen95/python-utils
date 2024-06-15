"""

# Errors

Platform Agnostic Error Messages

"""

from .. import Message, DataclassJSONMessageMixin
import enum
from dataclasses import dataclass, field, KW_ONLY
from typing import Any
import uuid

class ErrorKind(enum.Enum):
  """The Kind of Error Encounterd"""
  APPLICATION = enum.auto()
  """An Application dependent Error"""
  RUNTIME = enum.auto()
  """Some unexpected error occurred at runtime"""

@dataclass
class ErrorPayload:
  """TODO: What Exactly is the Payload for an Error?"""
  ...

@dataclass
class Error(Message, DataclassJSONMessageMixin):
  kind: ErrorKind
  payload: ErrorPayload # What exactly is the payload for an Error?

  _ : KW_ONLY

  id: str = field(default_factory=lambda: str(uuid.uuid4()))
  