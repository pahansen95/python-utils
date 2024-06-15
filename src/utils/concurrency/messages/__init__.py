"""

# Message

Platform Agnostic Messages

"""

from typing import Protocol, TypeVar, Generic, Mapping, ByteString
from abc import abstractmethod
import enum
import orjson
from dataclasses import asdict
import blake3
import enum
from typing import Any

T = TypeVar("T")

class Message(Generic[T], Protocol):
  """A Message agnostic to the underlying communication protocol"""

  kind: enum.Enum
  """The Kind of Message being spent"""
  id: str
  """A unique identifier for the Message"""
  payload: T
  """The Underlying Payload of the Message"""

  @property
  @abstractmethod
  def checksum(self) -> int:
    """The Checksum of the Message"""
    ...

  @abstractmethod
  def as_map(self) -> Mapping[str, T]:
    """Convert the Message into a Key-Value Mapping"""
    ...

  @classmethod
  @abstractmethod
  def from_map(cls, m: dict[str, T]) -> "Message":
    """Create a Message from a Key-Value Mapping"""
    ...
  
  @abstractmethod
  def marshal(self) -> ByteString:
    """Marshal the Message into a Byte String"""
    ...
  
  @classmethod
  @abstractmethod
  def unmarshal(cls, buffer: ByteString) -> "Message":
    """Unmarshal a Byte String from the buffer into a Message"""
    ...

class DataclassJSONMessageMixin:
  """A Convenience mixin for JSON Encoding Dataclasses as Messages"""

  @property
  def checksum(self) -> int:
    """The Checksum of the Message"""
    return int.from_bytes(blake3.blake3(orjson.dumps(self, option=orjson.OPT_SORT_KEYS)).digest(), "big")

  def marshal(self) -> bytes:
    """Marshal the Message into a Byte String"""
    return orjson.dumps(self)

  def as_map(self) -> dict[str, Any]:
    """Convert the Message into a Key-Value Mapping"""
    return orjson.loads(orjson.dumps(self)) # Faster than dataclasses.asdict (I think)
  
  @staticmethod
  def __load_json(buffer: ByteString) -> dict[str, Any]:
    """Dependency Injection; Load JSON from a Byte String for further processing"""
    return orjson.loads(buffer)