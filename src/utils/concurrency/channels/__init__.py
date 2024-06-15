"""

# Task Communication Channels

This Module provides an IPC protocol for Tasks to communicate with one another.

"""

from __future__ import annotations

from typing import Protocol, AsyncIterator
from collections.abc import Callable, ByteString
from abc import abstractmethod
from .. import Log
from ..messages import Message
from ..aio.fd import AsyncFileDescriptor

class ChannelMode(enum.Enum):
  """The Mode of the Channel"""
  RO = "read-only"
  """The Channel is Read-Only"""
  WO = "write-only"
  """The Channel is Write-Only"""
  RW = "read-write"
  """The Channel is Read-Write"""

class Channel(Protocol):
  """A Communication Channel between Tasks. This class represents one end of the channel."""
  
  source: AsyncFileDescriptor
  """The source to read from; None if the Channel is One-Way WO Channel"""
  sink: AsyncFileDescriptor
  """The sink to write to; None if the Channel is One-Way RO Channel"""
  mode: ChannelMode
  """The Mode of the Channel"""

  recv_queue: Log[Message]
  """The Queue of Messages received on the Channel."""
  send_queue: Log[Message]
  """The Queue of Messages to be sent on the Channel."""

  unmarshal: Callable[[ByteString], Message]
  """Dependency Injection; The Factory Function to Unmarshal a Message from a ByteString, usually just the `unmarshal` method of the corresponding Message Class"""

  @abstractmethod
  def __aiter__(self) -> AsyncIterator[Message]:
    """Return an Async Iterator for the Channel"""
    ...
  
  @abstractmethod
  def __anext__(self) -> Message:
    """Return the Next Message from the Channel"""
    ...

  @abstractmethod
  async def start(self) -> None:
    """Start Handling Communication on the Channel"""
    ...
  
  @abstractmethod
  async def stop(self) -> None:
    """Stop Handling Communication on the Channel"""
    ...
  
  @abstractmethod
  async def wait(self) -> None:
    """Wait until the peer's end of the channel is ready"""
    ...
    
  @abstractmethod
  async def clear(self) -> None:
    """Clear the Channel of all Messages"""
    ...
  
  @abstractmethod
  async def send(self, message: Message, block: bool) -> None:
    """Enqueue a Message to be sent on the Channel. In Blocking Mode, the method will block until the Message is sent."""
    ...
  
  @abstractmethod
  async def recv(self) -> Message:
    """Receive a Message from the Channel."""
    ...