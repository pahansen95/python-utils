"""

Asynchronous publish-subscribe communication.

"""
from __future__ import annotations
import time, orjson
from typing import Protocol, TypeVar, Generic, TypedDict, NotRequired, Literal
from collections.abc import Callable, Coroutine
from loguru import logger
from dataclasses import dataclass, field

from utils.errors import Error, NO_ERROR_T, NO_ERROR
from utils.concurrency.log import ItemLog

OBJ = TypeVar('OBJ', bound=dict)
userdata_t = dict[str, str]

def _raise_TODO(msg: str) -> None:
  logger.critical(f'TODO: {msg}')
  raise NotImplementedError(f'TODO: {msg}')

def generate_id(prefix: str, now: int = None) -> str:
  if now is None: now = time.monotonic_ns()
  return f"{prefix}-{now:x}"

class Message(Generic[OBJ], TypedDict):
  """A Datastructure representing an `inflight` payload."""
  metadata: Message.Metadata
  """Metadata associated w/ a Message"""
  payload: OBJ
  """A JSON Encodable dict of arbitrary content"""
  class Metadata(TypedDict):
    id: str
    """A Unique Identifier for the Message"""
    channel: str
    """A Unique Identifier for the Channel ID this Message Belongs to"""
    sender: NotRequired[str]
    """A Unique Identifier identifying who originally published this message"""
    recipient: NotRequired[str]
    """A Unique Identifier identifying what subscriber this message is destined to"""
    timestamp: int
    """The chronological timestamp when this message was published; the formating is backend dependant"""
    userdata: NotRequired[userdata_t]
    """Arbitrary Key-Value Metadata"""
  
  @staticmethod
  def factory(
    payload: OBJ,
    userdata: dict[str, str],
    **kwargs
  ) -> Message[OBJ]:
    now = time.monotonic_ns()
    metadata = { 'id': generate_id('msg', now=now), 'timestamp': now, 'userdata': userdata } | kwargs
    return { 'metadata': metadata, 'payload': payload }
  
  @staticmethod
  def marshal(msg: Message[OBJ], opts: int = orjson.OPT_APPEND_NEWLINE) -> bytes:
    return orjson.dumps(
      { 'metadata': msg['metadata'], 'payload': orjson.Fragment(msg['payload']) },
      option=opts
    )

  @staticmethod
  def unmarshal(data: bytes) -> Message[OBJ]:
    return orjson.loads(data)

unpacked_msg_t = tuple[OBJ, userdata_t]
msg_push_t = Callable[[Message[OBJ]], Coroutine[None, None, Error | NO_ERROR_T]]
msg_filter_t = Callable[[str, OBJ, dict[str, str]], bool]

class CongestionConfig(TypedDict):
  mode: Literal['buffer', 'drop']
  """The mode of congestion handling"""
  opts: NotRequired[BufferOpts]
  """Mode Specific Configuration Options"""

  class BufferOpts(TypedDict):
    limit: NotRequired[int]
    """The maximum number of messages to buffer before dropping messages; if omitted, the buffer is unbounded."""
    direction: NotRequired[Literal['head', 'tail']]
    """The direction to drop messages from when a bounded buffer is full; defaults to 'head' if omitted."""

@dataclass
class ChannelRegistry:
  messages: dict[str, Message] = field(default_factory=dict)
  """All currently recorded Messages in the Registry"""
  channels: dict[str, ItemLog[str]] = field(default_factory=dict)
  """The known set of Channels & their current Log of Messages"""
  publishments: dict[str, set[str]] = field(default_factory=dict)
  """Publishers associated with a channel: { channel_id: set(publisher_id, ...) }"""
  subscriptions: dict[str, set[str]] = field(default_factory=dict)
  """Subscribers associated with a channel: { channel_id: set(subscriber_id, ...) }"""

  def add_channel(self, channel_id: str):
    """Add a Channel to the Registry"""
    if channel_id in self.channels: raise ValueError(channel_id)
    self.channels[channel_id] = ItemLog()
    self.subscriptions[channel_id] = set()
    self.publishments[channel_id] = set()
  
  def remove_channel(self, channel_id: str):
    """Remove a Channel from the Registry"""
    if channel_id not in self.channels: raise ValueError(channel_id)
    msg_log = self.channels.pop(channel_id)
    for msg_id in msg_log:
      del self.messages[msg_id]
    del self.subscriptions[channel_id]
    del self.publishments[channel_id]
  
  def add_publisher(self, channel_id: str, client_id: str):
    """Add a publisher mapping to a Channel in the Registry"""
    if channel_id not in self.channels: raise ValueError(channel_id)
    self.publishments[channel_id].add(client_id)
  
  def remove_publisher(self, channel_id: str, client_id: str):
    """Remove a publisher mapping from a Channel in the Registry"""
    if channel_id not in self.channels: raise ValueError(channel_id)
    self.publishments[channel_id].remove(client_id)
  
  def add_subscriber(self, channel_id: str, client_id: str):
    """Add a subscriber mapping to a Channel in the Registry"""
    if channel_id not in self.channels: raise ValueError(channel_id)
    self.subscriptions[channel_id].add(client_id)
  
  def remove_subscriber(self, channel_id: str, client_id: str):
    """Remove a subscriber mapping from a Channel in the Registry"""
    if channel_id not in self.channels: raise ValueError(channel_id)
    self.subscriptions[channel_id].remove(client_id)
  
  async def add_message(self, msg: Message) -> Error | NO_ERROR_T:
    """Add a Message in the Registry adding it to the proper Channel Log."""
    msg_id = msg['metadata']['id']
    pub_id = msg['metadata']['sender']
    if msg_id in self.messages: return { 'kind': 'conflict', 'msg': f'{msg_id} already recorded' }
    channel_id = msg['metadata']['channel']
    if channel_id not in self.channels: return { 'kind': 'missing', 'msg': f'{channel_id} not registered' }
    if pub_id not in self.publishments[channel_id]: return { 'kind': 'missing', 'msg': f'{pub_id} is not a publisher of {channel_id}' }
    self.messages[msg_id] = msg
    await self.channels[channel_id].push(msg_id)
    return NO_ERROR

  async def remove_message(self, msg_id: str) -> Error | NO_ERROR_T:
    """Remove a Message from the Registry scrubbing it from it's channel log"""
    if msg_id not in self.messages: return { 'kind': 'missing', 'msg': f'{msg_id} not recorded' }
    msg = self.messages[msg_id]
    channel_id = msg['metadata']['channel']
    if channel_id in self.channels:
      ... # TODO: Remove the message from the channel log
      _raise_TODO('TODO: Message Removal from Channel Log')


### Protocols to be implemented by Backends

class ConsumerInterface(Protocol):
  @property
  def id(self) -> str: ...
  async def connect(self, broker: BrokerInterface) -> Error | NO_ERROR_T: ...
  async def disconnect(self) -> Error | NO_ERROR_T: ...
  def subscribe(self, channel_id: str, msg_filter: Callable[[str, OBJ, userdata_t], bool] | None = None) -> Error | NO_ERROR_T: ...
  def unsubscribe(self, channel_id: str) -> Error | NO_ERROR_T: ...
  async def listen(self, channel_id: str) -> tuple[Error | NO_ERROR_T, unpacked_msg_t]: ...

class ProducerInterface(Protocol):
  @property
  def id(self) -> str: ...
  async def connect(self, broker: BrokerInterface) -> Error | NO_ERROR_T: ...
  async def disconnect(self) -> Error | NO_ERROR_T: ...
  async def connect(self, broker: BrokerInterface) -> Error | NO_ERROR_T: ...
  async def publish(self, channel_id: str, payload: OBJ, userdata: userdata_t) -> Error | NO_ERROR_T: ...

class BrokerInterface(Protocol):
  @property
  def id(self) -> str: ...
  async def connect_producer(self, client_id: str) -> tuple[Error | NO_ERROR_T,  msg_push_t | None]: ...
  async def connect_consumer(self, client_id: str, consumer_rx: msg_push_t, congestion_cfg: CongestionConfig) -> Error | NO_ERROR_T: ...
  async def disconnect(self, client_id: str) -> Error | NO_ERROR_T: ...
  def announce_publisher(self, client_id: str, channel_id: str) -> Error | NO_ERROR_T: ...
  def revoke_publisher(self, client_id: str, channel_id: str) -> Error | NO_ERROR_T: ...
  def add_subscription(self, client_id: str, channel_id: str, msg_filter: msg_filter_t) -> Error | NO_ERROR_T: ...
  def cancel_subscription(self, client_id: str, channel_id: str) -> Error | NO_ERROR_T: ...
