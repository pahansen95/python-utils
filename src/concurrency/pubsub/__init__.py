"""
Implements basic Pub/Sub functionality

- Single Publisher, Multiple Subscriber Architecture
- Channels represent logical groupings of messages that can be published or subscribed to; ie. A Topic of Interest.
- Messages are associated w/ UTF-8 Encoded Keys (herein Channel Keys) that identify a particular stream
  - Key Sublogic is left up to the implementation; (ex. Hierarhcal Keys)
- Messages are encoded as JSON Object including metadata & payload.
  - Metadata is encoded as a JSON Object
  - Payloads are encoded as a JSON Object
- The Publisher maintains a registry of known channel keys & current subscriber set
  - The set of channel keys are maintained by the Publisher; Subscribers cannot CRUD channel keys 
  - Channel Keys exist regardless of Subscriber count
  - Each Channel Key maintains a chronological log of published messages
    - Log is bounded or unbounded as configured at runtiome
- The Subscriber subscribes to a Channel Key from a Publisher.
  - Responsible for maintain integrity of it's local log state
- Subscriber - Publisher Connection State is backend dependant; the protocol provides no garuantees
- Messages & Operations
  - `PUB` Msg - A Message detailing some chronological event on a Channel Key
  - `publish` Op - A Publisher sends a `PUB` Message to a Subscriber; delivered at Most Once, least effort
  - `LOG` Msg - A Message detailing the contents of a Log for a Channel Key
  - `SUB` Msg - A Message detailing a Channel Key & `n` latest messages in the log
  - `subscription` Op - A Subscriber sends a `SUB` Message to a Publisher; expects a `LOG` Msg as a response
"""
from __future__ import annotations
from typing import TypedDict, NotRequired, TypeVar, Generic, Literal
from collections.abc import Callable, Awaitable, Coroutine
from collections import deque
from abc import abstractmethod
from dataclasses import dataclass, field, KW_ONLY
from utils.concurrency.log import ItemLog, Log
from utils.errors import Error, NO_ERROR_T, NO_ERROR
import orjson, asyncio, time
from loguru import logger

OBJ = TypeVar('OBJ', bound=dict)
userdata_t = dict[str, str]

class Message(Generic[OBJ], TypedDict):
  """A Datastructure representing an `inflight` payload."""
  metadata: Message.Metadata
  """Metadata associated w/ a Message"""
  payload: OBJ
  """A JSON Encodable dict of arbitrary content"""
  class Metadata(TypedDict):
    name: str
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
    metadata = { 'name': f"msg-{now:x}", 'timestamp': now, 'userdata': userdata } | kwargs
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

msglog_t = ItemLog[Message[OBJ]]
"""TypeHint: A Log of Messages"""
unpacked_msg_t = tuple[OBJ, userdata_t]
"""TypeHint: An Unpacked Message Object"""
rxq_t = Callable[[str, str], Awaitable[tuple[Error | NO_ERROR_T, unpacked_msg_t | None]]]
"""A Receiver Queue Async Interface: A Factory function that given (client ID, channel ID) returns an Awaitable producing a new message or any errors encountered"""
txq_t = Callable[[str, str, OBJ, userdata_t], Awaitable[Error | NO_ERROR_T]]
"""A Transmitter Queue Async Interface: A factory function that given (client ID, channel ID, Payload, Userdata) returns an Awaitable, which upon completion garuntees the message was transmitted or an error was encountered."""

def _create_event(state: bool) -> asyncio.Event:
  event = asyncio.Event()
  if state: event.set()
  return event

class _MsgLogCtx(TypedDict):
  """The Stateful Context of the Message Log"""
  events: _MsgLogCtx.Events
  alerts: _MsgLogCtx.Alerts

  class Events(TypedDict):
    empty: asyncio.Event = field(default_factory=lambda: _create_event(True))
    """Is the Item Log Empty"""
    not_empty: asyncio.Event = field(default_factory=lambda: _create_event(False))
    """Is the Item Log not empty"""
    full: asyncio.Event = field(default_factory=lambda: _create_event(False))
    """Is the Item Log Full"""
    not_full: asyncio.Event = field(default_factory=lambda: _create_event(True))
    """Is the Item Log not Full"""
  
  class Alerts(TypedDict):
    push: asyncio.Queue = field(default_factory=asyncio.Queue)
    """An Item was pushed onto the Log, value is either 'head' or 'tail'"""
    pop: asyncio.Queue = field(default_factory=asyncio.Queue)
    """An Item was popped from the Log, value is either 'head' or 'tail'"""


# @dataclass
# class MessageLog(Log[Message[OBJ]]):
#   """A MessageLog is an implementation of the Log Protocol based on the ItemLog Implementation"""

#   log: deque[Message[OBJ]] = field(default_factory=deque)
#   """The Item Log"""
#   mutex: asyncio.Lock = field(default_factory=asyncio.Lock)
#   """The Async Lock for Mutually Exclusive Access to the Log"""
#   _ctx: _MsgLogCtx = field(default_factory=dict)

#   async def wait_until(self, event: Literal['empty', 'not_empty', 'full', 'not_full']) -> Literal[True]:
#     """Wait until an event occurs"""
#     if event == 'full' and self.log.maxlen is None: raise ValueError('impossible to wait for an unbounded log to become full')
#     return await self._ctx['events'][event].wait()
  
#   async def peek(self, block: bool = True, mode: Literal['head', 'tail'] = 'head') -> Message[OBJ] | None:
#     """Peek at the head or tail of the Message Log"""
#     while True:
#       # Wait for an Item or short circuit
#       if block: await self._ctx['events']['not_empty'].wait()
#       elif not self._ctx['events']['not_empty'].is_set(): return None # Short Circuit if non-blocking
#       # Return the head of the log
#       async with self.mutex:
#         if not self._ctx['events']['not_empty'].is_set(): continue # Protect against Race Conditions
#         if mode == 'head': return self.log[0]
#         elif mode == 'tail': return self.log[-1]
#         else: raise ValueError(f"Invalid mode: {mode}")
  
#   async def pop(self, block: bool = True, mode: Literal['head', 'tail'] = 'head') -> Message[OBJ] | None:
#     """Pop a Message from the head or tail of the Message Log; if non-blocking return None if no message is available"""
#     while True:
#       # Wait for an Item or Short Circuit
#       if block: await self._ctx['events']['not_empty'].wait() # Wait for an item to be pushed
#       elif not self._ctx['events']['not_empty'].is_set(): return None # Short Circuit if non-blocking
#       # Pop the head of the log
#       async with self.mutex:
#         if self._ctx['events']['empty'].is_set(): continue # Protect against Race Conditions
#         if mode == 'head': item = self.log.popleft()
#         elif mode == 'tail': item = self.log.pop()
#         else: raise ValueError(f"Invalid mode: {mode}")
#         self._ctx['alerts']['pop'].put_nowait(mode)
#         self._ctx['events']['full'].clear()
#         self._ctx['events']['not_full'].set()
#         if len(self.log) == 0: # We popped the last item in the log
#           self._ctx['events']['empty'].set()
#           self._ctx['events']['not_empty'].clear()
#         return item

  
#   async def push(self, item: Message[OBJ], block: bool = True, mode: Literal['head', 'tail'] = 'tail') -> None | Message[OBJ]:
#     """Push a Message onto the head or tail of the Message Log; if non-blocking return back the message if the log is full"""
#     while True:
#       # Wait for a slot or shortcircuit
#       if not block and self._ctx['events']['full'].is_set(): return item # Short Circuit
#       elif block and self._ctx['events']['full'].is_set(): await self._ctx['events']['not_full'].wait() # Wait until the log has a slot

#       # Pop the head of the log
#       async with self.mutex:
#         if self._ctx['events']['full'].is_set(): continue # Protect against Race Conditions
#         if mode == 'tail': self.log.append(item)
#         elif mode == 'head': self.log.appendleft(item)
#         else: raise ValueError(f"Invalid mode: {mode}")
#         self._ctx['alerts']['push'].put_nowait(mode)
#         self._ctx['events']['not_empty'].set()
#         self._ctx['events']['empty'].clear()
#         if self.log.maxlen is not None and len(self.log) >= self.log.maxlen: # We pushed into the last avialable slot on a bounded queue
#           self._ctx['events']['full'].set()
#           self._ctx['events']['not_full'].clear()
#         return None

@dataclass
class ChannelRegistry:
  messages: dict[str, Message] = field(default_factory=dict)
  """All currently recorded Messages in the Registry"""
  channels: dict[str, ItemLog[Message[OBJ]]] = field(default_factory=dict)
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
    if msg['metadata']['name'] in self.messages: return { 'kind': 'conflict', 'msg': f'{msg["metadata"]["name"]} already recorded' }
    channel_id = msg['metadata']['channel']
    if channel_id not in self.channels: return { 'kind': 'missing', 'msg': f'{channel_id} not registered' }
    if msg['metadata']['sender'] not in self.publishments[channel_id]: return { 'kind': 'missing', 'msg': f'{msg["metadata"]["sender"]} is not a publisher of {channel_id}' }
    self.messages[msg['metadata']['name']] = msg
    await self.channels[channel_id].push(msg) # TODO: Should we refactor MessageLogs to use just the IDs instead of the full message? 
    return NO_ERROR

  async def remove_message(self, msg_id: str) -> Error | NO_ERROR_T:
    """Remove a Message from the Registry scrubbing it from it's channel log"""
    if msg_id not in self.messages: return { 'kind': 'missing', 'msg': f'{msg_id} not recorded' }
    msg = self.messages[msg_id]
    channel_id = msg['metadata']['channel']
    if channel_id in self.channels:
      raise NotImplementedError # TODO: Remove the message from the channel log

# @dataclass
# class OLD_ChannelRegistry:
#   channels: dict[str, msglog_t] = field(default_factory=dict)
#   """The known set of Channels & their current Log of Messages"""
#   publishments: dict[str, set[str]]
#   """Publishers associated with a channel: { channel_id: set(publisher_id, ...) }"""
#   subscriptions: dict[str, set[str]]
#   """Subscribers associated with a channel: { channel_id: set(subscriber_id, ...) }"""

#   def register(self, channel_id: str, log: msglog_t | None = None):
#     if channel_id in self.channels: return { 'kind': 'conflict', 'msg': f'{channel_id} already registered' }
#     self.channels[channel_id] = log or ItemLog()
#     self.subscriptions[channel_id] = set()
#     self.publishments[channel_id] = set()
  
#   def deregister(self, channel_id: str) -> msglog_t:
#     if channel_id not in self.channels: return { 'kind': 'missing', 'msg': f'{channel_id} not registered' }
#     del self.subscriptions[channel_id]
#     del self.publishments[channel_id]
#     return self.channels.pop(channel_id)
  
#   def add_publishment(self, channel_id: str, pub_id: str):
#     if channel_id not in self.channels: raise ValueError(channel_id)
#     self.publishments[channel_id].add(pub_id)
  
#   def remove_publishment(self, channel_id: str, pub_id: str):
#     if channel_id not in self.channels: raise ValueError(channel_id)
#     self.publishments[channel_id].remove(pub_id)
  
#   def add_subscription(self, channel_id: str, sub_id: str):
#     if channel_id not in self.channels: raise ValueError(channel_id)
#     self.subscriptions[channel_id].add(sub_id)
  
#   def remove_subscription(self, channel_id: str, sub_id: str):
#     if channel_id not in self.channels: raise ValueError(channel_id)
#     self.subscriptions[channel_id].remove(sub_id)

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

class _PubState(TypedDict):
  rx_queue: ItemLog[Message[OBJ]]
  """The Message Reception queue for the publishing client"""
  disconnect: asyncio.Event
  """Tracks if the Client has disconnected"""

class _SubState(TypedDict):
  delivery_queue: ItemLog[str]
  """The Message Delivery Queue for the subscribing client"""
  tx_queue: ItemLog[str]
  """The Message Transmission Queue for the subscribing client"""
  listener: Callable[..., Coroutine[Error | NO_ERROR_T, None, None]]
  """The client's listener used to push messages to the client; should return an error if the message fails to deliver or would block indefinitely."""
  filters: dict[str, Callable[..., bool]]
  """The optional per channel filters for the subscriber"""
  disconnect: asyncio.Event
  """Tracks if the Client has disconnected"""

class _BrokerCtx(TypedDict):
  producers: dict[str, _PubState]
  """Connected Producers & their Publishing State"""
  consumers: dict[str, _SubState]
  """Connected Consumers & their Subscribing State"""
  tasks: dict[str, asyncio.Task]
  """Scheduled Concurrent Broker Tasks"""
  multicast_queue: NotRequired[ItemLog[str]]
  """The shared multicast queue for all channels"""

  @staticmethod
  def factory(**kwargs) -> _BrokerCtx:
    return {
      'producers': {},
      'consumers': {},
      'tasks': {},
    } | kwargs

@dataclass
class Broker:
  """Manage State & Lifecycle of message brokering between Producers & Consumers.

## Message Broker Overview

The Message Broker requires explicit state declaration from the developer and does not infer intentions. Consumers subscribe to channels (topics), and Producers must announce their intent to publish on a channel. Both Consumers and Producers must explicitly connect and disconnect from the Broker to subscribe or publish to channels. Here are the key details:

### Channel Management
- **Consumers**: Subscribe and unsubscribe from channels.
- **Producers**: Announce their intent to publish and revoke it when done.
- **Channel Declaration**: An error is raised if a subscription is attempted on a non-existent channel. At least one Producer must announce a channel before any Consumer can subscribe.

### Client Connection Management
- **Connection**: When connecting, the broker creates client-specific resources and spawns necessary client-specific processing loops.
- **Disconnection**: Upon disconnection, all client-specific resources are purged, and client-specific processing loops are canceled.

### Message Brokering Mechanism
Published messages on a channel are multicast to all channel subscribers. This process is implemented through multiple concurrent loops to maintain concurrency and prevent a single subscriber from blocking others:

1. **Multicast Loop**: 
  - Replicates published messages to respective subscriptions.
  - Pushes messages into the subscription's unbounded caching queue.

2. **Client Delivery Evaluation Loop**:
  - Pops a message from the Caching Queue
  - Evaluates if a message should be delivered to a client based on filter validation and client congestion.
  - Pushes the message into the bounded Delivery Queue

3. **Client Transmission (TX) Loop**:
  - Pops a message from the Delivery Queue
  - Transmits the message to the client.

### Loop Management
- **Subscription**: When a client subscribes to at least one channel, both a client delivery evaluation loop and a client transmission loop are spawned as tasks.
- **Unsubscription**: When a client unsubscribes from all channels, these loops are canceled.
- **Multicast Loop**: Spawns when there is at least one publishing announcement and is canceled when all announcements are revoked.

### Delivery Evaluation
Currently, delivery evaluation is basic:
- **Client Filters**: Provided by the client on subscription & applied against channel id, payload & userdata.
- **Delivery Queue**: If full, messages may either be dropped or delivery may be blocked as configured; when blocked new messages are retained in the unbounded caching queue.

### Asynchronous Handling
This broker implementation uses a single-threaded asyncio model:
- **Rx & Tx**: Handled by pushing and popping messages between ItemLogs.
- **Safety & Runtime Checks**: Wrapped in async functions to ensure safe operations.

### Consumer Unsubscription
When a consumer unsubscribes from a channel:
- **Message Purging**: All messages for that consumer on that channel are purged.
- **Inactive Status**: If unsubscribed from all channels, the consumer is considered "inactive" and does not participate in message multicasting.
  """

  id: str = field(default_factory=lambda: f"broker-{time.monotonic_ns():x}")
  """The Unique Identity of a Broker"""
  channel_registry: ChannelRegistry = field(default_factory=ChannelRegistry)
  """The current state of channels"""
  _: KW_ONLY
  _ctx: _BrokerCtx = field(default_factory=_BrokerCtx.factory)
  """Stateful Context of the Broker"""

  async def _setup_producer(self, client_id: str) -> Callable[[Message[OBJ]], Coroutine[None, None, Error | NO_ERROR_T]]:

    # Setup Broker Multicasting Loop on the first publisher
    task_key = f"loop_multicast"
    if task_key not in self._ctx['tasks']:
      if not 'multicast_queue' in self._ctx: self._ctx['multicast_queue'] = ItemLog()
      self._ctx['tasks'][task_key] = asyncio.create_task(
        self._loop_multicast(
          self._ctx['multicast_queue']
        )
      )

    if client_id in self._ctx['producers']: raise NotImplementedError('Client Connection Refresh')
    self._ctx['producers'][client_id] = {
      'rx_queue': ItemLog(),
      'disconnect': asyncio.Event(),
    }

    # Schedule the client Rx Loop
    task_key = f"loop_{client_id}_rx"
    if task_key in self._ctx['tasks']:
      raise NotImplementedError # TODO Cancel the Current Task
    self._ctx['tasks'][task_key] = asyncio.create_task(
      self._loop_rx(
        client_id=client_id,
        rx_queue=self._ctx['producers'][client_id]['rx_queue'],
        multicast_queue=self._ctx['multicast_queue']
      )
    )

    # Schedule the shared client disconnect wait task
    task_key = f"wait_{client_id}_disconnect"
    if task_key in self._ctx['tasks']:
      raise NotImplementedError # TODO Cancel the Current Task
    self._ctx['tasks'][task_key] = asyncio.create_task(self._ctx['producers'][client_id]['disconnect'].wait())

    return self._publish_message_coro_factory(
      client_id,
      self._ctx['producers'][client_id]['rx_queue']
    )
  
  async def _teardown_producer(self, client_id: str):
    if client_id not in self._ctx['producers']: raise ValueError(f'{client_id} was never connected')
    if client_id in self.channel_registry.publishments:
      raise NotImplementedError # TODO: Revoke all channel publishments
    logger.trace(f"Marking Client {client_id} as Disconnected")
    self._ctx['producers'][client_id]['disconnect'].set()
    logger.trace(f"Cancelling Client {client_id} loops")
    for task_id in (
      f"loop_{client_id}_rx",
    ): self._ctx['tasks'][task_id].cancel()
    logger.trace(f"Waiting for Client {client_id} loops to finish")
    await asyncio.wait((
      self._ctx['tasks'][task_id]
      for task_id in (
        f"loop_{client_id}_rx",
      )
    ))
    assert self._ctx['tasks'][f'wait_{client_id}_disconnect'].done()
    logger.trace(f"Cleaning up Client {client_id} resources")
    del self._ctx['producers'][client_id]
    logger.trace(f"Client {client_id} Disconnected")

    # NOTE: If this is the last producer, we need to cleanup the Broker's Multicast Loop
    if len(self._ctx['producers']) == 0:
      logger.trace("All Producers Disconnected; Cancelling Broker Multicast Loop")
      self._ctx['tasks']['loop_multicast'].cancel()
      await asyncio.wait((
        self._ctx['tasks'][task_id]
        for task_id in (
          'loop_multicast',
        )
      ))
      logger.trace("Broker Multicast Loop Cancelled")
  
  async def _setup_consumer(self,
    client_id: str,
    listener: Callable[..., Coroutine],
    congestion_cfg: CongestionConfig,
  ):
    if client_id in self._ctx['consumers']: raise NotImplementedError('Client Connection Refresh')
    self._ctx['consumers'][client_id] = {
      'delivery_queue': ItemLog(),
      'tx_queue': ItemLog(),
      'listener': listener,
      'disconnect': asyncio.Event(),
      'filters': {},
    }

    # Schedule client Delivery Eval Loop
    task_key = f"loop_{client_id}_delivery_eval"
    if task_key in self._ctx['tasks']:
      raise NotImplementedError # TODO Cancel the Current Task
    self._ctx['tasks'][task_key] = asyncio.create_task(
      self._loop_delivery_eval(
        client_id=client_id,
        delivery_queue=self._ctx['consumers'][client_id]['delivery_queue'],
        tx_queue=self._ctx['consumers'][client_id]['tx_queue'],
      )
    )
    
    # Schedule client TX Loop
    task_key = f"loop_{client_id}_tx"
    if task_key in self._ctx['tasks']:
      raise NotImplementedError # TODO Cancel the Current Task
    self._ctx['tasks'][task_key] = asyncio.create_task(
      self._loop_tx(
        client_id=client_id,
        listener=self._ctx['consumers'][client_id]['listener'],
        tx_queue=self._ctx['consumers'][client_id]['tx_queue'],
        cfg=congestion_cfg,
      )
    )

    # Schedule the shared client disconnect wait task
    task_key = f"wait_{client_id}_disconnect"
    if task_key in self._ctx['tasks']:
      raise NotImplementedError # TODO Cancel the Current Task
    self._ctx['tasks'][task_key] = asyncio.create_task(self._ctx['consumers'][client_id]['disconnect'].wait())

  async def _teardown_consumer(self, client_id: str):
    if client_id not in self._ctx['consumers']: raise ValueError(f'{client_id} was never connected')
    if client_id in self.channel_registry.subscriptions:
      raise NotImplementedError # TODO: Unsubscribe from all channels
    logger.trace(f"Marking Client {client_id} as Disconnected")
    self._ctx['consumers'][client_id]['disconnect'].set()
    logger.trace(f"Cancelling Client {client_id} loops")
    for task_id in (
      f"loop_{client_id}_delivery_eval",
      f"loop_{client_id}_tx",
    ): self._ctx['tasks'][task_id].cancel()
    logger.trace(f"Waiting for Client {client_id} loops to finish")
    await asyncio.wait((
      self._ctx['tasks'][task_id]
      for task_id in (
        f"loop_{client_id}_delivery_eval",
        f"loop_{client_id}_tx",
      )
    ))
    assert self._ctx['tasks'][f'wait_{client_id}_disconnect'].done()
    logger.trace(f"Cleaning up Client {client_id} resources")
    del self._ctx['consumers'][client_id]
    logger.trace(f"Client {client_id} Disconnected")

  def _add_channel_publisher(self, client_id: str, channel_id: str):
    if not channel_id in self.channel_registry.channels: self.channel_registry.add_channel(channel_id)    
    if client_id not in self.channel_registry.publishments: self.channel_registry.add_publisher(channel_id, client_id)
  
  def _remove_channel_publisher(self, client_id: str, channel_id: str):
    if not channel_id in self.channel_registry.channels: raise ValueError(f'{channel_id} was never registered')
    if client_id in self.channel_registry.publishments: self.channel_registry.remove_publisher(channel_id, client_id)
    if len(self.channel_registry.publishments[channel_id]) == 0: self.channel_registry.remove_channel(channel_id)

  def _publish_message_coro_factory(self, client_id: str, rx_queue: msglog_t) -> Callable[[Message[OBJ]], Coroutine[None, None, Error | NO_ERROR_T]]:
    offline_err = { 'kind': 'offline', 'message': f"Client `{client_id}` is offline" }
    wait_for_disconnect: asyncio.Task = self._ctx['tasks'][f"wait_{client_id}_disconnect"]
    async def publish_message(msg: Message[OBJ]) -> Error | NO_ERROR_T:
      logger.trace(f'Publishing Message for Client {client_id}')
      if client_id not in self._ctx['producers']: return offline_err
      channel_id = msg['metadata']['channel']
      if channel_id not in self.channel_registry.channels: return { 'kind': 'missing', 'message': f"Channel `{channel_id}` is not registered" }
      elif client_id not in self.channel_registry.publishments[channel_id]: return { 'kind': 'missing', 'message': f"Client `{client_id}` did not announce it's publishment to `{channel_id}`" }
      logger.trace(f'Scheduling Task to push message into the Rx Queue for Client {client_id}')
      push_task = asyncio.create_task(rx_queue.push(msg))
      logger.trace(f'Waiting for either the message to be pushed or Client {client_id} to disconnect')
      await asyncio.wait((
        push_task,
        wait_for_disconnect
      ), return_when=asyncio.FIRST_COMPLETED)
      if self._ctx['producers'][client_id]['disconnect'].is_set():
        logger.trace(f'Client {client_id} disconnected before the message was pushed; cancelling the push task')
        push_task.cancel()
        return offline_err
      assert push_task.done()
      logger.trace(f'Pushed Message into the Rx Queue for Client {client_id}')
      return NO_ERROR
    
    logger.trace(f'Created Publish Message Coroutine Factory for Client {client_id}')
    return publish_message
  
  def _add_channel_subscriber(self,
    client_id: str, channel_id: str,
    msg_filter: Callable[..., bool] | None,
  ):
    """Setup a Client's Channel Subscription"""
    if not channel_id in self.channel_registry.channels: raise ValueError(f'{channel_id} must first be registered for publishment before it can be subscribed to')
    if client_id not in self.channel_registry.subscriptions: self.channel_registry.add_subscriber(channel_id, client_id)
    if msg_filter is not None: self._ctx['consumers'][client_id]['filters'][channel_id] = msg_filter

  def _remove_channel_subscriber(self, client_id: str, channel_id: str):
    if not channel_id in self.channel_registry.channels: raise ValueError(f'{channel_id} was never registered')
    if client_id in self.channel_registry.subscriptions: self.channel_registry.remove_subscriber(channel_id, client_id)

  ### Broker Loops ###

  async def _loop_rx(self,
    client_id: str,
    rx_queue: msglog_t,
    multicast_queue: ItemLog[str],
  ):
    """Recieve messages from a publishing source, record them & then forward them to the multicast loop for distribution."""
    pop = False
    while True:
      if pop:
        logger.trace(f"Poppping a Message from the Rx Queue for Client {client_id}")
        await rx_queue.pop() # Drop the Message since we processed it
        pop = False

      if self._ctx['producers'][client_id]['disconnect'].is_set():
        raise NotImplementedError('Client Disconnected')

      logger.trace(f"Peeking at the Rx Queue for Client {client_id}")
      msg, pop = (await rx_queue.peek(), True)
      channel_id = msg['metadata']['channel']
      assert client_id == msg['metadata']['sender']

      logger.trace(f"Adding Message to the Channel Registry for Client {client_id}")
      err = await self.channel_registry.add_message(msg)
      if err is not NO_ERROR:
        raise NotImplementedError(f"{err['kind']}: {err['msg']}")
      else: await multicast_queue.push(msg['metadata']['name'])

  async def _loop_multicast(
    self,
    multicast_queue: ItemLog[str],
  ):
    """Distribute Messages to their respective channel Subscriptions"""
    pop = False
    while True:
      if pop:
        logger.trace("Popping a Message from the Broker's Multicast Queue")
        await multicast_queue.pop() # Deferrred: Drop the Message since it's been processed
        pop = False
      logger.trace("Peeking at the Broker's Multicast Queue")
      (msg_id, pop) = (await multicast_queue.peek(), True)
      channel_id = self.channel_registry.messages[msg_id]['metadata']['channel']
      # await asyncio.gather(*(
      #   self._ctx['consumers'][client_id]['delivery_queue'].push(msg_id)
      #   for client_id in self.channel_registry.subscriptions[channel_id]
      # ))
      for client_id in self.channel_registry.subscriptions[channel_id]:
        logger.trace(f"Pushing Message {msg_id} to the Delivery Queue for Client {client_id}")
        await self._ctx['consumers'][client_id]['delivery_queue'].push(msg_id)
  
  async def _loop_delivery_eval(self,
    client_id: str,
    delivery_queue: ItemLog[str],
    tx_queue: ItemLog[str],
  ):
    """Evaluate if a Message should be delivered to a Client
    
    This loop is 1 to 1 w/ each subscribing Client so can handled per client congestion, buffering & batching.
    """

    pop = False
    while True:
      if pop: # Deferred Message Pop
        logger.trace(f"Poppping a Message from the Delivery Queue for Client {client_id}")
        _msg_id = await delivery_queue.pop(block=False) # Drop the Message since we evaluated it
        assert msg_id == _msg_id
        pop = False
      
      logger.trace(f"Peeking at the Delivery Queue for Client {client_id}")
      msg_id, pop = (await delivery_queue.peek(), True)
      channel_id = self.channel_registry.messages[msg_id]['metadata']['channel']
      msg_filter = self._ctx['consumers'][client_id]['filters'].get(channel_id)

      ### State Checks ###
      logger.trace(f"Evaluating Message {msg_id} for Delivery to Client {client_id}")
      if channel_id not in self.channel_registry.channels:
        raise NotImplementedError # Not a valid channel
      elif client_id not in self.channel_registry.subscriptions[channel_id]:
        raise NotImplementedError # Client isn't subscribed to this channel
      elif self._ctx['consumers'][client_id]['disconnect'].is_set():
        raise NotImplementedError # Client is currently disconnected
      elif msg_filter is not None and not msg_filter(
        channel_id,
        self.channel_registry.messages[msg_id]['payload'],
        self.channel_registry.messages[msg_id]['metadata'].get('userdata', {})
      ):
        raise NotImplementedError # Client filtered out the message
      else:
        logger.trace(f"Pushing Message {msg_id} to the Tx Queue for Client {client_id}")
        await tx_queue.push(msg_id) # The Message is valid; handoff to the Tx Queue

  async def _loop_tx(self,
    client_id: str,
    listener: Callable[[Message[OBJ]], Coroutine[None, None, Error | NO_ERROR_T]],
    tx_queue: ItemLog[str],
    cfg: CongestionConfig,
  ):
    """Transmit the Message to the Client via the provided listener"""

    async def _tx_message(msg_id: str) -> Error | NO_ERROR_T:
      logger.trace(f"Retrieving Message {msg_id} from the Channel Registry")
      msg = self.channel_registry.messages[msg_id]
      tx_msg = {
        'metadata': msg['metadata'] | {
          'recipient': client_id
        },
        'payload': msg['payload']
      }
      logger.trace(f"Transmitting Message {msg_id} to Client {client_id}")
      return await listener(tx_msg)

    congestion_mode = cfg['mode']
    buffer_limit = cfg.get('opts', {}).get('limit')
    buffer_pop_from = cfg.get('opts', {}).get('direction', 'head')

    still_congested: asyncio.Event
    mode: Literal['normal', 'congested', 'drain'] = 'normal'
    buffer: deque[str] = deque(maxlen=buffer_limit)
    pop = False
    while True:
      if pop:
        logger.trace(f"Poppping a Message from the Tx Queue for Client {client_id}")
        await tx_queue.pop()
        pop = False

      logger.trace(f"Peeking at the Tx Queue for Client {client_id}")
      msg_id, pop = (await tx_queue.peek(), True)

      if mode == 'normal':
        ### When in Normal Mode, we just attempt to transmit the message ###
        err = await _tx_message(msg_id) # TODO: We need to duplicate the message.
        if err is not NO_ERROR:
          if err['kind'] == 'congested':
            logger.trace(f'Client {client_id} is congested; transitioning to Congested Mode')
            still_congested = err['still_congested'] # TODO: Implement
            mode = 'congested'
            buffer.append(msg_id)
          else: raise NotImplementedError(err['kind'])
      elif mode == 'congested':
        ### While in Congested Mode, we either buffer or drop messages
        if not still_congested.is_set():
          logger.trace(f'Client {client_id} is no longer congested; transitioning to Drain Mode')
          mode = 'drain'
          pop = False
        elif congestion_mode == 'buffer':
          logger.trace(f'Buffering Message {msg_id} for Client {client_id}')
          if buffer_limit is not None and len(buffer) >= buffer_limit:
            logger.trace(f'Buffer is full; popping from {buffer_pop_from} to make room')
            if buffer_pop_from == 'head': buffer.popleft()
            elif buffer_pop_from == 'tail': buffer.pop()
            else: raise ValueError(buffer_pop_from)
          buffer.append(msg_id)
        elif congestion_mode == 'drop':
          raise NotImplementedError # Drop the messae
        else: raise ValueError(congestion_mode)
      elif mode == 'drain':
        ### While in Drain Mode, we attempt to first deliver all messages in the buffer ###
        logger.trace(f'Draining the Buffer for Client {client_id}')
        pop = False
        idx = 0
        for msg_id in buffer:
          err = await _tx_message(msg_id)
          if err is not NO_ERROR:
            still_congested = err['still_congested'] # TODO: Implement
            mode = 'congested'
            break
          else: idx += 1
        buffer = buffer[idx:]
        if len(buffer) == 0: mode = 'normal'
      else: raise ValueError(mode)
      
  ### Client Interface ###

  async def connect_producer(self, client_id: str) -> tuple[Error | NO_ERROR_T, Callable[[Message[OBJ]], Coroutine[None, None, Error | NO_ERROR_T]] | None]:
    """Asynchronously connects a Producer Client to the Broker
    Returns a Coroutine Factory the publisher may use to push a message into the Broker.
    """
    if client_id in self._ctx['consumers']: return { 'kind': 'conflict', 'message': f"Client `{client_id}` is already connected as a Consumer" }, None
    publisher_tx = await self._setup_producer(client_id)
    assert client_id in self._ctx['producers']

    return NO_ERROR, publisher_tx
  
  async def connect_consumer(self,
    client_id: str,
    consumer_rx: Callable[[Message[OBJ]], Coroutine[None, None, Error | NO_ERROR_T]],
    congestion_cfg: CongestionConfig = None
  ) -> Error | NO_ERROR_T:
    """Asynchronously connects a Conumer Client to the Broker.
    The consumer must provide a Coroutine Factory the broker can use to push a message to the client.
    """
    if client_id in self._ctx['producers']: return { 'kind': 'conflict', 'message': f"Client `{client_id}` is already connected as a Producer" }
    await self._setup_consumer(client_id, consumer_rx, congestion_cfg or {
      'mode': 'buffer',
      'opts': {
        'limit': 100,
        'direction': 'head'
      }
    })
    assert client_id in self._ctx['consumers']
    return NO_ERROR
  
  async def disconnect(self, client_id: str) -> Error | NO_ERROR_T:
    """Asynchronously disconnects a Client from the Broker"""
    if client_id in self._ctx['producers']: await self._teardown_producer(client_id)
    elif client_id in self._ctx['consumers']: await self._teardown_consumer(client_id)
    else: return { 'kind': 'missing', 'message': f"Client `{client_id}` is not connected" }

    return NO_ERROR
  
  def announce_publisher(self, client_id: str, channel_id: str) -> Error | NO_ERROR_T:
    """Announces a Client will publish to the channel
    Returns a Coroutine Factory the publisher may use to push a message into the Broker.
    """
    if client_id not in self._ctx['producers']:
      if client_id in self._ctx['consumers']: return { 'kind': 'conflict', 'message': f"Client `{client_id}` connected as a Consumer; it cannot announce publishment" }
      else: return { 'kind': 'missing', 'message': f"Client `{client_id}` is not connected" }
    
    self._add_channel_publisher(client_id, channel_id)

    return NO_ERROR
  
  def revoke_publisher(self, client_id: str, channel_id: str) -> Error | NO_ERROR_T:
    """Revoke a previous publisher announcement"""
    if client_id not in self._ctx['producers']:
      if client_id in self._ctx['consumers']: return { 'kind': 'conflict', 'message': f"Client `{client_id}` connected as a Consumer; it cannot revoke publishment" }
      else: return { 'kind': 'missing', 'message': f"Client `{client_id}` is not connected" }
    
    self._remove_channel_publisher(client_id, channel_id)

    return NO_ERROR
  
  def add_subscription(self, client_id: str, channel_id: str, msg_filter: Callable[[str, OBJ, dict[str, str]], bool] = None) -> Error | NO_ERROR_T:
    """Sets up a new client subscription to a channel.
    The caller may optionally provide a message filtering function that is evaluated during delivery evaluation.
    """
    if client_id not in self._ctx['consumers']:
      if client_id in self._ctx['producers']: return { 'kind': 'conflict', 'message': f"Client `{client_id}` connected as a Prodcuer; it cannot subscribe" }
      else: return { 'kind': 'missing', 'message': f"Client `{client_id}` is not connected" }
    
    self._add_channel_subscriber(client_id, channel_id, msg_filter)

    return NO_ERROR
  
  def cancel_subscription(self, client_id: str, channel_id: str) -> Error | NO_ERROR_T:
    """Cancel a client subscription"""
    if client_id not in self._ctx['consumers']:
      if client_id in self._ctx['producers']: return { 'kind': 'conflict', 'message': f"Client `{client_id}` connected as a Prodcuer; it cannot unsubscribe" }
      else: return { 'kind': 'missing', 'message': f"Client `{client_id}` is not connected" }

    self._remove_channel_subscriber(client_id, channel_id)

    return NO_ERROR  

class _ProducerCtx(TypedDict):
  """Stateful Context of the Producer"""
  broker: NotRequired[Broker]
  """The currently subscribed Broker"""
  publish: NotRequired[txq_t]
  """The Publish Async Interface"""

  @staticmethod
  def factory(**kwargs) -> _ProducerCtx:
    return {} | { k: v for k, v in kwargs.items if k in ['broker', 'txq'] }

@dataclass
class Producer:
  """A Produce publishes messages on a Broker"""

  _: KW_ONLY
  id: str = field(default_factory=lambda: f"producer-{time.monotonic_ns():x}")
  """The Unique Identity of a Broker"""
  _ctx: _ProducerCtx = field(default_factory=dict)
  """Current State of the Producer"""

  def connect(self, broker: Broker) -> Error | NO_ERROR_T:
    """Connects, or refreshes the connection, to a Broker"""
    if 'broker' in self._ctx and self._ctx['broker'] is not broker: return { 'kind': 'conflict', 'message': f"Producer `{self.id}` is already connected to a different Broker" }
    err, publish = broker.connect_producer(self.id)
    if err is not NO_ERROR:
      return err
    self._ctx['broker'] = broker
    self._ctx['publish'] = publish
    return NO_ERROR

  def announce(self, channel_id: str) -> Error | NO_ERROR_T:
    """Inform the Broker of the producer's intent to publish messages on a channel"""
    if 'broker' not in self._ctx: raise RuntimeError(f"Producer `{self.id}` is not connected to a Broker")
    return self._ctx['broker'].announce_publisher(self.id, channel_id)
  
  def revoke(self, channel_id: str) -> Error | NO_ERROR_T:
    """Revokes a previous announcement of the producer's intent to publish messages on a channel."""
    if 'broker' not in self._ctx: raise RuntimeError(f"Producer `{self.id}` is not connected to a Broker")
    return self._ctx['broker'].revoke_publisher(self.id, channel_id)
  
  async def publish(self, channel_id: str, payload: OBJ, userdata: userdata_t = None) -> Error | NO_ERROR_T:
    """Publish a Payload & associated user data on the channel"""
    if 'broker' not in self._ctx: raise RuntimeError(f"Producer `{self.id}` is not connected to a Broker")
    # TODO: Handle Disconnection
    return await self._ctx['publish'](Message.factory(
      payload=payload,
      sender=self.id,
      recepient=self._ctx['broker'].id,
      channel=channel_id,
      userdata=userdata or {},
    ))

class _ConsumerCtx(TypedDict):
  """Stateful Context of the Consumer"""
  broker: NotRequired[Broker]
  """The currently subscribed Broker"""
  rx: ItemLog[Message[OBJ]]
  """The RX Log of Messages"""
  buffer: dict[str, deque[Message[OBJ]]]
  """The Buffer of Messages keyed on Channel"""
  is_congested: asyncio.Event
  """The Congestion State of the Consumer"""

@dataclass
class Consumer:
  """A Consumer subscribes to messages from a Broker"""

  _: KW_ONLY
  id: str = field(default_factory=lambda: f"consumer-{time.monotonic_ns():x}")
  """The Unique Identity of a Broker"""
  _ctx: _ConsumerCtx = field(default_factory=dict)
  """Current State of the Producer"""

  async def _rx(self, msg: Message[OBJ]) -> Error | NO_ERROR_T:
    """The Consumer's Async Rx Interface"""
    if 'broker' not in self._ctx: raise RuntimeError(f"Consumer `{self.id}` is not connected to a Broker")
    channel_id = msg['metadata']['channel']
    if channel_id not in self._ctx['subscriptions']: return { 'kind': 'missing', 'message': f"Consumer `{self.id}` is not subscribed to channel `{channel_id}`" }
    msg = await self._ctx['rx'].push(msg, block=False)
    if msg is None: return NO_ERROR
    else:
      self._ctx['is_congested'] = _create_event(True)
      return { 'kind': 'congested', 'message': f"Consumer `{self.id}`'s message log is full", 'still_congested': self._ctx['is_congested'] }

  def connect(self, broker: Broker, congestion_cfg: CongestionConfig = None) -> Error | NO_ERROR_T:
    """Connects, or refreshes the connection, to a Broker"""
    if 'broker' in self._ctx and self._ctx['broker'] is not broker: return { 'kind': 'conflict', 'message': f"Consumer `{self.id}` is already connected to a different Broker" }
    if congestion_cfg: err = broker.connect_consumer(self.id, self.listen, congestion_cfg)
    else: err = broker.connect_consumer(self.id, self.listen)
    if err is not NO_ERROR: return err
    # Initialize the Consumer State
    self._ctx['broker'] = broker
    self._ctx['rx'] = ItemLog()
    self._ctx['subscriptions'] = set()
    self._ctx['buffer'] = {}
    self._ctx['is_congested'] = _create_event(False)
    return NO_ERROR

  def disconnect(self) -> Error | NO_ERROR_T:
    """Disconnect from the currently connected Broker"""
    if 'broker' not in self._ctx: return NO_ERROR
    err = self._ctx['broker'].disconnect(self.id)
    if err is not NO_ERROR: return err
    # Clear the Consumer State
    for key in ('broker', 'rx', 'buffer', 'is_congested'):
      del self._ctx[key]
    return NO_ERROR

  def subscribe(self, channel_id: str, msg_filter: Callable[[str, OBJ, userdata_t], bool] | None = None) -> Error | NO_ERROR_T:
    """Subscribe to a channel on the Broker"""
    if 'broker' not in self._ctx: raise RuntimeError(f"Consumer `{self.id}` is not connected to a Broker")
    if channel_id not in self._ctx['buffer']: self._ctx['buffer'][channel_id] = deque()
    if msg_filter: return self._ctx['broker'].add_subscription(self.id, channel_id, msg_filter=msg_filter)
    else: return self._ctx['broker'].add_subscription(self.id, channel_id)
  
  def unsubscribe(self, channel_id: str) -> Error | NO_ERROR_T:
    """Unsubscribe to a previously subscribed channel from the Broker"""
    if 'broker' not in self._ctx: raise RuntimeError(f"Consumer `{self.id}` is not connected to a Broker")
    self._ctx['buffer'].pop(channel_id, None)
    return self._ctx['broker'].cancel_subscription(self.id, channel_id)
  
  async def listen(self, channel_id: str) -> tuple[Error | NO_ERROR_T, unpacked_msg_t]:
    """Listens indefinitely for the next available message on the channel"""
    if 'broker' not in self._ctx: raise RuntimeError(f"Consumer `{self.id}` is not connected to a Broker")

    # TODO: Implement Disconnection Handling

    msg = None
    if len(self._ctx['buffer'][channel_id]) > 0:
      msg = self._ctx['buffer'][channel_id].popleft()
    else:
      pop = False
      while True:
        if pop:
          await self._ctx['rx'].pop() # Deferred; Drop the Message since we processed it
          if self._ctx['is_congested'].is_set(): self._ctx['is_congested'].clear()
          pop = False

        if msg is not None: break # We found a message

        msg, pop = (await self._ctx['rx'].peek(), True)
        if msg['metadata']['channel'] != channel_id:
          self._ctx['buffer'][channel_id].append(msg) # Stash the message if it's not the right channel
          msg = None # Reset the message; we haven't found it yet

    return None, (
      msg['payload'],
      msg['metadata'].get('userdata', {}),
    )

async def _():
  """
  
  An Example Implementation of Message Multicasting
  
  """
  from typing import Literal
  from collections import deque
  broker = Broker()

  async def emulate_producer_publish(
    pub_id: str,
    channel_id: str,
  ):
    """Emulate a Producer Publishing Messages to a Channel"""
    while True:
      await broker._ctx['rxq'][pub_id].push(Message.factory(
        payload={'time': time.monotonic_ns()},
        sender=pub_id,
        recepient=broker.id,
        channel=channel_id,
      ))
  
  async def emulate_consumer_listen(
    sub_id: str,
    channel_id: str,
  ) -> Message[OBJ]:
    """Emulate a Consumer Listening for Messages"""
    while True:
      msg = await broker._ctx['txq'][sub_id].pop()
      if msg['metadata']['channel'] != channel_id: continue # TODO
      pass # NOTE: We would otherwise do something w/ this

  def _get_caching_queue(client_id: str) -> ItemLog[Message[OBJ]]:
    """Returns an unbounded caching Queue for further evaluation"""
    if client_id not in broker._ctx['queues']['cache']:
      broker._ctx['queues']['cache'][client_id] = ItemLog()
    return broker._ctx['queues']['cache'][client_id]

  def _get_delivery_queue(client_id: str) -> ItemLog[Message[OBJ]]:
    """Returns the delivery queue for a client"""
    if client_id not in broker._ctx['queues']['delivery']:
      broker._ctx['queues']['delivery'][client_id] = ItemLog(deque(maxlen=100))
    return broker._ctx['queues']['delivery'][client_id]

  def _duplicate_message(client_id: str, msg: Message[OBJ]) -> Message[OBJ]:
    raise NotImplementedError

  async def multicast_loop(rxq: msglog_t):
    """Handle Message Multicasting"""
    while True:
      msg = await rxq.pop()
      channel_id = msg['metadata']['channel']
      consumers = broker.channel_registry.consumers[channel_id]
      for cid in consumers:
        consumer_caching_queue = _get_caching_queue(cid)
        assert not consumer_caching_queue.full # The Caching Queue should be unbounded to prevent blocking
        consumer_caching_queue.push(_duplicate_message(cid, msg))
  
  async def client_delivery_eval_loop(
    client_id: str,
    caching_queue: ItemLog[Message[OBJ]],
    delivery_queue: ItemLog[Message[OBJ]],
    client_filter: Callable[..., bool],
    action_on_block: Literal['wait', 'drop'],
  ):
    """Handles Evaluation of Message Delivery to a client"""

    while True:
      msg = await caching_queue.pop()
      if not client_filter(
        channel_id=msg['metadata']['channel'],
        payload=msg['payload'],
        userdata=msg['metadata']['userdata'],
      ): continue # Drop the Message
      if delivery_queue.full:
        if action_on_block == 'drop':
          # Log a warning
          continue
        elif action_on_block == 'wait':
          await delivery_queue.wait_until(state='NotFull')
        else: raise NotImplementedError('TODO')
      
      assert not delivery_queue.full
      await delivery_queue.push(msg)
  
  async def client_tx_loop(
    client_id: str,
    txq: ItemLog[Message[OBJ]],
    delivery_queue: ItemLog[Message[OBJ]],
  ):
    """Handles the transimission of messages to be delivered to the client"""
    while True:
      await txq.push(
        await delivery_queue.pop()
      )

async def _example_subscription_delivery():
  """
  
  An Example of how Subscription delivery should be implemented between Message Broker & Subscribing Client
  
  """

  channel_registry = ChannelRegistry()
  client_channel_filters: dict[str, dict[str, Callable[..., bool]]] = {}
  client_connected_state: dict[str, asyncio.Event] = {}

  async def _client_delivery_eval_loop(
    client_id: str,
    client_delivery_queue: ItemLog[Message[OBJ]],
    client_tx_queue: ItemLog[Message[OBJ]],
  ):
    pop = False
    while True:
      if pop:
        await client_delivery_queue.pop()
        pop = False
      msg = await client_delivery_queue.peek()
      channel_id = msg['metadata']['channel']
      
      assert channel_id in channel_registry.channels
      assert client_id in channel_registry.subscriptions[channel_id]
      if not client_connected_state[client_id].is_set(): await client_connected_state[client_id].wait()

      pop = True

      # Eval if we should Deliver
      if not (client_channel_filters[client_id][channel_id])(msg): continue
      await client_tx_queue.push(msg)
  
  async def _client_tx_loop(
    client_id: str,
    client_tx_queue: ItemLog[Message[OBJ]],
    client_listener: Callable[..., Coroutine[None, None, Error | NO_ERROR_T]],
    congestion_mode: Literal['buffer', 'drop'],
  ):
    congestion_over: asyncio.Event = None
    mode: Literal['default', 'congested'] = 'default'
    pop = False
    buffer = []

    async def _deliver_msg(msg: Message[OBJ]) -> bool:
      nonlocal mode
      nonlocal congestion_over
      err = await client_listener(msg)
      if err is not NO_ERROR:
        if err['kind'] == 'congested':
          mode = 'congested'
          congestion_over = err['session']
          return False
        else: raise NotImplementedError
      return True

    while True:
      if pop:
        await client_tx_queue.pop()
        pop = False
      msg = await client_tx_queue.peek()
      assert msg['metadata']['recipient'] == client_id

      pop = True
      if mode == 'default':
        if not _deliver_msg(msg):
          pop = False
          continue
      elif mode == 'congested':
        assert congestion_over is not None
        if congestion_over.is_set():
          # Attempt to Deliver the buffered Messages
          idx = 0
          for msg in buffer:
            if not _deliver_msg(msg):
              pop = False
              break
            else: idx += 1
          buffer = buffer[idx:]
          if len(buffer) <= 0: # All Messages were delivered
            mode = 'default'
            congestion_over = None
            continue
        else:
          if congestion_mode == 'buffer':
            buffer.append(msg)
          elif congestion_mode == 'drop':
            continue
          else: raise ValueError(congestion_mode)
      else: raise ValueError(mode)
