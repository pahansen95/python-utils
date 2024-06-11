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
from typing import TypedDict, NotRequired, Literal
from collections import deque
from dataclasses import dataclass, field, KW_ONLY
from utils.concurrency.log import ItemLog
from utils.errors import Error, NO_ERROR_T, NO_ERROR
import orjson, asyncio, time
from loguru import logger
from .. import (
  BrokerInterface, ConsumerInterface, ProducerInterface,
  CongestionConfig, Message, ChannelRegistry,
  generate_id, _raise_TODO,
  OBJ, userdata_t, unpacked_msg_t, msg_push_t, msg_filter_t
)

def _create_event(state: bool) -> asyncio.Event:
  event = asyncio.Event()
  if state: event.set()
  return event

class _MsgRetention(TypedDict):
  done: asyncio.Event
  """Was the message processed by the Broker?"""
  result: NotRequired[Literal['retained', 'rejected']]
  """Did the Broker retain or reject the message? Not expected until `done` is set"""
  err: NotRequired[Error]
  """If the message was rejected, what was the error? Not expected unless `result` is 'rejected'"""

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
  client_rx: msg_push_t
  """The client's Rx Interface used to push messages to the client; should return an error if the message fails to deliver or would block indefinitely."""
  filters: dict[str, msg_filter_t]
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
class Broker(BrokerInterface):
  """TODO: Update Broker Documentation

Manage State & Lifecycle of message brokering between Producers & Consumers.

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

  _id: str = field(default_factory=lambda: generate_id('broker'))
  """The Unique Identity of a Broker"""
  channel_registry: ChannelRegistry = field(default_factory=ChannelRegistry)
  """The current state of channels"""
  _: KW_ONLY
  _ctx: _BrokerCtx = field(default_factory=_BrokerCtx.factory)
  """Stateful Context of the Broker"""

  @property
  def id(self) -> str: return self._id

  async def _setup_producer(self, client_id: str) -> msg_push_t:

    # Setup Broker Multicasting Loop on the first publisher
    task_key = f"loop_multicast"
    if task_key not in self._ctx['tasks']:
      if not 'multicast_queue' in self._ctx: self._ctx['multicast_queue'] = ItemLog()
      self._ctx['tasks'][task_key] = asyncio.create_task(
        self._loop_multicast(
          self._ctx['multicast_queue']
        )
      )

    if client_id in self._ctx['producers']:
      ... # TODO: Implement Client Connection Refresh
      _raise_TODO('Client Connection Refresh')
    self._ctx['producers'][client_id] = {
      'rx_queue': ItemLog(),
      'disconnect': asyncio.Event(),
    }

    # Schedule the client Rx Loop
    task_key = f"loop_{client_id}_rx"
    if task_key in self._ctx['tasks']:
      ... # TODO Cancel the Current Task
      _raise_TODO('Client Connection Refresh')
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
      ... # TODO Cancel the Current Task
      _raise_TODO('Cancel the current disconnect task')
    self._ctx['tasks'][task_key] = asyncio.create_task(self._ctx['producers'][client_id]['disconnect'].wait())

    return self._publish_message_coro_factory(
      client_id,
      self._ctx['producers'][client_id]['rx_queue']
    )
  
  async def _teardown_producer(self, client_id: str):
    if client_id not in self._ctx['producers']: raise ValueError(f'{client_id} was never connected')
    logger.trace(f"Removing Client {client_id} from all Publishments")
    for channel_id in [
      channel_id
      for channel_id, publishers in self.channel_registry.publishments.items()
      if client_id in publishers
    ]: self._remove_channel_publisher(client_id, channel_id)
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
    client_rx: msg_push_t,
    congestion_cfg: CongestionConfig,
  ):
    if client_id in self._ctx['consumers']: raise NotImplementedError('Client Connection Refresh')
    self._ctx['consumers'][client_id] = {
      'delivery_queue': ItemLog(),
      'tx_queue': ItemLog(),
      'client_rx': client_rx,
      'disconnect': asyncio.Event(),
      'filters': {},
    }

    # Schedule client Delivery Eval Loop
    task_key = f"loop_{client_id}_delivery_eval"
    if task_key in self._ctx['tasks']:
      _raise_TODO('Cancel the current task')
      ... # TODO Cancel the Current Task
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
      _raise_TODO('Cancel the current task')
      ... # TODO Cancel the Current Task
    self._ctx['tasks'][task_key] = asyncio.create_task(
      self._loop_tx(
        client_id=client_id,
        client_rx=self._ctx['consumers'][client_id]['client_rx'],
        tx_queue=self._ctx['consumers'][client_id]['tx_queue'],
        cfg=congestion_cfg,
      )
    )

    # Schedule the shared client disconnect wait task
    task_key = f"wait_{client_id}_disconnect"
    if task_key in self._ctx['tasks']:
      _raise_TODO('Cancel the current task')
      ... # TODO Cancel the Current Task
    self._ctx['tasks'][task_key] = asyncio.create_task(self._ctx['consumers'][client_id]['disconnect'].wait())

  async def _teardown_consumer(self, client_id: str):
    if client_id not in self._ctx['consumers']: raise ValueError(f'{client_id} was never connected')
    for channel_id in list(
      channel_id
      for channel_id, subscribers in self.channel_registry.subscriptions.items()
      if client_id in subscribers
    ): self._remove_channel_subscriber(client_id, channel_id)
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
    if client_id not in self.channel_registry.publishments[channel_id]: self.channel_registry.add_publisher(channel_id, client_id)
  
  def _remove_channel_publisher(self, client_id: str, channel_id: str):
    if not channel_id in self.channel_registry.channels: raise ValueError(f'{channel_id} was never registered')
    if client_id in self.channel_registry.publishments[channel_id]: self.channel_registry.remove_publisher(channel_id, client_id)
    if len(self.channel_registry.publishments[channel_id]) == 0: self.channel_registry.remove_channel(channel_id)

  def _publish_message_coro_factory(self, client_id: str, rx_queue: ItemLog[tuple[Message[OBJ], _MsgRetention]]) -> msg_push_t:
    offline_err = { 'kind': 'offline', 'message': f"Client `{client_id}` is offline" }
    wait_for_disconnect: asyncio.Task = self._ctx['tasks'][f"wait_{client_id}_disconnect"]
    async def push_and_process_message(msg: Message[OBJ], retention: _MsgRetention):
      logger.trace(f'Pushing Message into the Rx Queue for Client {client_id}')
      await rx_queue.push((msg, retention))
      logger.trace(f'Waiting for Broker to process the Message for Client {client_id}')
      await retention['done'].wait()
      logger.trace(f'Broker has processed the Message for Client {client_id}')
    async def publish_message(msg: Message[OBJ]) -> Error | NO_ERROR_T:
      logger.trace(f'Publishing Message for Client {client_id}')
      if client_id not in self._ctx['producers']: return offline_err
      retention = { 'done': _create_event(False) }
      msg_id = msg['metadata']['id']
      channel_id = msg['metadata']['channel']
      if channel_id not in self.channel_registry.channels: return { 'kind': 'missing', 'message': f"Channel `{channel_id}` is not registered" }
      elif client_id not in self.channel_registry.publishments[channel_id]: return { 'kind': 'missing', 'message': f"Client `{client_id}` did not announce it's publishment to `{channel_id}`" }
      logger.trace(f'Scheduling Task to push message into the Rx Queue for Client {client_id}')
      # push_task = asyncio.create_task(rx_queue.push((msg, retained)))
      push_task = asyncio.create_task(push_and_process_message(msg, retention))
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
      if retention['result'] == 'retained':
        logger.trace(f'Broker {self.id} has taken posession of Message {msg_id} from Client {client_id}')
        return NO_ERROR
      elif retention['result'] == 'rejected':
        return retention['err']
      else: raise RuntimeError(f'Unexpected Message Retention State: {retention["result"]}')
    
    logger.trace(f'Created Publish Message Coroutine Factory for Client {client_id}')
    return publish_message
  
  def _add_channel_subscriber(self,
    client_id: str,
    channel_id: str,
    msg_filter: msg_filter_t | None,
  ):
    """Setup a Client's Channel Subscription"""
    if not channel_id in self.channel_registry.channels: raise ValueError(f'{channel_id} must first be registered for publishment before it can be subscribed to')
    if client_id not in self.channel_registry.subscriptions[channel_id]: self.channel_registry.add_subscriber(channel_id, client_id)
    if msg_filter is not None: self._ctx['consumers'][client_id]['filters'][channel_id] = msg_filter

  def _remove_channel_subscriber(self, client_id: str, channel_id: str):
    if not channel_id in self.channel_registry.channels: raise ValueError(f'{channel_id} was never registered')
    if client_id in self.channel_registry.subscriptions[channel_id]: self.channel_registry.remove_subscriber(channel_id, client_id)
    if channel_id in self._ctx['consumers'][client_id]['filters']: del self._ctx['consumers'][client_id]['filters'][channel_id]

  ### Broker Loops ###

  async def _loop_rx(self,
    client_id: str,
    rx_queue: ItemLog[tuple[Message[OBJ], _MsgRetention]],
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
        _raise_TODO('Client Disconnected')

      logger.trace(f"Peeking at the Rx Queue for Client {client_id}")
      (msg, retention), pop = (await rx_queue.peek(), True)
      # channel_id = msg['metadata']['channel']
      # assert client_id == msg['metadata']['sender']

      logger.trace(f"Adding Message to the Channel Registry for Client {client_id}")
      err = await self.channel_registry.add_message(msg)
      if err is not NO_ERROR:
        # logger.critical(f"Failed to add Message {msg['metadata']['id']} to the Channel Registry for Client {client_id}: {err['kind']}: {err['msg']}")
        retention['result'] = 'rejected'
        retention['err'] = err
      else:
        retention['result'] = 'retained'
      logger.trace(f"Broker {self.id} has processed Message {msg['metadata']['id']} from Client {client_id}")
      retention['done'].set() # Inform the Cooperative Publishing Function the broker has processed the message
      
      if retention['result'] == 'retained':
        logger.trace(f"Pushing Message {msg['metadata']['id']} into the Broker's Multicast Queue")
        await multicast_queue.push(msg['metadata']['id'])

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
        _raise_TODO('Not a Valid Channel') # Not a valid channel
      elif client_id not in self.channel_registry.subscriptions[channel_id]:
        _raise_TODO('Client not subscribed to this channel') # Client isn't subscribed to this channel
      elif self._ctx['consumers'][client_id]['disconnect'].is_set():
        _raise_TODO('Client is disconnected') # Client is currently disconnected
      elif msg_filter is not None and not msg_filter(
        channel_id,
        self.channel_registry.messages[msg_id]['payload'],
        self.channel_registry.messages[msg_id]['metadata'].get('userdata', {})
      ):
        _raise_TODO('Client filtered out the message') # Client filtered out the message
      else:
        logger.trace(f"Pushing Message {msg_id} to the Tx Queue for Client {client_id}")
        await tx_queue.push(msg_id) # The Message is valid; handoff to the Tx Queue

  async def _loop_tx(self,
    client_id: str,
    client_rx: msg_push_t,
    tx_queue: ItemLog[str],
    cfg: CongestionConfig,
  ):
    """Transmit the Message to the Client via the provided client_rx"""

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
      return await client_rx(tx_msg)

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
          else: _raise_TODO(f'Handle Error for Client {client_id}: {err["kind"]}: {err["message"]}')
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
          _raise_TODO('Drop the message') # TODO: Implement
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

  async def connect_producer(self, client_id: str) -> tuple[Error | NO_ERROR_T, msg_push_t | None]:
    """Asynchronously connects a Producer Client to the Broker
    Returns a Coroutine Factory the publisher may use to push a message into the Broker.
    """
    if client_id in self._ctx['consumers']: return { 'kind': 'conflict', 'message': f"Client `{client_id}` is already connected as a Consumer" }, None
    publisher_tx = await self._setup_producer(client_id)
    assert client_id in self._ctx['producers']

    return NO_ERROR, publisher_tx
  
  async def connect_consumer(self,
    client_id: str,
    consumer_rx: msg_push_t,
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
  
  def add_subscription(self, client_id: str, channel_id: str, msg_filter: msg_filter_t = None) -> Error | NO_ERROR_T:
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
  publish: NotRequired[...]
  """The Publish Async Interface"""

  @staticmethod
  def factory(**kwargs) -> _ProducerCtx:
    return {} | { k: v for k, v in kwargs.items if k in ['broker', 'txq'] }

@dataclass
class Producer(ProducerInterface):
  """A Produce publishes messages on a Broker"""

  _: KW_ONLY
  _id: str = field(default_factory=lambda: generate_id('producer'))
  """The Unique Identity of a Broker"""
  _ctx: _ProducerCtx = field(default_factory=dict)
  """Current State of the Producer"""

  @property
  def id(self) -> str: return self._id

  async def connect(self, broker: Broker) -> Error | NO_ERROR_T:
    """Connects, or refreshes the connection, to a Broker"""
    if 'broker' in self._ctx and self._ctx['broker'] is not broker: return { 'kind': 'conflict', 'message': f"Producer `{self.id}` is already connected to a different Broker" }
    err, publish = await broker.connect_producer(self.id)
    if err is not NO_ERROR:
      return err
    self._ctx['broker'] = broker
    self._ctx['publish'] = publish
    return NO_ERROR

  async def disconnect(self) -> Error | NO_ERROR_T:
    if 'broker' not in self._ctx: return { 'kind': 'missing', 'message': f"Producer `{self.id}` is not connected to a Broker" }
    err = await self._ctx['broker'].disconnect(self.id)
    del self._ctx['broker']
    del self._ctx['publish']
    return err

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
class Consumer(ConsumerInterface):
  """A Consumer subscribes to messages from a Broker"""

  _: KW_ONLY
  _id: str = field(default_factory=lambda: generate_id('consumer'))
  """The Unique Identity of a Broker"""
  _ctx: _ConsumerCtx = field(default_factory=dict)
  """Current State of the Producer"""


  async def _rx(self, msg: Message[OBJ]) -> Error | NO_ERROR_T:
    """The Consumer's Async Rx Interface"""
    if 'broker' not in self._ctx: raise RuntimeError(f"Consumer `{self.id}` is not connected to a Broker")
    channel_id = msg['metadata']['channel']
    if channel_id not in self._ctx['buffer']: return { 'kind': 'missing', 'message': f"Consumer `{self.id}` is not subscribed to channel `{channel_id}`" }
    msg = await self._ctx['rx'].push(msg, block=False)
    if msg is None: return NO_ERROR
    else:
      self._ctx['is_congested'] = _create_event(True)
      return { 'kind': 'congested', 'message': f"Consumer `{self.id}`'s message log is full", 'still_congested': self._ctx['is_congested'] }

  async def connect(self, broker: Broker, congestion_cfg: CongestionConfig = None) -> Error | NO_ERROR_T:
    """Connects, or refreshes the connection, to a Broker"""
    if 'broker' in self._ctx and self._ctx['broker'] is not broker: return { 'kind': 'conflict', 'message': f"Consumer `{self.id}` is already connected to a different Broker" }
    if congestion_cfg: err = await broker.connect_consumer(self.id, self._rx, congestion_cfg)
    else: err = await broker.connect_consumer(self.id, self._rx)
    if err is not NO_ERROR: return err
    # Initialize the Consumer State
    self._ctx['broker'] = broker
    self._ctx['rx'] = ItemLog()
    self._ctx['buffer'] = {}
    self._ctx['is_congested'] = _create_event(False)
    return NO_ERROR

  async def disconnect(self) -> Error | NO_ERROR_T:
    """Disconnect from the currently connected Broker"""
    if 'broker' not in self._ctx: return NO_ERROR
    err = await self._ctx['broker'].disconnect(self.id)
    if err is not NO_ERROR: return err
    # Clear the Consumer State
    for key in ('broker', 'rx', 'buffer', 'is_congested'):
      del self._ctx[key]
    return NO_ERROR

  def subscribe(self, channel_id: str, msg_filter: msg_filter_t | None = None) -> Error | NO_ERROR_T:
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

    return NO_ERROR, (
      msg['payload'],
      msg['metadata'].get('userdata', {}),
    )
