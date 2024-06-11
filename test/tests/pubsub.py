from __future__ import annotations
from loguru import logger
from utils.testing import TestResult, TestCode
import asyncio
from typing import TypedDict, NotRequired, Callable, Coroutine

__all__ = [
  'test_producer',
  'test_consumer',
  'test_broker',
  'test_broker_integration',
]

class _TestState(TypedDict):
  complete: asyncio.Event
  state: NotRequired[TestCode]
broker_test_state: _TestState = { 'complete': asyncio.Event(), }
consumer_test_state: _TestState = { 'complete': asyncio.Event(), }
producer_test_state: _TestState = { 'complete': asyncio.Event(), }
def _track_test_state(state: dict):
  def _track_test_state_wrapper(tst: Callable[..., Coroutine[None, None, TestResult]]) -> Callable[..., Coroutine[None, None, TestResult]]:
    async def _wrapper(*args, **kwargs) -> TestResult:
      try:
        result = await tst(*args, **kwargs)
        state['state'] = result.code
        return result
      except:
        state['state'] = TestCode.FAIL
        raise
      finally:
        state['complete'].set()
    return _wrapper
  return _track_test_state_wrapper

async def _test_tmpl(*args, **kwargs) -> TestResult:
  from utils.errors import (
    Error, NO_ERROR, NO_ERROR_T
  )
  from utils.concurrency.comms.pubsub.backends.local import (
    Broker, Producer, Consumer, Message, ChannelRegistry, OBJ
  )
  from utils.concurrency.log import (
    ItemLog
  )
  data = [
    (
      {'foo': f'bar{i:02d}'},
      {'baz': f'qux{i:02d}'},
    ) for i in range(4)
  ]
  msgs: ItemLog[tuple[OBJ, dict[str, str]]] = ItemLog()
  async def dummy_consumer_rx(msg: Message[OBJ]) -> Error | NO_ERROR_T:
    logger.debug(f"Received Message: {msg['metadata']['id']}")
    await msgs.push((msg['payload'], msg['metadata']['userdata']))
    logger.debug(f"Pushed Message `{msg['metadata']['id']}` to Dummy Subscriber Queue")
    return NO_ERROR

  try:
    channel_id = 'test-channel'
    broker_id = 'test-broker'
    producer_id = 'test-producer'
    consumer_id = 'test-consumer'

    ... # TODO: Implement Test
  except AssertionError as e: return TestResult(TestCode.FAIL, str(e))
  return TestResult(TestCode.PASS)

async def test_broker_integration(*args, **kwargs) -> TestResult:
  await asyncio.gather(
    broker_test_state['complete'].wait(),
    producer_test_state['complete'].wait(),
    consumer_test_state['complete'].wait(),
  )
  if any(
    state['state'] is not TestCode.PASS
    for state in (broker_test_state, producer_test_state, consumer_test_state)
  ): return TestResult(TestCode.SKIP, msg="This test is depenedant on the Broker, Producer, and Consumer but one or more of the tests didn't Pass")

  from utils.errors import (
    Error, NO_ERROR, NO_ERROR_T
  )
  from utils.concurrency.comms.pubsub.backends.local import (
    Broker, Producer, Consumer, Message, ChannelRegistry, OBJ
  )
  from utils.concurrency.log import (
    ItemLog
  )
  data = [
    (
      {'foo': f'bar{i:02d}'},
      {'baz': f'qux{i:02d}'},
    ) for i in range(4)
  ]

  try:
    channel_id = 'test-channel'
    broker = Broker(_id='test-broker')
    consumer = Consumer(_id='test-consumer')
    producer = Producer(_id='test-producer')

    logger.info("Testing End to End Broker Integration")

    logger.info("Connecting Clients")
    logger.debug("Connecting Producer")
    err = await producer.connect(broker=broker)
    assert err is NO_ERROR, err
    logger.debug("Connecting Consumer")
    err = await consumer.connect(broker=broker)
    assert err is NO_ERROR, err

    logger.info("Setting up Channels")
    logger.debug("Announcing Channel")
    err = producer.announce(channel_id)
    assert err is NO_ERROR, err
    logger.debug("Adding Subscription")
    err = consumer.subscribe(channel_id)
    assert err is NO_ERROR, err

    logger.info("Publishing Multiple Messages")
    for i, (payload, userdata) in enumerate(data, start=1):
      logger.debug(f"Publishing Message {i}")
      err = await producer.publish(
        channel_id=channel_id,
        payload=payload,
        userdata=userdata,
      )
      assert err is NO_ERROR, err

      logger.debug(f"Waiting for Message {i} to be recieved")
      err, (_payload, _userdata) = await consumer.listen(channel_id)
      assert err is NO_ERROR, err
      logger.debug(f"Received Message {i}: { dict([('userdata', _userdata), ('payload', _payload)]) }")
      assert _payload == payload, f"Expected {payload} but got {_payload}"
      assert _userdata == userdata, f"Expected {userdata} but got {_userdata}"

    logger.info("All Messages Received; Tearing Down")
    logger.debug("Disconnecting Consumer; implicitly unsubscribes")
    err = await consumer.disconnect()
    assert err is NO_ERROR, err
    logger.debug("Disconnecting Producer; implicitly revokes publishment")
    err = await producer.disconnect()
    assert err is NO_ERROR, err

  except AssertionError as e: return TestResult(TestCode.FAIL, str(e))
  return TestResult(TestCode.PASS)

@_track_test_state(producer_test_state)
async def test_producer(*args, **kwargs) -> TestResult:
  await broker_test_state['complete'].wait()
  if broker_test_state['state'] is not TestCode.PASS: return TestResult(TestCode.SKIP, msg="This test is depenedant on the Broker but the Broker Test didn't Pass")

  from utils.errors import (
    Error, NO_ERROR, NO_ERROR_T
  )
  from utils.concurrency.comms.pubsub.backends.local import (
    Broker, Producer, Consumer, Message, ChannelRegistry, OBJ
  )
  from utils.concurrency.log import (
    ItemLog
  )
  data = [
    (
      {'foo': f'bar{i:02d}'},
      {'baz': f'qux{i:02d}'},
    ) for i in range(4)
  ]
  msgs: ItemLog[tuple[OBJ, dict[str, str]]] = ItemLog()
  async def dummy_consumer_rx(msg: Message[OBJ]) -> Error | NO_ERROR_T:
    logger.debug(f"Received Message: {msg['metadata']['id']}")
    await msgs.push((msg['payload'], msg['metadata']['userdata']))
    logger.debug(f"Pushed Message `{msg['metadata']['id']}` to Dummy Subscriber Queue")
    return NO_ERROR  

  try:
    channel_id = 'test-channel'
    consumer_id = 'test-consumer'
    producer = Producer(_id='test-producer')
    broker = Broker(_id='test-broker')

    logger.info("Testing Producer")

    err = await producer.connect(broker=broker)
    assert err is NO_ERROR, err
    err = producer.announce(channel_id) 
    assert err is NO_ERROR, err
    assert channel_id in broker.channel_registry.channels
    assert producer.id in broker.channel_registry.publishments[channel_id]

    ### Connect the Dummy Clients
    logger.info("Connecting Clients")
    err = await broker.connect_consumer(consumer_id, dummy_consumer_rx)
    assert err is NO_ERROR, err

    ### Setup the Channel
    logger.info("Setting up Channel")
    err = broker.add_subscription(consumer_id, channel_id)
    assert err is NO_ERROR, err

    logger.info("Publishing Multiple Messages")
    for i, (payload, userdata) in enumerate(data, start=1):
      err = await producer.publish(
        channel_id=channel_id,
        payload=payload,
        userdata=userdata,
      )
      assert err is NO_ERROR, err
      logger.debug(f"Published Message {i}")
    
    logger.info("Waiting for Messages to be recieved")
    for i, (payload, userdata) in enumerate(data, start=1):
      (_payload, _userdata) = await msgs.pop()
      logger.debug(f"Received Message {i}: { dict([('userdata', _userdata), ('payload', _payload)]) }")
      assert _payload == payload, f"Expected {payload} but got {_payload}"
      assert _userdata == userdata, f"Expected {userdata} but got {_userdata}"

    logger.info("All Messages Received; Tearing Down")
    logger.debug("Unsubscribing Dummy Consumer")
    err = broker.cancel_subscription(consumer_id, channel_id)
    assert err is NO_ERROR, err
    logger.debug("Disconnecting Dummy Consumer")
    err = await broker.disconnect(consumer_id)
    assert err is NO_ERROR, err
    logger.debug("Disconnecting Producer without explicitly Revoking Channel")
    assert channel_id in broker.channel_registry.channels, f"Channel {channel_id} doesn't exist"
    assert producer.id in broker.channel_registry.publishments[channel_id], f"Channel {channel_id} has no publishers"
    err = await producer.disconnect() # This implicitly revokes any publisherments
    assert channel_id not in broker.channel_registry.channels, f"Channel {channel_id} still exists"
    assert channel_id not in broker.channel_registry.publishments, f"Channel {channel_id} still has publishers"
    assert err is NO_ERROR, err

  except AssertionError as e: return TestResult(TestCode.FAIL, str(e))
  return TestResult(TestCode.PASS)

@_track_test_state(consumer_test_state)
async def test_consumer(*args, **kwargs) -> TestResult:
  await broker_test_state['complete'].wait()
  if broker_test_state['state'] is not TestCode.PASS: return TestResult(TestCode.SKIP, msg="This test is depenedant on the Broker but the Broker Test didn't Pass")

  from utils.errors import (
    Error, NO_ERROR, NO_ERROR_T
  )
  from utils.concurrency.comms.pubsub.backends.local import (
    Broker, Producer, Consumer, Message, ChannelRegistry, OBJ
  )
  from utils.concurrency.log import (
    ItemLog
  )
  data = [
    (
      {'foo': f'bar{i:02d}'},
      {'baz': f'qux{i:02d}'},
    ) for i in range(4)
  ]

  try:
    channel_id = 'test-channel'
    producer_id = 'test-producer'
    consumer = Consumer(_id='test-consumer')
    broker = Broker(_id='test-broker')

    logger.info("Testing Consumer")

    logger.info("Connecting Clients")
    logger.debug("Connecting Dummy Producer")
    err, producer_tx = await broker.connect_producer(producer_id)
    assert err is NO_ERROR, err
    logger.debug("Announcing Dummy Publisher")
    err = broker.announce_publisher(producer_id, channel_id)
    assert err is NO_ERROR, err

    logger.debug("Connecting Consumer")
    err = await consumer.connect(broker=broker)
    assert err is NO_ERROR, err
    logger.debug("Adding Subscription")
    err = consumer.subscribe(channel_id)
    assert err is NO_ERROR, err

    logger.info("Publishing Multiple Messages")
    for i, (payload, userdata) in enumerate(data, start=1):
      err = await producer_tx(Message.factory(
        payload=payload,
        userdata=userdata,
        channel=channel_id,
        sender=producer_id,
        recipient=broker.id,
      ))
      assert err is NO_ERROR, err
      logger.debug(f"Published Message {i}")
    
    logger.info("Waiting for Messages to be recieved")
    for i, (payload, userdata) in enumerate(data, start=1):
      err, (_payload, _userdata) = await consumer.listen(channel_id)
      assert err is not None, err
      logger.debug(f"Received Message {i}: { dict([('userdata', _userdata), ('payload', _payload)]) }")
      assert _payload == payload, f"Expected {payload} but got {_payload}"
      assert _userdata == userdata, f"Expected {userdata} but got {_userdata}"

    logger.info("All Messages Received; Tearing Down")
    logger.debug("Unsubscribing Consumer")
    err = consumer.unsubscribe(channel_id)
    assert err is NO_ERROR, err
    assert channel_id in broker.channel_registry.subscriptions, f"Unsubscribing from Channel {channel_id} shouldn't delete the subscription key"
    assert len(broker.channel_registry.subscriptions[channel_id]) == 0, f"Channel {channel_id} still has subscribers"
    assert channel_id in broker.channel_registry.channels, f"Channel {channel_id} doesn't exist"

    logger.debug("Revoking Dummy Publisher")
    err = broker.revoke_publisher(producer_id, channel_id)
    assert err is NO_ERROR, err

    logger.debug("Disconnecting Consumer")
    err = await consumer.disconnect()
    assert err is NO_ERROR, err
    logger.debug("Disconnecting Dummy Producer")
    err = await broker.disconnect(producer_id)
    assert err is NO_ERROR, err

  except AssertionError as e: return TestResult(TestCode.FAIL, str(e))
  return TestResult(TestCode.PASS)

@_track_test_state(broker_test_state)
async def test_broker(*args, **kwargs) -> TestResult:
  from utils.errors import (
    Error, NO_ERROR, NO_ERROR_T
  )
  from utils.concurrency.comms.pubsub.backends.local import (
    Broker, Message, OBJ
  )
  from utils.concurrency.log import (
    ItemLog
  )
  data = [
    (
      {'foo': f'bar{i:02d}'},
      {'baz': f'qux{i:02d}'},
    ) for i in range(4)
  ]
  msgs: ItemLog[tuple[OBJ, dict[str, str]]] = ItemLog()
  async def dummy_consumer_rx(msg: Message[OBJ]) -> Error | NO_ERROR_T:
    logger.debug(f"Received Message: {msg['metadata']['id']}")
    await msgs.push((msg['payload'], msg['metadata']['userdata']))
    logger.debug(f"Pushed Message `{msg['metadata']['id']}` to Dummy Subscriber Queue")
    return NO_ERROR

  try:
    logger.info("Testing Broker")
    pub_id = 'test-pub'
    sub_id = 'test-sub'
    channel_id = 'test-channel'

    broker = Broker()

    ### Connect the Dummy Clients
    logger.info("Connecting Dummy Producer")
    assert pub_id not in broker._ctx['producers'], f"Client {pub_id} already exists"
    err, producer_tx = await broker.connect_producer(pub_id)
    assert err is NO_ERROR, err
    assert pub_id in broker._ctx['producers'], f"Client {pub_id} doesn't exist"

    logger.info("Connecting Dummy Consumer")
    assert sub_id not in broker._ctx['consumers'], f"Client {sub_id} already exists"
    err = await broker.connect_consumer(sub_id, dummy_consumer_rx)
    assert err is NO_ERROR, err
    assert sub_id in broker._ctx['consumers'], f"Client {sub_id} doesn't exist"

    ### Setup the Channel
    logger.info("Announcing Publishment")
    assert channel_id not in broker.channel_registry.channels, f"Channel {channel_id} already exists"
    err = broker.announce_publisher(pub_id, channel_id)
    assert err is NO_ERROR, err
    assert channel_id in broker.channel_registry.channels, f"Channel {channel_id} doesn't exist"
    assert channel_id in broker.channel_registry.publishments, f"Channel {channel_id} has no publishers"
    assert pub_id in broker.channel_registry.publishments[channel_id], f"Channel {channel_id} has no publishers"

    logger.info("Adding Subscription")
    assert sub_id not in broker.channel_registry.subscriptions[channel_id], f"Client {sub_id} already subscribed to Channel {channel_id} in Broker"
    err = broker.add_subscription(sub_id, channel_id)
    assert err is NO_ERROR, err
    assert sub_id in broker.channel_registry.subscriptions[channel_id], f"Client {sub_id} not subscribed to Channel {channel_id} in Broker"

    ### Attempt to Publish & Receive a Single Message
    logger.info("Publishing a Single Message")
    msg_0 = Message.factory(
      payload=data[0][0],
      userdata=data[0][1],
      channel=channel_id,
      sender=pub_id,
      recipient=broker.id,
    )
    assert msg_0['metadata']['id'] not in broker.channel_registry.messages, f"Message {msg_0['metadata']['id']} already exists"
    err = await producer_tx(msg_0)
    assert err is NO_ERROR, err
    assert msg_0['metadata']['id'] in broker.channel_registry.messages, f"Message {msg_0['metadata']['id']} doesn't exist"

    ### Check we received the message
    logger.info("Waiting for Message to be recieved")
    (_payload, _userdata) = await msgs.pop()
    logger.debug(f"Received Message: { dict([('userdata', _userdata), ('payload', _payload)]) }")
    assert _payload == data[0][0], f"Expected {data[0][0]} but got {_payload}"
    assert _userdata == data[0][1], f"Expected {data[0][1]} but got {_userdata}"

    ### Attempt to Publish & Recieve Multiple Messages
    logger.info("Publishing Multiple Messages")
    for i, (payload, userdata) in enumerate(data[1:], start=1):
      msg_n = Message.factory(
        payload=payload,
        userdata=userdata,
        channel=channel_id,
        sender=pub_id,
        recipient=broker.id,
      )
      assert msg_n['metadata']['id'] not in broker.channel_registry.messages, f"Message {msg_n['metadata']['id']} already exists"
      err = await producer_tx(msg_n)
      assert err is NO_ERROR, err
      assert msg_n['metadata']['id'] in broker.channel_registry.messages, f"Message {msg_n['metadata']['id']} doesn't exist"
      logger.debug(f"Published Message {i}")
    
    ### Check we received the messages
    logger.info("Waiting for Messages to be recieved")
    for i, (payload, userdata) in enumerate(data[1:], start=1):
      (_payload, _userdata) = await msgs.pop()
      logger.debug(f"Received Message {i}: { dict([('userdata', _userdata), ('payload', _payload)]) }")
      assert _payload == payload, f"Expected {payload} but got {_payload}"
      assert _userdata == userdata, f"Expected {userdata} but got {_userdata}"

    logger.info("All Messages Received")

    ### Cleanup Clients
    logger.info("Cleaning up Clients")
    # Unsub/Revoke
    logger.debug("Unsubscribing Consumer")
    err = broker.cancel_subscription(sub_id, channel_id)
    assert err is NO_ERROR, err
    assert sub_id not in broker.channel_registry.subscriptions[channel_id], f"Client {sub_id} still subscribed to Channel {channel_id} in Broker"
    assert channel_id in broker.channel_registry.channels, f"Channel {channel_id} doesn't exist"
    assert channel_id in broker.channel_registry.publishments, f"Channel {channel_id} doesn't have publishers"
    assert channel_id in broker.channel_registry.subscriptions, f"Unsubscribing from Channel {channel_id} shouldn't delete the subscription key"
    assert len(broker.channel_registry.subscriptions[channel_id]) == 0, f"Channel {channel_id} still has subscribers"
    logger.debug("Revoking Publisher")
    err = broker.revoke_publisher(pub_id, channel_id)
    assert err is NO_ERROR, err
    assert channel_id not in broker.channel_registry.channels, f"Channel {channel_id} still exists after the last publisher revoked"
    assert channel_id not in broker.channel_registry.publishments, f"Channel {channel_id} still has publishers"
    assert channel_id not in broker.channel_registry.subscriptions, f"Channel {channel_id} still has subscribers"
    # Disconnect
    logger.debug("Disconnecting Clients")
    err = await broker.disconnect(sub_id)
    assert err is NO_ERROR, err
    assert sub_id not in broker._ctx['consumers'], f"Client {sub_id} still exists"
    err = await broker.disconnect(pub_id)
    assert err is NO_ERROR, err
    assert pub_id not in broker._ctx['producers'], f"Client {pub_id} still exists"
    logger.debug("Client Cleanup Complete")

    # TODO: Assert the Broker has no background tasks running
  except AssertionError as e: return TestResult(TestCode.FAIL, str(e))
  return TestResult(TestCode.PASS)
