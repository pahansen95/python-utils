from __future__ import annotations
from loguru import logger
from utils.testing import TestResult, TestCode

__all__ = [
  'test_broker',
]

async def _test_tmpl(*args, **kwargs) -> TestResult:
  from utils.concurrency.pubsub import (
    Broker
  )

  try:
    ... # TODO: Implement Test
  except AssertionError as e: return TestResult(TestCode.FAIL, str(e))
  return TestResult(TestCode.PASS)

async def test_broker(*args, **kwargs) -> TestResult:
  from utils.errors import (
    Error, NO_ERROR, NO_ERROR_T
  )
  from utils.concurrency.pubsub import (
    Broker, Message, OBJ
  )
  from utils.concurrency.log import (
    ItemLog
  )
  data = [
    (
      {'foo': f'bar{i:02d}'},
      {'baz': f'qux{i:02d}'},
    ) for i in range(5)
  ]
  msgs: ItemLog[tuple[OBJ, dict[str, str]]] = ItemLog()
  async def consumer_rx(msg: Message[OBJ]) -> Error | NO_ERROR_T:
    logger.debug(f"Received Message: {msg['metadata']['name']}")
    await msgs.push((msg['payload'], msg['metadata']['userdata']))
    logger.debug(f"Pushed Message `{msg['metadata']['name']}` to Dummy Subscriber Queue")
    return NO_ERROR

  try:
    logger.info("Testing Broker")
    broker = Broker()
    pub_id = 'test-pub'
    sub_id = 'test-sub'
    channel_id = 'test-channel'

    ### Connect the Dummy Clients
    logger.info("Connecting Clients")
    err, producer_tx = await broker.connect_producer(pub_id)
    assert err is NO_ERROR, err
    err = await broker.connect_consumer(sub_id, consumer_rx)
    assert err is NO_ERROR, err

    ### Setup the Channel
    logger.info("Setting up Channel")
    err = broker.announce_publisher(pub_id, channel_id)
    assert err is NO_ERROR, err
    err = broker.add_subscription(sub_id, channel_id)
    assert err is NO_ERROR, err

    ### Attempt to Publish & Receive a Single Message
    logger.info("Publishing a Single Message")
    err = await producer_tx(Message.factory(
      payload=data[0][0],
      userdata=data[0][1],
      channel=channel_id,
      sender=pub_id,
      recipient=broker.id,
    ))
    assert err is NO_ERROR, err

    ### Check we received the message
    logger.info("Waiting for Message to be recieved")
    (_payload, _userdata) = await msgs.pop()
    logger.debug(f"Received Message: { dict([('userdata', _userdata), ('payload', _payload)]) }")
    assert _payload == data[0][0], f"Expected {data[0][0]} but got {_payload}"
    assert _userdata == data[0][1], f"Expected {data[0][1]} but got {_userdata}"

    ### Attempt to Publish & Recieve Multiple Messages
    logger.info("Publishing Multiple Messages")
    for i, (payload, userdata) in enumerate(data[1:], start=1):
      err = await producer_tx(Message.factory(
        payload=payload,
        userdata=userdata,
        channel=channel_id,
        sender=pub_id,
        recipient=broker.id,
      ))
      assert err is NO_ERROR, err
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
    logger.debug("Revoking Publisher")
    err = broker.revoke_publisher(pub_id, channel_id)
    assert err is NO_ERROR, err
    # Disconnect
    logger.debug("Disconnecting Clients")
    err = await broker.disconnect(sub_id)
    assert err is NO_ERROR, err
    err = await broker.disconnect(pub_id)
    assert err is NO_ERROR, err
    logger.debug("Client Cleanup Complete")

    # TODO: Assert the Broker has no background tasks running
  except AssertionError as e: return TestResult(TestCode.FAIL, str(e))
  return TestResult(TestCode.PASS)
