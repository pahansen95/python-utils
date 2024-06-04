from __future__ import annotations
import asyncio, os, tempfile, random, string
from typing import ContextManager, Literal, Generator, ByteString, Coroutine
from contextlib import contextmanager
from loguru import logger
from utils.testing import TestResult, TestCode

__all__ = [
  'test_ItemLog_unbounded'
  'test_ItemLog_bounded'
]

async def test_ItemLog_unbounded(*args, **kwargs) -> TestResult:
  from utils.concurrency.log import (
    ItemLog
  )
  try:
    unbounded_log: ItemLog[object] = ItemLog()
    assert unbounded_log.empty, "A New Unbounded Log should be empty"
    assert not unbounded_log.full, "A New Unbounded Log should not be full"
    assert unbounded_log.max_size is None, f"A New Unbounded Log should not have a max_size; got {unbounded_log.max_size}"

    objs = [object() for _ in range(10)]
    none = await unbounded_log.peek(block=False)
    assert none is None, f"Peeking an Empty Log should return None in nonblocking mode; got {none}"

    none = await unbounded_log.push(objs[0])
    assert none is None, f"Pushing an Object should return None in blocking mode; got {none}"
    item = await unbounded_log.peek()
    assert item is objs[0], f"Peeking should return the head of the log, expected {objs[0]} but got {item}"

    await unbounded_log.push(objs[1])
    item = await unbounded_log.peek()
    assert item is objs[0], f"Peeking should return the head of the log, expected {objs[0]} but got {item}"

    item = await unbounded_log.pop()
    assert item == objs[0], f"Popping should return the head of the log, expected {objs[0]} but got {item}"
    
    item = await unbounded_log.peek()
    assert item == objs[1], f"Popping should return the head of the log, expected {objs[1]} but got {item}"

    async with unbounded_log.lock():
      await unbounded_log.clear()
    
    assert unbounded_log.empty, "The Log should be empty after clearing the log"
    assert not unbounded_log.full, "The Log should not be full after clearing the log"

  except AssertionError as e: return TestResult(TestCode.FAIL, str(e))

  return TestResult(TestCode.PASS)

async def test_ItemLog_bounded(*args, **kwargs) -> TestResult:
  from utils.concurrency.log import (
    ItemLog
  )
  from collections import deque
  try:
    bounded_log: ItemLog[object] = ItemLog(log=deque(maxlen=10))
    assert bounded_log.empty, "A New Bounded Log should be empty"
    assert not bounded_log.full, "A New Bounded Log should not be full"
    assert bounded_log.max_size is not None, "A New Bounded Log should have a max_size"

    objs = [object() for _ in range(10)]

    none = await bounded_log.peek(block=False)
    assert none is None, f"Peeking an Empty Log should return None in nonblocking mode; got {none}"

    none = await bounded_log.push(objs[0])
    assert none is None, f"Pushing an Object should return None in blocking mode; got {none}"
    item = await bounded_log.peek()
    assert item is objs[0], f"Peeking should return the head of the log, expected {objs[0]} but got {item}"

    await bounded_log.push(objs[1])
    item = await bounded_log.peek()
    assert item is objs[0], f"Peeking should return the head of the log, expected {objs[0]} but got {item}"

    item = await bounded_log.pop()
    assert item == objs[0], f"Popping should return the head of the log, expected {objs[0]} but got {item}"
    
    item = await bounded_log.peek()
    assert item == objs[1], f"Popping should return the head of the log, expected {objs[1]} but got {item}"

    async with bounded_log.lock():
      await bounded_log.clear()
    
    assert bounded_log.empty, "The Log should be empty after clearing the log"
    assert not bounded_log.full, "The Log should not be full after clearing the log"

    for obj in objs:
      await bounded_log.push(obj)
    
    assert not bounded_log.empty, "A Full Log should not be empty"
    assert bounded_log.full, "A Full Log should be full"

    # Test the bound log blocks when full
    obj = object()
    item = await bounded_log.push(obj, block=False)
    assert item is obj, f"A NonBlocking Push on a full log should return back the item: expected `{obj}` but got `{item}`"
    try:
      async with asyncio.timeout(1.0):
        _ = await bounded_log.push(obj)
      assert False, "Pushing onto a full log should indefinitely block until a slot is open; should never have come here!"
    except asyncio.TimeoutError: pass

    _sync = asyncio.Event()
    async def push_item(obj: object):
      try:
        async with asyncio.timeout(1.0):
          _sync.set()
          _ = await bounded_log.push(obj)
      except asyncio.TimeoutError:
        logger.warning("Timeout")
        assert False, "Pushing onto a full log should have stopped blocking after a slot was opened; should never have come here!"
    async def pop_item() -> object:
      try:
        async with asyncio.timeout(1.0):
          await _sync.wait()
          return await bounded_log.pop()
      except asyncio.TimeoutError:
        logger.warning("Timeout")
        assert False, "never should have come here!"
    async with asyncio.TaskGroup() as group:
      pop_task = group.create_task(pop_item())
      push_task = group.create_task(push_item(obj))
    
    try:
      item = pop_task.result()
      push_task.result()
    except Exception as e: assert False, str(e)

    assert item is objs[0], f"Expected the popped Log Item to be {objs[0]} but got {item}"
    assert bounded_log[-1] is obj, f"Expected the Tail of the Log to be {obj} but got {bounded_log[-1]}: {list(bounded_log)}"

  except AssertionError as e: return TestResult(TestCode.FAIL, str(e))

  return TestResult(TestCode.PASS)
