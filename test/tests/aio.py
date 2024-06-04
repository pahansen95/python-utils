from __future__ import annotations
import asyncio, os, tempfile, random, string
from typing import ContextManager, Literal, Generator, ByteString, Coroutine
from contextlib import contextmanager
from loguru import logger
from utils.testing import TestResult, TestCode

### Test Exports
__all__ = [
  "test_fd_read_file",
  "test_fd_read_stream",
]
###

fd_kinds_t = Literal['file', 'pty', 'pipe']

def _fd_context_factory(kind: fd_kinds_t,) -> ContextManager[tuple[int, int]]:
  
  # TODO: Implement Unix Domain, TCP & UDP Sockets

  if kind == 'file': _factory = lambda: tempfile.mkstemp()[0]
  elif kind == 'pty': _factory = lambda: os.openpty()
  elif kind == 'pipe': _factory = lambda: os.pipe()
  else: raise ValueError(f"Invalid kind: '{kind}'")

  @contextmanager
  def _fd_factory(kind: Literal['file', 'pty', 'pipe']) -> Generator[tuple[int, int], None, None]:
    """Returns a File Descriptor for reading and writing"""
    _cleanup = lambda: None
    try:
      if kind == 'file':
        fd = _factory()
        os.set_blocking(fd, False)
        def _cleanup() -> None:
          os.close(fd)
        yield (fd, fd)
      elif kind == 'pty':
        rfd, wfd = _factory()
        os.set_blocking(rfd, False)
        os.set_blocking(wfd, False)
        def _cleanup() -> None:
          os.close(rfd)
          os.close(wfd)
        yield (rfd, wfd)
      elif kind == 'pipe':
        rfd, wfd = _factory()
        os.set_blocking(rfd, False)
        os.set_blocking(wfd, False)
        def _cleanup() -> None:
          os.close(rfd)
          os.close(wfd)
        yield (rfd, wfd)
      else: raise NotImplementedError(kind)
    except:
      logger.opt(exception=True).error("Error")
      raise
    finally:
      logger.info(f"Cleaning up {kind} file descriptor")
      _cleanup()

  return _fd_factory(kind.lower())

def _fd_duplicate(fd: int) -> int:
  dup = os.dup(fd)
  os.set_blocking(dup, os.get_blocking(fd))
  return dup

VALID_CHARS = [c.encode('utf-8') for c in string.ascii_letters + string.digits]
def generate_rand_content(small: int, big: int) -> tuple[bytes, bytes]:
  # Need to make sure we aren't passing things like newlines b/c certain streaming fds (ie. pty) mangles output
  return (
    bytes(bytearray([int.from_bytes(random.choice(VALID_CHARS)) for _ in range(small)])),
    bytes(bytearray([int.from_bytes(random.choice(VALID_CHARS)) for _ in range(big)])),
  )

def calc_diff(left: ByteString, right: ByteString) -> int:
  """Calculates the Index where the 2 Byte Strings Differ"""
  min_len = min(len(left), len(right))
  
  for i in range(min_len):
    if left[i] != right[i]:
      return i
  
  if len(left) != len(right):
    return min_len
  
  raise RuntimeError('Byte Strings are not difference')

def render_diff(left: ByteString, right: ByteString, pad: int = 4) -> str:
  """Print the bit of the ByteString where the diff starts +/- the specified padding"""
  index = calc_diff(left, right)
  start = max(0, index - pad)
  end = min(max(len(left), len(right)), index + pad + 1)
  segment1 = left[start:end]
  segment2 = right[start:end]
  # diff_marker = ' ' * (index - start) + '^'
  return (
    f"expected  : `{segment1}`\n"
    f"got       : `{segment2}`\n"
)

def _fd_combinations(kinds: list[str], contents: list[bytes]) -> Generator[str, tuple[tuple[int, int], memoryview, int], None]:
  """Generates combinations given the set of kinds & contents
  
  returns: (
    kind: str,
    (readFD: int, writeFD: int),
    contents: memoryview,
    content_bytesize: int,
  )
  """
  for kind in kinds:
    with _fd_context_factory(kind) as (rfd, wfd):
      for content in contents:
        _content = memoryview(content)
        _content_size = len(content)
        logger.info(f"FD Combo: ( kind={kind}, rfd={rfd}, wfd={wfd}, content={_content}, size={_content_size} )")
        yield (
          kind,
          (rfd, wfd),
          _content,
          _content_size,
        )

async def _set_fd_state_for_reading(
  kind: fd_kinds_t,
  rfd: int, wfd: int,
  content: ByteString, content_size: int
) -> Coroutine | None:
  """Set up the state of a FD to be read from; returns a cleanup coroutine for streaming kinds"""
  if kind in ('file',):
    os.lseek(wfd, 0, os.SEEK_SET)
    os.truncate(wfd, 0)
    
    total_bytes_written = 0
    while total_bytes_written < content_size:
      try:
        total_bytes_written += os.write(wfd, content[total_bytes_written:content_size])
      except OSError as err:
        if err.errno == 11: # Non Blocking Error
          await asyncio.sleep(0) # yield to event loop so we don't block the thread
          continue
        else: raise RuntimeError(f"Unexpected OSError encounted when writing to nonblocking {kind}: No. {err.errno}: {err.strerror}")

    os.lseek(rfd, 0, os.SEEK_SET)
  elif kind in ('pty', 'pipe'):
    async def _drain_stream(rfd: int, write_task: asyncio.Task):
      try:
        logger.debug(f"Draining fd:{rfd}")
        if not write_task.done():
          logger.debug(f"Waiting for Write Task to fd:{wfd} to cancel")
          write_task.cancel()
          try: await write_task
          except asyncio.CancelledError: pass
        while True:
          try:
            chunk = os.read(rfd, 16384)
            logger.trace(f"Drained Chunk of size {len(chunk)} from fd:{rfd}")
          except OSError as err:
            logger.trace(f"fd:{rfd} encountered OS Error No. {err.errno}: {err.strerror}")
            if err.errno == 11: break # No more to read
            else: raise RuntimeError(f"Unexpected OSError encounted when writing to nonblocking {kind}: No. {err.errno}: {err.strerror}")  
      except:
        logger.opt(exception=True).debug("Error Encountered Draining Stream")
        raise
      finally: logger.trace(f"Exit _drain_stream for fd:{rfd}")
    async def _write_content_to_stream(wfd: int, content: ByteString, content_size: int):
      try:
        total_bytes_written = 0
        while total_bytes_written < content_size:
          try:
            total_bytes_written += os.write(wfd, content[total_bytes_written:content_size])
            logger.debug(f"{(100 * total_bytes_written / content_size):.1f}% written to fd:{wfd}")
          except OSError as err:
            logger.trace(f"fd:{wfd} encountered OS Error No. {err.errno}: {err.strerror}")
            if err.errno == 11: # Non Blocking Error
              await asyncio.sleep(0) # yield to event loop so we don't block the thread
              continue
            else: raise RuntimeError(f"Unexpected OSError encounted when writing to nonblocking {kind}: No. {err.errno}: {err.strerror}")
      except:
        logger.opt(exception=True).debug("Error Encountered Writing Content to Stream")
        raise
      finally: logger.trace(f"Exit _write_content_to_stream for fd:{wfd}")
    return _drain_stream(rfd, asyncio.create_task(_write_content_to_stream(wfd, content, content_size)))
  else: raise NotImplementedError(kind)

async def _set_fd_state_for_writing(
  kind: fd_kinds_t,
  rfd: int, wfd: int,
  content: ByteString, content_size: int
) -> tuple[Coroutine, Coroutine | None]:
  """Sets up a FD to be written to; return as tuple of Coroutines where the first returns the content written to the fd & the optional second cleans up any resources"""
  if kind in ('file',):
    async def _dump_file(rfd: int) -> bytes:
      try:
        os.lseek(rfd, 0, os.SEEK_SET)
        buffer = bytearray()
        while True:
          try:
            chunk = os.read(rfd, 16384)
            logger.trace(f"Read a chunk of size {len(chunk)} bytes from fd{rfd}")
            if chunk == b'': break
            buffer.extend(chunk)
          except OSError as err:
            if err.errno == 11: await asyncio.sleep(0)
            else: raise RuntimeError(f"Unexpected OSError encounted when writing to nonblocking {kind}: No. {err.errno}: {err.strerror}")  
        logger.trace(f"Read a total of {len(buffer)} bytes from fd{rfd}")
        return bytes(buffer)
      except:
        logger.opt(exception=True).debug(f"Error Encountered while Dumping fd{rfd} Contents")
        raise
      finally: logger.trace(f"Exit _set_fd_state_for_writing for fd:{wfd}")
    async def _reset_file(rfd: int, wfd: int) -> bytes:
      try:
        os.lseek(wfd, 0, os.SEEK_SET)
        os.truncate(wfd, 0)
        os.lseek(rfd, 0, os.SEEK_SET)
      except:
        logger.opt(exception=True).debug(f"Error Encountered while Reseting fd{wfd} Contents")
        raise
      finally: logger.trace(f"Exit _set_fd_state_for_writing for fd:{wfd}")
    return (_dump_file(rfd), _reset_file(rfd, wfd))
  elif kind in ('pty', 'pipe'):
    buffer = bytearray()
    async def _read_fd(rfd: int, buffer: bytearray):
      try:
        while len(buffer) < content_size:
          try:
            chunk = os.read(rfd, 16384)
            buffer.extend(chunk)
          except OSError as err:
            if err.errno == 11: await asyncio.sleep(0) # NonBlock
            else: raise err
      except asyncio.CancelledError: pass
      except: logger.opt(exception=True).critical(f"Unhandled Error encounterd while reading from fd:{rfd}")
    async def _dump_fd(read_task: asyncio.Task, buffer: bytearray) -> bytes:
      if not read_task.done():
        logger.trace(f"Awaiting ReadTask for fd:{rfd}")
        await read_task
      return bytes(buffer)
    async def _reset_fd(read_task: asyncio.Task, rfd: int) -> bytes:
      if not read_task.done():
        read_task.cancel()
        try: await read_task
        except asyncio.CancelledError: pass
      try:
        while True:
          try:
            os.read(rfd, 16384)
          except OSError as err:
            if err.errno == 11: break
            else: raise
      except asyncio.CancelledError: pass
      except: logger.opt(exception=True).critical(f"Unhandled Error encounterd while reading from fd:{rfd}")
    read_task = asyncio.create_task(_read_fd(rfd, buffer))
    return (_dump_fd(read_task, buffer), _reset_fd(read_task, rfd))
  else: raise NotImplementedError

async def test_fd_read_file(*args, **kwargs) -> TestResult:
  """Tests the OS Specific `fd_read` function against FileLike FileDescriptors

  Multiple Tests are conducted:

  - Read Behaviour for content sized smaller & larger than the Kernel Buffer amount
  - Reading without supplying a buffer
  - Reading supplying a buffer
  - Reading supplying a buffer with an offset
  
  """

  from utils.concurrency.aio import (
    IOErrorReason,
    aio_backend, AIOBackend
  )
  if aio_backend != AIOBackend.LINUX: return TestResult(TestCode.SKIP, "fd_read is only supported on Linux")
  from utils.concurrency.aio.linux import (
    fd_read,
  )
  import os

  small_content, big_content = generate_rand_content(32, 1024*128)
  buffer = bytearray()
  def _reset_buffer(size: int):
    buffer.clear()
    buffer.extend(b'\x00' * size)

  for (
    kind,
    (rfd, wfd),
    content,
    content_size,
  ) in _fd_combinations(
    ['file',],
    [small_content, big_content]
  ):
    try:
      """"NOTE Test 1 - Read `n` bytes from File

      - Read `content_size` bytes from `rfd`
      - result should be equal to `content`

      """
      logger.info(f"Test 1 - Read `{content_size}` bytes from fd:{rfd}")
      await _set_fd_state_for_reading(kind, rfd, wfd, content, content_size)
      _reset_buffer(0)
      # while len(buffer) < content_size:
      while len(buffer) < content_size:
        err, chunk = fd_read(rfd, content_size)
        if len(chunk) > 0:
          assert err == IOErrorReason.NONE, f"When a chunk is read, the expected IOErrorReason is {IOErrorReason.NONE.name}; we got {err.name}"
          buffer.extend(chunk)
        elif not err in (IOErrorReason.BLOCK, err == IOErrorReason.INTERRUPT): assert False, f'Encountered an unexpected IOErrorReason while reading from `{kind}` File Descriptor: {err.name}'
        else: await asyncio.sleep(0) # We need to explicitly yield to the event loop so we don't block the thread
      err, _ = fd_read(rfd, content_size)
      assert err == IOErrorReason.EOF, f"When reading a file after first exhausting it, the expected IOErrorReason is {IOErrorReason.EOF.name}; we got {err.name}"
      assert len(buffer) == len(content), f"content size mismatch: got {len(buffer)}; expected {len(content)}"
      assert buffer[:] == content[:], f"content mismatch: got {bytes(buffer)}; expected {bytes(content)}"
      logger.info("Test 1 -  Successful Completion")

      """NOTE Test 2 - Read `n` bytes from File into Buffer

      - Read `content_size` bytes from `rfd` into `buffer`
      - `buffer` should be equal to `content`

      """
      logger.info(f"Test 2 - Read `{content_size}` bytes from fd:{rfd} into {id(buffer)}")
      await _set_fd_state_for_reading(kind, rfd, wfd, content, content_size)
      _reset_buffer(content_size)
      total_bytes_read = 0
      while total_bytes_read < content_size:
        err, bytes_read = fd_read(rfd, content_size - total_bytes_read, buffer, total_bytes_read)
        if bytes_read > 0:
          assert err == IOErrorReason.NONE, f"When a chunk is read, the expected IOErrorReason is {IOErrorReason.NONE.name}; we got {err.name}"
          total_bytes_read += bytes_read
        elif not err in (IOErrorReason.BLOCK, IOErrorReason.INTERRUPT): assert False, f'Encountered an unexpected IOErrorReason while reading from `{kind}` File Descriptor: {err.name}'
        else: await asyncio.sleep(0) # We need to explicitly yield to the event loop so we don't block the thread
      err, _ = fd_read(rfd, 1)
      assert err == IOErrorReason.EOF, f"When reading a file after first exhausting it, the expected IOErrorReason is {IOErrorReason.EOF.name}; we got {err.name}"
      assert len(buffer) == len(content), f"content size mismatch: got {len(buffer)}; expected {len(content)}"
      assert buffer[:] == content[:], f"content mismatch: got {bytes(buffer)}; expected {bytes(content)}"
      logger.info("Test 2 -  Successful Completion")
    except AssertionError as ae: return TestResult(TestCode.FAIL, msg=str(ae))

  for (
    kind,
    (rfd, wfd),
    _, _
  ) in _fd_combinations(
    ['file',],
    [b''],
  ):
    try:
      """NOTE Test 3 - Test Failure Modes

      - Invalid Read Sizes [0, -1]
      - Undersized Buffer
      - Buffer Offset out of bounds
      - Reading a Closed FD returns CLOSED
      
      """
      logger.info(f"Test 3 - Test Failure Modes when reading from fd:{rfd}")
      await _set_fd_state_for_reading(kind, rfd, wfd, b'', 0)
      _reset_buffer(0)
      try: fd_read(rfd, 0)
      except ValueError: pass
      try: fd_read(rfd, -1)
      except ValueError: pass
      try: fd_read(rfd, 16384, buffer) # Undersized
      except ValueError: pass
      _reset_buffer(16384)
      try: fd_read(rfd, 16384, buffer, 16384 - 1) # Undersized after offset
      except ValueError: pass
      try: fd_read(rfd, 16384, buffer, -1) # offset out of bounds (negative)
      except ValueError: pass
      try: fd_read(rfd, 16384, buffer, 16384) # offset out of bounds (positive)
      except ValueError: pass
      fd = _fd_duplicate(rfd)
      os.close(fd)
      err, _empty_bytes = fd_read(fd, 16384)
      assert err == IOErrorReason.CLOSED, f"Reading from a closed FileDescriptor should exclusively result in a IOErrorReason of `{IOErrorReason.CLOSED.name}`: got {err.name}"
      assert _empty_bytes[:] == b'', f"Reading from a closed FileDescriptor should exclusively result in an empty byte string: got `{_empty_bytes}`"
      logger.info("Test 3 -  Successful Completion")
    except AssertionError as ae: return TestResult(TestCode.FAIL, msg=str(ae))

  return TestResult(TestCode.PASS)

async def test_fd_read_stream(*args, **kwargs) -> TestResult:
  """Tests the OS Specific `fd_read` function against StreamLike FileDescriptors

  Multiple Tests are conducted:

  - Reading without supplying a buffer
  - Reading supplying a buffer
  - Reading supplying a buffer with an offset
  """
  from utils.concurrency.aio import (
    IOErrorReason,
    aio_backend, AIOBackend
  )
  if aio_backend != AIOBackend.LINUX: return TestResult(TestCode.SKIP, "fd_read is only supported on Linux")
  from utils.concurrency.aio.linux import (
    fd_read,
  )
  import os

  small_content, big_content = generate_rand_content(32, 1024*128)
  buffer = bytearray()
  def _reset_buffer(size: int):
    buffer.clear()
    buffer.extend(b'\x00' * size)
    assert len(buffer) == size
    logger.trace(f"Reset Buffer {id(buffer)} to size {len(buffer)}")

  stream_cleanup: Coroutine | None
  for (
    kind,
    (rfd, wfd),
    content,
    content_size,
  ) in _fd_combinations(
    ['pty', 'pipe'],
    [small_content, big_content]
  ):
    try:
      """"NOTE Test 1 - Read `n` bytes from Stream

      - Read `content_size` bytes from `rfd`
      - result should be equal to `content`

      """
      logger.info(f"Test 1 - Read `{content_size}` bytes from fd:{rfd}")
      stream_cleanup = await _set_fd_state_for_reading(kind, rfd, wfd, content, content_size)
      _reset_buffer(0)
      while len(buffer) < content_size:
        err, chunk = fd_read(rfd, content_size)
        if len(chunk) > 0:
          assert err == IOErrorReason.NONE, f"When a chunk is read, the expected IOErrorReason is {IOErrorReason.NONE.name}; we got {err.name}"
          buffer.extend(chunk)
        elif not err in (IOErrorReason.BLOCK, err == IOErrorReason.INTERRUPT): assert False, f'Encountered an unexpected IOErrorReason while reading from `{kind}` File Descriptor: {err.name}'
        else: await asyncio.sleep(0) # We need to explicitly yield to the event loop so we don't block the thread
      err, _ = fd_read(rfd, content_size)
      assert err == IOErrorReason.BLOCK, f"When reading a stream after first exhausting it, the expected IOErrorReason is {IOErrorReason.BLOCK.name}; we got {err.name}"
      assert len(buffer) == len(content), f"content size mismatch: got {len(buffer)}; expected {len(content)}"
      assert buffer[:] == content[:], f"content mismatch: got {bytes(buffer)}; expected {bytes(content)}"
      await stream_cleanup; stream_cleanup = None
      logger.info("Test 1 -  Successful Completion")

      """NOTE Test 2 - Read `n` bytes from File into Buffer

      - Read `content_size` bytes from `rfd` into `buffer`
      - `buffer` should be equal to `content`

      """
      logger.info(f"Test 2 - Read `{content_size}` bytes from fd:{rfd} into {id(buffer)}")
      stream_cleanup = await _set_fd_state_for_reading(kind, rfd, wfd, content, content_size)
      _reset_buffer(content_size)
      total_bytes_read = 0
      while total_bytes_read < content_size:
        err, bytes_read = fd_read(rfd, max(content_size - total_bytes_read, 1), buffer, total_bytes_read)
        if bytes_read > 0:
          assert err == IOErrorReason.NONE, f"When a chunk is read, the expected IOErrorReason is {IOErrorReason.NONE.name}; we got {err.name}"
          total_bytes_read += bytes_read
        elif not err in (IOErrorReason.BLOCK, err == IOErrorReason.INTERRUPT): assert False, f'Encountered an unexpected IOErrorReason while reading from `{kind}` File Descriptor: {err.name}'
        else: await asyncio.sleep(0) # We need to explicitly yield to the event loop so we don't block the thread
      err, _ = fd_read(rfd, content_size)
      assert err == IOErrorReason.BLOCK, f"When reading a stream after first exhausting it, the expected IOErrorReason is {IOErrorReason.BLOCK.name}; we got {err.name}"
      assert len(buffer) == len(content), f"content size mismatch: got {len(buffer)}; expected {len(content)}"
      assert buffer[:] == content[:], f"content mismatch: got {bytes(buffer)}; expected {bytes(content)}"
      await stream_cleanup; stream_cleanup = None
      logger.info("Test 2 -  Successful Completion")
    except AssertionError as ae: return TestResult(TestCode.FAIL, msg=str(ae))
    finally:
      if stream_cleanup is not None: await stream_cleanup

  for (
    kind,
    (rfd, wfd),
    _, _
  ) in _fd_combinations(
    ['pty', 'pipe'],
    [b''],
  ):
    try:
      """NOTE Test 3 - Test Failure Modes

      - Invalid Read Sizes [0, -1]
      - Undersized Buffer
      - Buffer Offset out of bounds
      - Reading a Closed FD returns CLOSED
      
      """
      logger.info(f"Test 3 - Test Failure Modes when reading from fd:{rfd}")
      stream_cleanup = await _set_fd_state_for_reading(kind, rfd, wfd, b'', 0)
      _reset_buffer(0)
      try: fd_read(rfd, 0)
      except ValueError: pass
      try: fd_read(rfd, -1)
      except ValueError: pass
      try: fd_read(rfd, 16384, buffer) # Undersized
      except ValueError: pass
      _reset_buffer(16384)
      try: fd_read(rfd, 16384, buffer, 16384 - 1) # Undersized after offset
      except ValueError: pass
      try: fd_read(rfd, 16384, buffer, -1) # offset out of bounds (negative)
      except ValueError: pass
      try: fd_read(rfd, 16384, buffer, 16384) # offset out of bounds (positive)
      except ValueError: pass
      fd = _fd_duplicate(rfd)
      os.close(fd)
      err, _empty_bytes = fd_read(fd, 16384)
      assert err == IOErrorReason.CLOSED, f"Reading from a closed FileDescriptor should exclusively result in a IOErrorReason of `{IOErrorReason.CLOSED.name}`: got {err.name}"
      assert _empty_bytes[:] == b'', f"Reading from a closed FileDescriptor should exclusively result in an empty byte string: got `{_empty_bytes}`"
      await stream_cleanup; stream_cleanup = None
      logger.info("Test 3 -  Successful Completion")
    except AssertionError as ae: return TestResult(TestCode.FAIL, msg=str(ae))
    finally:
      if stream_cleanup is not None: await stream_cleanup

  return TestResult(TestCode.PASS)

async def test_fd_write_file(*args, **kwargs) -> TestResult:
  """Tests the OS Specific `fd_write` function against FileLike FileDescriptors

  Multiple Tests are conducted:

  - Write Behaviour for content sized smaller & larger than the Kernel Buffer amount
  - Reading supplying a buffer
  - Reading supplying a buffer with an offset
  
  """
  from utils.concurrency.aio import (
    IOErrorReason,
    aio_backend, AIOBackend
  )
  if aio_backend != AIOBackend.LINUX: return TestResult(TestCode.SKIP, "fd_read is only supported on Linux")
  from utils.concurrency.aio.linux import (
    fd_write,
  )
  import os

  reset_fd = None
  small_content, big_content = generate_rand_content(32, 1024*128)
  buffer = bytearray()
  def _reset_buffer(content: bytes):
    buffer.clear()
    buffer.extend(content)

  for (
    kind,
    (rfd, wfd),
    content,
    content_size,
  ) in _fd_combinations(
    ['file',],
    [small_content, big_content]
  ):
    try:
      """"NOTE Test 1 - Write `n` bytes to File

      - Write `content_size` bytes to `wfd`
      - The file contents should be equal to `content`

      """
      logger.info(f"Test 1 - Write `{content_size}` bytes to fd:{wfd}")
      (dump_fd, reset_fd) = await _set_fd_state_for_writing(kind, rfd, wfd, content, content_size)
      _reset_buffer(content)
      total_bytes_written = 0
      while total_bytes_written < content_size:
        err, bytes_written = fd_write(wfd, buffer, content_size - total_bytes_written, total_bytes_written)
        if bytes_written > 0:
          assert err == IOErrorReason.NONE, f"When content is written to a FileDescriptor, a IOErrorReason of `{IOErrorReason.NONE.name}` is expected; got `{err.name}`"
          total_bytes_written += bytes_written
        elif not err in (IOErrorReason.BLOCK, IOErrorReason.INTERRUPT): assert False, f'Encountered an unexpected IOErrorReason while writing to `{kind}` File Descriptor: {err.name}'
        else: await asyncio.sleep(0)
      file_content = await dump_fd; await reset_fd; reset_fd = None
      assert file_content[:] == content[:], f"content mismatch writing to fd{wfd}:\n{render_diff(bytes(file_content), bytes(content))}"
      logger.info("Test 1 -  Successful Completion")
    except AssertionError as ae: return TestResult(TestCode.FAIL, msg=str(ae))
    finally:
      if reset_fd is not None: await reset_fd; reset_fd = None

  for (
    kind,
    (rfd, wfd),
    content,
    content_size,
  ) in _fd_combinations(
    ['file',],
    [small_content,]
  ):
    try:
      """NOTE Test 2 - Write > `n` bytes to File

      - Write more bytes than are in the buffer into the file
      - Write more bytes than are int the buffer starting at an offset into the file
      
      """
      logger.info(f"Test 2 - Write `>{content_size}` bytes to fd:{wfd}")
      (dump_fd, reset_fd) = await _set_fd_state_for_writing(kind, rfd, wfd, content, content_size)
      _reset_buffer(content)
      # Write double the buffer size starting from an offset
      buffer_offset = 1
      err, bytes_written = fd_write(wfd, buffer, content_size * 2, buffer_offset)
      file_content = await dump_fd; await reset_fd; reset_fd = None
      assert file_content[:] == content[buffer_offset:], f"content mismatch writing to fd{wfd}:\n{render_diff(bytes(file_content), bytes(content))}"
      logger.info("Test 2 - Successful Completion")

      """NOTE Test 3 - Test Failure Modes

      - Invalid Write Sizes [0, -1]
      - Buffer Offset out of bounds
      - Writing to a Closed FD returns CLOSED
      
      """
      logger.info(f"Test 3 - Test Failure Modes when reading from fd:{rfd}")
      (dump_fd, reset_fd) = await _set_fd_state_for_writing(kind, rfd, wfd, content, content_size)
      _reset_buffer(content)
      try: fd_write(wfd, b'', 16384) # Empty Buffer
      except ValueError: pass
      try: fd_write(wfd, buffer, 0) # No write
      except ValueError: pass
      try: fd_write(wfd, buffer, -1) # negative write
      except ValueError: pass
      try: fd_write(wfd, buffer, 16384, -1) # Offset under bounds
      except ValueError: pass
      try: fd_write(wfd, buffer, 16384, len(buffer)) # Offset over bounds
      except ValueError: pass
      fd = _fd_duplicate(wfd)
      os.close(fd)
      err, _ = fd_write(fd, buffer, 16384)
      assert err == IOErrorReason.CLOSED, f"Writing to a closed FileDescriptor should exclusively result in a IOErrorReason of `{IOErrorReason.CLOSED.name}`: got {err.name}"
      await reset_fd; reset_fd = None
      logger.info("Test 3 -  Successful Completion")
    except AssertionError as ae: return TestResult(TestCode.FAIL, msg=str(ae))
    finally:
      if reset_fd is not None: await reset_fd; reset_fd = None
  return TestResult(TestCode.PASS)

async def test_fd_write_stream(*args, **kwargs) -> TestResult:
  """Tests the OS Specific `fd_write` function against StreamLike FileDescriptors

  Multiple Tests are conducted:

  - Write Behaviour for content sized smaller & larger than the Kernel Buffer amount
  - Reading supplying a buffer
  - Reading supplying a buffer with an offset
  
  """
  from utils.concurrency.aio import (
    IOErrorReason,
    aio_backend, AIOBackend
  )
  if aio_backend != AIOBackend.LINUX: return TestResult(TestCode.SKIP, "fd_read is only supported on Linux")
  from utils.concurrency.aio.linux import (
    fd_write,
  )
  import os

  reset_fd = None
  small_content, big_content = generate_rand_content(32, 1024*128)
  buffer = bytearray()
  def _reset_buffer(content: bytes):
    buffer.clear()
    buffer.extend(content)

  for (
    kind,
    (rfd, wfd),
    content,
    content_size,
  ) in _fd_combinations(
    ['pty', 'pipe'],
    [small_content, big_content]
  ):
    try:
      """"NOTE Test 1 - Write `n` bytes to Stream

      - Write `content_size` bytes to `wfd`
      - The stream contents should be equal to `content`

      """
      logger.info(f"Test 1 - Write `{content_size}` bytes to fd:{wfd}")
      (dump_fd, reset_fd) = await _set_fd_state_for_writing(kind, rfd, wfd, content, content_size)
      _reset_buffer(content)
      total_bytes_written = 0
      while total_bytes_written < content_size:
        err, bytes_written = fd_write(wfd, buffer, content_size - total_bytes_written, total_bytes_written)
        if bytes_written > 0:
          assert err == IOErrorReason.NONE, f"When content is written to a FileDescriptor, a IOErrorReason of `{IOErrorReason.NONE.name}` is expected; got `{err.name}`"
          total_bytes_written += bytes_written
        elif not err in (IOErrorReason.BLOCK, IOErrorReason.INTERRUPT): assert False, f'Encountered an unexpected IOErrorReason while writing to `{kind}` File Descriptor: {err.name}'
        else: await asyncio.sleep(0)
      fd_contents = await dump_fd; await reset_fd; reset_fd = None
      assert fd_contents[:] == content[:], f"content mismatch writing to fd{wfd}:\n{render_diff(bytes(fd_contents), bytes(content))}"
      logger.info("Test 1 -  Successful Completion")
    except AssertionError as ae: return TestResult(TestCode.FAIL, msg=str(ae))
    finally:
      if reset_fd is not None: await reset_fd; reset_fd = None
  
  for (
    kind,
    (rfd, wfd),
    content,
    content_size,
  ) in _fd_combinations(
    ['file',],
    [small_content,]
  ):
    try:
      """NOTE Test 2 - Write > `n` bytes to File

      - Write more bytes than are in the buffer into the file
      - Write more bytes than are int the buffer starting at an offset into the file
      
      """
      logger.info(f"Test 2 - Write `>{content_size}` bytes to fd:{wfd}")
      (dump_fd, reset_fd) = await _set_fd_state_for_writing(kind, rfd, wfd, content, content_size)
      _reset_buffer(content)
      # Write double the buffer size starting from an offset
      buffer_offset = 1
      err, bytes_written = fd_write(wfd, buffer, content_size * 2, buffer_offset)
      file_content = await dump_fd; await reset_fd; reset_fd = None
      assert file_content[:] == content[buffer_offset:], f"content mismatch writing to fd{wfd}:\n{render_diff(bytes(file_content), bytes(content))}"
      logger.info("Test 2 - Successful Completion")

      """NOTE Test 3 - Test Failure Modes

      - Invalid Write Sizes [0, -1]
      - Buffer Offset out of bounds
      - Writing to a Closed FD returns CLOSED
      
      """
      logger.info(f"Test 3 - Test Failure Modes when reading from fd:{rfd}")
      (dump_fd, reset_fd) = await _set_fd_state_for_writing(kind, rfd, wfd, content, content_size)
      _reset_buffer(content)
      try: fd_write(wfd, b'', 16384) # Empty Buffer
      except ValueError: pass
      try: fd_write(wfd, buffer, 0) # No write
      except ValueError: pass
      try: fd_write(wfd, buffer, -1) # negative write
      except ValueError: pass
      try: fd_write(wfd, buffer, 16384, -1) # Offset under bounds
      except ValueError: pass
      try: fd_write(wfd, buffer, 16384, len(buffer)) # Offset over bounds
      except ValueError: pass
      fd = _fd_duplicate(wfd)
      os.close(fd)
      err, _ = fd_write(fd, buffer, 16384)
      assert err == IOErrorReason.CLOSED, f"Writing to a closed FileDescriptor should exclusively result in a IOErrorReason of `{IOErrorReason.CLOSED.name}`: got {err.name}"
      await reset_fd; reset_fd = None
      logger.info("Test 3 -  Successful Completion")
    except AssertionError as ae: return TestResult(TestCode.FAIL, msg=str(ae))
    finally:
      if reset_fd is not None: await reset_fd; reset_fd = None
  return TestResult(TestCode.PASS)
