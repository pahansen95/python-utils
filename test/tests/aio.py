from __future__ import annotations
import asyncio, os, tempfile, random, string, socket
from typing import ContextManager, Literal, Generator, ByteString, Coroutine
from contextlib import contextmanager
from loguru import logger
from utils.testing import TestResult, TestCode

### Test Exports
__all__ = [
  "test_fd_read_file", "test_fd_read_stream",
  "test_fd_write_file", "test_fd_write_stream",
  "test_AsyncFileDescriptor_read_file", "test_AsyncFileDescriptor_read_stream",
  "test_AsyncFileDescriptor_write_file", "test_AsyncFileDescriptor_write_stream",
]
###

fd_kinds_t = Literal['file', 'unix', 'pipe']

@contextmanager
def _unix_socket_factory() -> Generator[tuple[int, int], None, None]:
  """Creates a Unix Socket returning a tuple of (read_fd, write_fd)"""
  server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
  client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
  socket_path = tempfile.mktemp()
  try:
    server_socket.bind(socket_path)
    server_socket.listen(1)
    client_socket.connect(socket_path)
    conn, _ = server_socket.accept()
    rfd, wfd = conn.fileno(), client_socket.fileno()
    os.set_blocking(rfd, False); os.set_blocking(wfd, False)
    yield (rfd, wfd)
  finally:
    server_socket.close()
    client_socket.close()
    if os.path.exists(socket_path):
      os.remove(socket_path)

@contextmanager
def _file_fd_factory() -> Generator[tuple[int, int], None, None]:
  """Creates a temporary file returning a tuple of (read_fd, write_fd)"""
  fd = tempfile.mkstemp()[0]
  try:
    os.set_blocking(fd, False)
    yield (fd, fd)
  finally:
    os.close(fd)

@contextmanager
def _pipe_fd_factory() -> Generator[tuple[int, int], None, None]:
  """Creates a pipe returning a tuple of (read_fd, write_fd)"""
  rfd, wfd = os.pipe()
  try:
    os.set_blocking(rfd, False); os.set_blocking(wfd, False)
    yield (rfd, wfd)
  finally:
    os.close(rfd); os.close(wfd)

def _fd_context_factory(kind: fd_kinds_t) -> ContextManager[tuple[int, int]]:
  if kind == 'file': return _file_fd_factory()
  elif kind == 'unix': return _unix_socket_factory()
  elif kind == 'pipe': return _pipe_fd_factory()
  else: raise ValueError(f"Invalid kind: '{kind}'")

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
  elif kind in ('unix', 'pipe'):
    async def _drain_stream(rfd: int, write_task: asyncio.Task):
      try:
        logger.debug(f"Draining fd:{rfd}")
        if not write_task.done():
          logger.debug(f"Waiting for Write Task to fd:{wfd} to cancel")
          write_task.cancel()
          try: await write_task
          except: pass
        while True:
          try:
            chunk = os.read(rfd, 16384)
          except OSError as err:
            if err.errno == 11: break # No more to read
            else: raise RuntimeError(f"Unexpected OSError encounted when writing to nonblocking {kind}: No. {err.errno}: {err.strerror}")  
      except:
        logger.opt(exception=True).debug("Error Encountered Draining Stream")
        raise
    async def _write_content_to_stream(wfd: int, content: ByteString, content_size: int):
      try:
        total_bytes_written = 0
        while total_bytes_written < content_size:
          try:
            total_bytes_written += os.write(wfd, content[total_bytes_written:content_size])
            logger.debug(f"{(100 * total_bytes_written / content_size):.1f}% written to fd:{wfd}")
          except OSError as err:
            if err.errno == 11: # Non Blocking Error
              await asyncio.sleep(0) # yield to event loop so we don't block the thread
              continue
            else: raise RuntimeError(f"Unexpected OSError encounted when writing to nonblocking {kind}: No. {err.errno}: {err.strerror}")
      except asyncio.CancelledError: return
      except: logger.opt(exception=True).debug("Error Encountered Writing Content to Stream")
    return _drain_stream(rfd, asyncio.create_task(_write_content_to_stream(wfd, content, content_size)))
  else: raise NotImplementedError(kind)

async def _set_fd_state_for_writing(
  kind: fd_kinds_t,
  rfd: int, wfd: int,
  content: ByteString, content_size: int
) -> tuple[Coroutine, Coroutine | None]:
  """Sets up a FD to be written to; return as tuple of Coroutines where the first returns the content written to the fd & the optional second cleans up any resources"""
  assert not os.get_blocking(rfd) and not os.get_blocking(wfd)
  if kind in ('file',):
    async def _dump_file(rfd: int) -> bytes:
      try:
        os.lseek(rfd, 0, os.SEEK_SET)
        buffer = bytearray()
        while True:
          try:
            chunk = os.read(rfd, 16384)
            if chunk == b'': break
            buffer.extend(chunk)
          except OSError as err:
            if err.errno == 11: await asyncio.sleep(0)
            else: raise RuntimeError(f"Unexpected OSError encounted when writing to nonblocking {kind}: No. {err.errno}: {err.strerror}")  
        return bytes(buffer)
      except:
        logger.opt(exception=True).debug(f"Error Encountered while Dumping fd{rfd} Contents")
        raise
    async def _reset_file(rfd: int, wfd: int) -> bytes:
      try:
        os.lseek(wfd, 0, os.SEEK_SET)
        os.truncate(wfd, 0)
        os.lseek(rfd, 0, os.SEEK_SET)
      except:
        logger.opt(exception=True).debug(f"Error Encountered while Reseting fd{wfd} Contents")
        raise
    return (_dump_file(rfd), _reset_file(rfd, wfd))
  elif kind in ('unix', 'pipe'):
    buffer = bytearray()
    async def _read_fd(rfd: int, buffer: bytearray):
      try:
        while len(buffer) < content_size:
          try:
            chunk = os.read(rfd, 16384)
            buffer.extend(chunk)
          except OSError as err:
            if err.errno == 11: await asyncio.sleep(0) # Blocking Error
            else:
              logger.opt(exception=err).warning(f"Error Reading FD: {err.errno}: {err.strerror}")
              return
      except asyncio.CancelledError: return
      except: logger.opt(exception=True).critical(f"Unhandled Error encounterd while reading from fd:{rfd}")

    async def _dump_fd(read_task: asyncio.Task, buffer: bytearray) -> bytes:
      if not read_task.done():
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
      except asyncio.CancelledError: return
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

  small_content, big_content = generate_rand_content(32, 1024*128)
  buffer = bytearray()
  def _reset_buffer(size: int):
    buffer.clear()
    buffer.extend(b'\x00' * size)
    assert len(buffer) == size

  stream_cleanup: Coroutine | None
  for (
    kind,
    (rfd, wfd),
    content,
    content_size,
  ) in _fd_combinations(
    ['unix', 'pipe'],
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
      await stream_cleanup; stream_cleanup = None
      assert err == IOErrorReason.BLOCK, f"When reading a stream after first exhausting it, the expected IOErrorReason is {IOErrorReason.BLOCK.name}; we got {err.name}"
      assert len(buffer) == len(content), f"content size mismatch: got {len(buffer)}; expected {len(content)}"
      assert buffer[:] == content[:], f"content mismatch: got {bytes(buffer)}; expected {bytes(content)}"
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
      await stream_cleanup; stream_cleanup = None
      assert err == IOErrorReason.BLOCK, f"When reading a stream after first exhausting it, the expected IOErrorReason is {IOErrorReason.BLOCK.name}; we got {err.name}"
      assert len(buffer) == len(content), f"content size mismatch: got {len(buffer)}; expected {len(content)}"
      assert buffer[:] == content[:], f"content mismatch: got {bytes(buffer)}; expected {bytes(content)}"
      logger.info("Test 2 -  Successful Completion")
    except AssertionError as ae: return TestResult(TestCode.FAIL, msg=str(ae))
    finally:
      if stream_cleanup is not None: await stream_cleanup

  for (
    kind,
    (rfd, wfd),
    _, _
  ) in _fd_combinations(
    ['unix', 'pipe'],
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
      await stream_cleanup; stream_cleanup = None
      assert err == IOErrorReason.CLOSED, f"Reading from a closed FileDescriptor should exclusively result in a IOErrorReason of `{IOErrorReason.CLOSED.name}`: got {err.name}"
      assert _empty_bytes[:] == b'', f"Reading from a closed FileDescriptor should exclusively result in an empty byte string: got `{_empty_bytes}`"
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
    ['unix', 'pipe'],
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

async def test_AsyncFileDescriptor_read_file(*args, **kwargs) -> TestResult:
  """Test Reading from an AsyncFile Descriptor backed by a file.

  """
  from utils.concurrency.aio import (
    AIOError,
    IOErrorReason, IOEvent, IOCondition,
    ItemLog,
  )
  from utils.concurrency.aio.fd import AsyncFileDescriptor
  from utils.concurrency.aio.watch import (
    IOWatcher, WatchBackend,
  )

  io_watcher = IOWatcher() # Create a seperate IOWatcher for this test
  small_content, big_content = generate_rand_content(32, 1024*128)
  buffer = bytearray()
  def _reset_buffer(size: int):
    buffer.clear()
    buffer.extend(b'\x00' * size)

  await io_watcher.start()
  try:
    for (
      kind,
      (rfd, wfd),
      content,
      content_size,
    ) in _fd_combinations(
      ['file',],
      [small_content, big_content],
    ):
      afd = AsyncFileDescriptor(rfd, 'file', io_watcher.register(rfd, IOEvent.READ | IOEvent.ERROR | IOEvent.CLOSE))
      try:
        """Test 1.1 - Read `n` Bytes"""
        logger.info("Test 1 Start")
        async with afd.event_log.lock():
          await afd.event_log.clear() # Reset the internal state of the AFD
        await _set_fd_state_for_reading(kind, rfd, wfd, content, content_size)
        _reset_buffer(0)
        _buffer = await afd.read(content_size)
        assert _buffer[:] == content[:], f"content mismatch: got {bytes(_buffer)}; expected {bytes(content)}"
        logger.info("Test 1.1 Complete")

        logger.info("Test 1.2 Start")
        async with afd.event_log.lock():
          await afd.event_log.clear() # Reset the internal state of the AFD
        await _set_fd_state_for_reading(kind, rfd, wfd, content, content_size)
        _reset_buffer(content_size)
        bytes_written = await afd.read(content_size, buffer, 0)
        assert bytes_written == content_size, f"content length mismatch: got {bytes_written}; expected {content_size}"
        assert buffer[:] == content[:], f"content mismatch: got {bytes(buffer)}; expected {bytes(content)}"
        logger.info("Test 1.2 Complete")

        logger.info("Test 1.3 Start")
        async with afd.event_log.lock():
          await afd.event_log.clear() # Reset the internal state of the AFD
        await _set_fd_state_for_reading(kind, rfd, wfd, content, content_size)
        _reset_buffer(content_size + 1)
        bytes_written = await afd.read(content_size, buffer, 1)
        assert bytes_written == content_size, f"content length mismatch: got {bytes_written}; expected {content_size}"
        assert buffer[1:] == content[:], f"content mismatch: got {bytes(buffer[1:])}; expected {bytes(content)}"
        logger.info("Test 1.3 Complete")

        """Test 2 - Read Bytes until `EOF` is encountered"""
        logger.info("Test 2.1 Start")
        async with afd.event_log.lock():
          await afd.event_log.clear() # Reset the internal state of the AFD
        await _set_fd_state_for_reading(kind, rfd, wfd, content, content_size)
        _reset_buffer(0)
        _buffer = await afd.read(IOCondition.EOF)
        assert _buffer[:] == content[:], f"content mismatch: got {bytes(_buffer)}; expected {bytes(content)}"
        logger.info("Test 2.1 Complete")

        logger.info("Test 2.2 Start")
        async with afd.event_log.lock():
          await afd.event_log.clear() # Reset the internal state of the AFD
        await _set_fd_state_for_reading(kind, rfd, wfd, content, content_size)
        _reset_buffer(content_size)
        bytes_written = await afd.read(IOCondition.EOF, buffer, 0)
        assert bytes_written == content_size, f"content length mismatch: got {bytes_written}; expected {content_size}"
        assert buffer[:] == content[:], f"content mismatch: got {bytes(buffer)}; expected {bytes(content)}"
        logger.info("Test 2.2 Complete")

        logger.info("Test 2.3 Start")
        async with afd.event_log.lock():
          await afd.event_log.clear() # Reset the internal state of the AFD
        await _set_fd_state_for_reading(kind, rfd, wfd, content, content_size)
        _reset_buffer(content_size + 1)
        bytes_written = await afd.read(IOCondition.EOF, buffer, 1)
        assert bytes_written == content_size, f"content length mismatch: got {bytes_written}; expected {content_size}"
        assert buffer[1:] == content[:], f"content mismatch: got {bytes(buffer[1:])}; expected {bytes(content)}"
        logger.info("Test 2.3 Complete")
      finally:
        io_watcher.unregister(rfd)
  finally:
    await io_watcher.stop()

  return TestResult(TestCode.PASS)

async def test_AsyncFileDescriptor_read_stream(*args, **kwargs) -> TestResult:
  """Test Reading from an AsyncFile Descriptor backed by a stream.

  """
  from utils.concurrency.aio import (
    IOEvent, IOCondition,
  )
  from utils.concurrency.aio.fd import AsyncFileDescriptor
  from utils.concurrency.aio.watch import (
    IOWatcher,
  )

  io_watcher = IOWatcher() # Create a seperate IOWatcher for this test
  small_content, big_content = generate_rand_content(32, 1024*128)
  buffer = bytearray()
  stream_cleanup = None
  def _reset_buffer(size: int):
    buffer.clear()
    buffer.extend(b'\x00' * size)

  await io_watcher.start()
  try:
    for (
      kind,
      (rfd, wfd),
      content,
      content_size,
    ) in _fd_combinations(
      ['unix', 'pipe'],
      [small_content, big_content],
    ):
      afd = AsyncFileDescriptor(rfd, 'stream', io_watcher.register(rfd, IOEvent.READ | IOEvent.ERROR | IOEvent.CLOSE))
      try:
        """Test 1.1 - Read `n` Bytes"""
        logger.info("Test 1 Start")
        async with afd.event_log.lock():
          await afd.event_log.clear() # Reset the internal state of the AFD
        stream_cleanup = await _set_fd_state_for_reading(kind, rfd, wfd, content, content_size)
        _reset_buffer(0)
        _buffer = await afd.read(content_size)
        _empty_bytes = await afd.read(IOCondition.BLOCK)
        await stream_cleanup; stream_cleanup = None
        assert _buffer[:] == content[:], f"content mismatch: got {bytes(_buffer)}; expected {bytes(content)}"
        assert _empty_bytes == b'', f"Reading after exhausting the stream expects an empty ByteString but got `{_empty_bytes}`"
        logger.info("Test 1.1 Complete")

        logger.info("Test 1.2 Start")
        async with afd.event_log.lock():
          await afd.event_log.clear() # Reset the internal state of the AFD
        stream_cleanup = await _set_fd_state_for_reading(kind, rfd, wfd, content, content_size)
        _reset_buffer(content_size)
        bytes_written = await afd.read(content_size, buffer, 0)
        _n_is_0 = await afd.read(IOCondition.BLOCK, buffer, 0)
        await stream_cleanup; stream_cleanup = None
        assert bytes_written == content_size, f"content length mismatch: got {bytes_written}; expected {content_size}"
        assert buffer[:] == content[:], f"content mismatch: got {bytes(buffer)}; expected {bytes(content)}"
        assert _n_is_0 == 0, f"Reading after exhausting the stream expects an a read length of 0 but got `{_n_is_0}`"
        logger.info("Test 1.2 Complete")

        logger.info("Test 1.3 Start")
        async with afd.event_log.lock():
          await afd.event_log.clear() # Reset the internal state of the AFD
        stream_cleanup = await _set_fd_state_for_reading(kind, rfd, wfd, content, content_size)
        _reset_buffer(content_size + 1)
        bytes_written = await afd.read(content_size, buffer, 1)
        await stream_cleanup; stream_cleanup = None
        assert bytes_written == content_size, f"content length mismatch: got {bytes_written}; expected {content_size}"
        assert buffer[1:] == content[:], f"content mismatch: got {bytes(buffer[1:])}; expected {bytes(content)}"
        logger.info("Test 1.3 Complete")
      finally:
        io_watcher.unregister(rfd)
  finally:
    if stream_cleanup is not None: await stream_cleanup
    await io_watcher.stop()

  return TestResult(TestCode.PASS)

async def test_AsyncFileDescriptor_write_file(*args, **kwargs) -> TestResult:
  from utils.concurrency.aio import (
    IOEvent,
  )
  from utils.concurrency.aio.fd import AsyncFileDescriptor
  from utils.concurrency.aio.watch import (
    IOWatcher,
  )

  io_watcher = IOWatcher() # Create a seperate IOWatcher for this test
  reset_fd = None
  small_content, big_content = generate_rand_content(32, 1024*128)
  buffer = bytearray()
  def _reset_buffer(content: bytes):
    buffer.clear()
    buffer.extend(content)

  await io_watcher.start()
  try:
    for (
      kind,
      (rfd, wfd),
      content,
      content_size,
    ) in _fd_combinations(
      ['file',],
      [small_content, big_content],
    ):
      afd = AsyncFileDescriptor(wfd, 'file', io_watcher.register(wfd, IOEvent.WRITE | IOEvent.ERROR | IOEvent.CLOSE))
      try:
        """Test 1.1 - Write `n` Bytes"""
        logger.info("Test 1.1 Start")
        async with afd.event_log.lock():
          await afd.event_log.clear() # Reset the internal state of the AFD
        (dump_fd, reset_fd) = await _set_fd_state_for_writing(kind, rfd, wfd, content, content_size)
        _reset_buffer(content)
        bytes_written = await afd.write(buffer, content_size)
        _fd_content = await dump_fd; await reset_fd; reset_fd = None
        assert bytes_written == content_size, f"content length mismatch: got {bytes_written}; expected {content_size}"
        assert _fd_content[:] == content[:], f"content mismatch writing to fd{wfd}:\n{render_diff(bytes(_fd_content), bytes(content))}"
        logger.info("Test 1.1 Complete")

        logger.info("Test 1.2 Start")
        async with afd.event_log.lock():
          await afd.event_log.clear() # Reset the internal state of the AFD
        (dump_fd, reset_fd) = await _set_fd_state_for_writing(kind, rfd, wfd, content, content_size)
        _reset_buffer(content)
        bytes_written = await afd.write(buffer, content_size - 1, 1)
        _fd_content = await dump_fd; await reset_fd; reset_fd = None
        assert bytes_written == content_size - 1, f"content length mismatch: got {bytes_written}; expected {content_size - 1}"
        assert _fd_content[:] == content[1:], f"content mismatch writing to fd{wfd}:\n{render_diff(bytes(_fd_content), bytes(content[1:]))}"
        logger.info("Test 1.2 Complete")
      finally:
        io_watcher.unregister(rfd)
  finally:
    if reset_fd is not None: await reset_fd
    await io_watcher.stop()

  return TestResult(TestCode.PASS)

async def test_AsyncFileDescriptor_write_stream(*args, **kwargs) -> TestResult:
  from utils.concurrency.aio import (
    IOEvent
  )
  from utils.concurrency.aio.fd import AsyncFileDescriptor
  from utils.concurrency.aio.watch import (
    IOWatcher,
  )

  io_watcher = IOWatcher() # Create a seperate IOWatcher for this test
  reset_fd = None
  small_content, big_content = generate_rand_content(32, 1024*128)
  buffer = bytearray()
  def _reset_buffer(content: bytes):
    buffer.clear()
    buffer.extend(content)

  await io_watcher.start()
  try:
    for (
      kind,
      (rfd, wfd),
      content,
      content_size,
    ) in _fd_combinations(
      ['unix', 'pipe'],
      [small_content, big_content],
    ):
      afd = AsyncFileDescriptor(wfd, 'stream', io_watcher.register(wfd, IOEvent.WRITE | IOEvent.ERROR | IOEvent.CLOSE))
      try:
        """Test 1.1 - Write `n` Bytes"""
        logger.info("Test 1.1 Start")
        (dump_fd, reset_fd) = await _set_fd_state_for_writing(kind, rfd, wfd, content, content_size)
        _reset_buffer(content)
        bytes_written = await afd.write(buffer, content_size)
        _fd_content = await dump_fd; await reset_fd; reset_fd = None
        assert bytes_written == content_size, f"content length mismatch: got {bytes_written}; expected {content_size}"
        assert _fd_content[:] == content[:], f"content mismatch writing to fd{wfd}:\n{render_diff(bytes(_fd_content), bytes(content))}"
        logger.info("Test 1.1 Complete")

        logger.info("Test 1.2 Start")
        (dump_fd, reset_fd) = await _set_fd_state_for_writing(kind, rfd, wfd, content, content_size - 1)
        _reset_buffer(content)
        bytes_written = await afd.write(buffer, content_size - 1, 1)
        assert bytes_written == content_size - 1
        _fd_content = await dump_fd; await reset_fd; reset_fd = None
        assert bytes_written == content_size - 1, f"content length mismatch: got {bytes_written}; expected {content_size - 1}"
        assert _fd_content[:] == content[1:], f"content mismatch writing to fd{wfd}:\n{render_diff(bytes(_fd_content), bytes(content[1:]))}"
        logger.info("Test 1.2 Complete")
      finally:
        io_watcher.unregister(wfd)
  finally:
    if reset_fd is not None: await reset_fd
    await io_watcher.stop()

  return TestResult(TestCode.PASS)
