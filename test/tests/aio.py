import asyncio
from loguru import logger
from typing import Literal, ContextManager, Generator
from contextlib import contextmanager
import tempfile
import os
from utils.concurrency import _log_trapper
from utils.testing import TestResult, TestCode

def _fd_context_factory(kind: Literal['file', 'pty', 'pipe'],) -> ContextManager[tuple[int, int]]:
  
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

async def test_fd_read(*args, **kwargs) -> TestResult:
  from utils.concurrency.aio import (
    IOErrorReason,
    aio_backend, AIOBackend
  )
  if aio_backend != AIOBackend.LINUX: return TestResult(TestCode.SKIP, "fd_read is only supported on Linux")
  from utils.concurrency.aio.linux import (
    fd_read,
  )
  import os

  for fd_kind in ('file', 'pty', 'pipe'):
    with _fd_context_factory(fd_kind) as (rfd, wfd):
      content = b"Hello, World!"
      content_size = len(content)
      fd = os.dup(rfd)
      os.write(wfd, content)
      fd_closed = False
      buffer_size = content_size * 2
      buffer = bytearray()

      def _reset_state() -> None:
        nonlocal fd
        nonlocal fd_closed
        # If the file descriptor is closed, then we need to reopen it
        if fd_closed:
          fd = os.dup(rfd)
          fd_closed = False
        if fd_kind in ('file',): os.lseek(fd, 0, os.SEEK_SET)
        if fd_kind in ('pty', 'pipe'): # Reset the buffer if it's a stream
          try:
            os.read(rfd, content_size)
          except OSError as e:
            if e.errno == 11: pass
            else: raise
          os.write(wfd, content)
        buffer.clear()
        buffer.extend(b'\x00' * buffer_size)
      
      def _cleanup_state() -> None:
        if not fd_closed: os.close(fd)

      ### Read Tests ###

      try:
        ### Test reading the entire fd into a buffer ###
        _reset_state()      
        err, n = fd_read(fd, content_size, buffer, 0)
        assert err == IOErrorReason.NONE, f"Expected {IOErrorReason.NONE.name}, got {err.name}"
        assert n == content_size, f"Expected {content_size}, got {n}"
        assert buffer[:n] == content, f"Expected {content}, got {buffer[:n]}"

        ### Test reading the entire file into a buffer at an offset ###
        _reset_state()
        err, n = fd_read(fd, content_size, buffer, content_size)
        assert err == IOErrorReason.NONE, f"Expected {IOErrorReason.NONE.name}, got {err.name}"
        assert n == content_size, f"Expected {content_size}, got {n}"
        assert buffer[content_size:content_size+n] == content, f"Expected {content}, got {buffer[content_size:content_size+n]}"

        ### Test reading the entire file without passing a buffer ###
        _reset_state()
        err, _read_content = fd_read(fd, content_size)
        assert err == IOErrorReason.NONE, f"Expected {IOErrorReason.NONE.name}, got {err.name}"
        assert len(_read_content) == content_size, f"Expected {content_size}, got {len(_read_content)}"
        assert _read_content == content, f"Expected {content}, got {_read_content}"

        ### Test that into_offset is ignored when no buffer is passed ###
        _reset_state()
        err, _read_content = fd_read(fd, content_size, into_offset=content_size)
        assert err == IOErrorReason.NONE, f"Expected {IOErrorReason.NONE.name}, got {err.name}"
        assert len(_read_content) == content_size, f"Expected {content_size}, got {len(_read_content)}"
        assert _read_content == content, f"Expected {content}, got {_read_content}"

        ### Test that reading a second time returns EOF or BLOCK ###
        err, _read_content = fd_read(fd, content_size)
        if fd_kind in ('file',): assert err == IOErrorReason.EOF, f"Expected {IOErrorReason.EOF.name}, got {err.name}"
        elif fd_kind in ('pty', 'pipe',): assert err == IOErrorReason.BLOCK, f"Expected {IOErrorReason.BLOCK.name}, got {err.name}"
        assert _read_content == b'', f"Expected empty bytes, got {_read_content}"

        ### Test for proper exception handling ###
        _reset_state()

        # Invalid Read Size
        try:
          fd_read(fd, 0)
          fd_read(fd, -1)
          assert False, "Expected ValueError to be raised"
        except ValueError as e:
          pass

        # Undersized Buffer
        try:
          fd_read(fd, content_size, bytearray()) # Undersized
          fd_read(fd, content_size, buffer, buffer_size - 1) # Undersized after offset
          assert False, "Expected ValueError to be raised"
        except ValueError as e:
          pass
        
        # Out of Bounds Buffer Offset
        try:
          fd_read(fd, content_size, buffer, -1)
          fd_read(fd, content_size, buffer, buffer_size)
          assert False, "Expected ValueError to be raised"
        except ValueError as e:
          pass

        ### Test that closing the file descriptor returns CLOSED ###
        _reset_state()
        os.close(fd); fd_closed = True
        err, _read_content = fd_read(fd, content_size)
        assert err == IOErrorReason.CLOSED, f"Expected {IOErrorReason.CLOSED.name}, got {err.name}"
        assert _read_content == b'', f"Expected empty bytes, got {_read_content}"

      except AssertionError as e:
        logger.opt(exception=True).error("Assertion Error")
        return TestResult(TestCode.FAIL, fd_kind + ": " + str(e))
      except:
        logger.warning(f"Unexpected error when testing `{fd_kind}`")
        raise
      finally:
        _cleanup_state()

  return TestResult(TestCode.PASS)

async def test_fd_write(*args, **kwargs) -> TestResult:
  from utils.concurrency.aio import (
    IOErrorReason,
    aio_backend, AIOBackend
  )
  if aio_backend != AIOBackend.LINUX: return TestResult(TestCode.SKIP, "fd_write is only supported on Linux")
  from utils.concurrency.aio.linux import (
    fd_write,
  )
  import os

  for fd_kind in ('file', 'pty', 'pipe'):
    with _fd_context_factory(fd_kind) as (rfd, wfd):
      content = b"Hello, World!"
      content_size = len(content)
      fd = os.dup(wfd)
      fd_closed = False
      buffer_size = content_size * 2
      buffer = bytes(content * 2)

      def _reset_state() -> None:
        nonlocal fd
        nonlocal fd_closed
        # If the file descriptor is closed, then we need to reopen it
        if fd_closed:
          fd = os.dup(wfd)
          fd_closed = False
        if fd_kind in ('file',): # Truncate the File
          os.truncate(fd, 0)
          os.lseek(fd, 0, os.SEEK_SET)
        if fd_kind in ('pty', 'pipe'): # Clear the Stream
          try:
            os.read(rfd, buffer_size)
          except OSError as e:
            if e.errno == 11: pass
            else: raise
      
      def _cleanup_state() -> None:
        if not fd_closed: os.close(fd)
      
      def _dump_fd() -> bytes:
        if fd_kind in ('file',): os.lseek(rfd, 0, os.SEEK_SET)
        return os.read(rfd, buffer_size)

      ### Write Tests ###
      try:
        ### Test writing the entire buffer to the file ###
        _reset_state()
        err, n = fd_write(fd, buffer, content_size)
        _file_bytes = _dump_fd()
        assert err == IOErrorReason.NONE, f"Expected {IOErrorReason.NONE.name}, got {err.name}"
        assert n == content_size, f"Expected {content_size}, got {n}"
        assert _file_bytes[:n] == content, f"Expected {content}, got {_file_bytes[:n]}"
      
        ### Test writing the buffer starting at an offset to the file ###
        _reset_state()
        err, n = fd_write(fd, buffer, content_size, 4)
        _file_bytes = _dump_fd()
        assert err == IOErrorReason.NONE, f"Expected {IOErrorReason.NONE.name}, got {err.name}"
        assert n == content_size, f"Expected {content_size}, got {n}"
        assert _file_bytes == buffer[4:4+content_size], f"Expected {buffer[4:4+content_size]}, got {_file_bytes}"

        ### Test writing more bytes than are in the buffer ###
        _reset_state()
        err, n = fd_write(fd, buffer, buffer_size * 2)
        _file_bytes = _dump_fd()
        assert err == IOErrorReason.NONE, f"Expected {IOErrorReason.NONE.name}, got {err.name}"
        assert n == buffer_size, f"Expected {buffer_size}, got {n}: '{_file_bytes}'"
        assert _file_bytes == buffer, f"Expected {buffer}, got {_file_bytes}"

        ### Test writing more bytes than are in the buffer starting at an offset ###
        _reset_state()
        err, n = fd_write(fd, buffer, buffer_size * 2, 4)
        _file_bytes = _dump_fd()
        assert err == IOErrorReason.NONE, f"Expected {IOErrorReason.NONE.name}, got {err.name}"
        assert n == buffer_size - 4, f"Expected {buffer_size}, got {n}"
        assert _file_bytes == buffer[4:], f"Expected {buffer[4:]}, got {_file_bytes}"

        ### Test Proper Exception Handling ###
        _reset_state()
        try:
          err, n = fd_write(fd, buffer, content_size, -1)
          assert False, "Expected ValueError to be raised"
        except ValueError as e:
          pass

        try:
          err, n = fd_write(fd, buffer, -1)
          err, n = fd_write(fd, buffer, 0)
          assert False, "Expected ValueError to be raised"
        except ValueError as e:
          pass

        try:
          err, n = fd_write(fd, b'', 1)
          assert False, "Expected ValueError to be raised"
        except ValueError as e:
          pass

        ### Test that closing the file descriptor returns CLOSED ###
        os.close(fd); fd_closed = True
        err, n = fd_write(fd, buffer, content_size)
        assert err == IOErrorReason.CLOSED, f"Expected {IOErrorReason.CLOSED.name}, got {err.name}"
        assert n == 0, f"Expected 0, got {n}"
      except AssertionError as e:
        logger.opt(exception=True).error("Assertion Error")
        return TestResult(TestCode.FAIL, fd_kind + ": " + str(e))
      except:
        logger.warning(f"Unexpected error when testing `{fd_kind}`")
        raise
      finally:
        _cleanup_state()

  return TestResult(TestCode.PASS)

async def test_AsyncFileDescriptor_read(*args, **kwargs) -> TestResult:
  """Test the AsyncFileDescriptor Reading Interface"""
  
  from utils.concurrency import _log_trapper
  from utils.concurrency.aio import (
    AIOError,
    IOErrorReason, IOEvent, IOCondition,
    ItemLog,
  )
  from utils.concurrency.aio.fd import AsyncFileDescriptor
  from utils.concurrency.aio.watch import (
    IOWatcher, WatchBackend,
  )
  import os
  import tempfile

  # Create a seperate IOWatcher for this test
  io_watcher = IOWatcher()

  # with tempfile.NamedTemporaryFile(mode='r+b') as f:
  #   content = b"Hello, World!"
  #   content_size = len(content)
  #   if f.write(content) < content_size: raise RuntimeError("Failed to write content to file")
  #   f.seek(0)
  #   f.flush()
  #   os.set_blocking(f.fileno(), False)
  #   fd = os.dup(f.fileno())
  #   fd_closed = False
  #   buffer_size = content_size * 2
  #   buffer = bytearray()

  #   afd = AsyncFileDescriptor(fd, io_watcher.register(fd, IOEvent.READ | IOEvent.ERROR | IOEvent.CLOSE))
  #   await io_watcher.start()

  #   async def _reset_state() -> None:
  #     nonlocal fd
  #     nonlocal afd
  #     nonlocal fd_closed
  #     f.seek(0)
  #     # If the file descriptor is closed, then we need to reopen it
  #     if fd_closed:
  #       fd = os.dup(f.fileno())
  #       fd_closed = False
  #       await io_watcher.stop()
  #       io_watcher.unregister(fd)
  #       afd = AsyncFileDescriptor(afd, io_watcher.register(fd, IOEvent.READ | IOEvent.ERROR | IOEvent.CLOSE))
  #       await io_watcher.start()
  #     if fd_kind in ('file',): os.lseek(fd, 0, os.SEEK_SET)
  #     buffer.clear()
  #     buffer.extend(b'\x00' * buffer_size)
    
  #   async def _cleanup_state() -> None:
  #     await io_watcher.stop()
  #     io_watcher.unregister(fd)
  #     if not fd_closed: os.close(fd)
    
  #   def _dump_file(offset: int = 0) -> bytes:
  #     f.seek(offset)
  #     return f.read(content_size)

  #   ### Read Tests ###
  for fd_kind in ('file', 'pty', 'pipe'):
  # for fd_kind in ('pty',):
    _io_cond = IOCondition.BLOCK if fd_kind in ('pty', 'pipe') else IOCondition.EOF
    with _fd_context_factory(fd_kind) as (rfd, wfd):
      content = b"Hello, World!"
      content_size = len(content)
      fd = os.dup(rfd)
      fd_closed = False
      buffer_size = content_size * 2
      buffer = bytearray()
      await io_watcher.start()
      afd = AsyncFileDescriptor(fd, io_watcher.register(fd, IOEvent.READ | IOEvent.ERROR | IOEvent.CLOSE))

      @_log_trapper
      async def _reset_state() -> None:
        nonlocal afd
        nonlocal fd
        nonlocal fd_closed
        io_watcher.unregister(fd)
        # If the file descriptor is closed, then we need to reopen it
        if fd_closed:
          fd = os.dup(rfd)
          fd_closed = False
        if fd_kind in ('file',):
          os.lseek(wfd, 0, os.SEEK_SET)
          os.truncate(wfd, 0)
          os.write(wfd, content)
          os.lseek(fd, 0, os.SEEK_SET)
        if fd_kind in ('pty', 'pipe'): # Reset the buffer if it's a stream
          try:
            os.read(rfd, buffer_size)
          except OSError as e:
            if e.errno == 11: pass
            else: raise
          os.write(wfd, content)
        buffer.clear()
        buffer.extend(b'\x00' * buffer_size)
        afd = AsyncFileDescriptor(fd, io_watcher.register(fd, IOEvent.READ | IOEvent.ERROR | IOEvent.CLOSE))
      
      @_log_trapper
      async def _cleanup_state() -> None:
        io_watcher.unregister(fd)
        if not fd_closed: os.close(fd)
        await io_watcher.stop()

      ### Read Tests ###

      try:

        ### Test reading the entire file into a buffer by size###
        await _reset_state()
        n = await afd.read(content_size, buffer)
        assert n == content_size, f"Expected {content_size}, got {n}"
        assert buffer[:n] == content, f"Expected {content}, got {buffer[:n]}"
        
        ### Test reading the entire file into a buffer by EoF###
        await _reset_state()
        n = await afd.read(_io_cond, buffer)
        assert n == content_size, f"Expected {content_size}, got {n}: '{buffer[:n]}'"
        assert buffer[:n] == content, f"Expected {content}, got {buffer[:n]}"

        ### Test reading the entire file into a buffer at an offset ###
        await _reset_state()
        n = await afd.read(content_size, buffer, content_size)
        assert n == content_size, f"Expected {content_size}, got {n}"
        assert buffer[content_size:content_size+n] == content, f"Expected {content}, got {buffer[content_size:content_size+n]}"

        ### Test reading the entire file without passing a buffer ###
        await _reset_state()
        _read_content = await afd.read(content_size)
        assert len(_read_content) == content_size, f"Expected {content_size}, got {len(_read_content)}"
        assert _read_content == content, f"Expected {content}, got {_read_content}"

        ### Test that into_offset is ignored when no buffer is passed ###
        await _reset_state()
        _read_content = await afd.read(content_size, buffer_offset=content_size)
        assert len(_read_content) == content_size, f"Expected {content_size}, got {len(_read_content)}"
        assert _read_content == content, f"Expected {content}, got {_read_content}"

        ### Test that reading a second time returns EoF or blocks ###
        await _reset_state()
        _read_content = await afd.read(content_size)
        assert _read_content == content, f"Expected {content}, got {_read_content}"
        if fd_kind in ('file',):
          _read_content = await afd.read(content_size)
          assert _read_content == b'', f"Expected empty bytes, got {_read_content}"
        elif fd_kind in ('pty', 'pipe',):
          try:
            async with asyncio.timeout(0.1):
              _read_content = await afd.read(content_size)
          except asyncio.TimeoutError:
            pass

        ### Test for proper exception handling ###
        await _reset_state()
        
        # Undersized Buffer
        for args in [
          # Invalid read Sizes
          (0,),
          (-1,),
          # Undersized buffers
          (content_size, bytearray()),
          (content_size, buffer, buffer_size - 1),
          # Invalid/Out of Bounds Buffer Offset
          (content_size, buffer, -1),
          (content_size, buffer, buffer_size),
        ]:
          try:
            result = await afd.read(*args) # Undersized
            assert False, f"Expected ValueError to be raised for `{args}`: got `{result}`"
          except ValueError as e:
            pass

      except AssertionError as e:
        logger.opt(exception=True).error("Assertion Error")
        return TestResult(TestCode.FAIL, fd_kind + ": " + str(e))
      except:
        logger.warning(f"Unexpected error when testing `{fd_kind}`")
        raise
      finally:
        await _cleanup_state()

  return TestResult(TestCode.PASS)

async def test_AsyncFileDescriptor_write(*args, **kwargs) -> TestResult:
  """Test the AsyncFileDescriptor Writing Interface"""
  
  from utils.concurrency.aio import (
    AIOError,
    IOErrorReason, IOEvent, IOCondition,
    ItemLog, 
  )
  from utils.concurrency.aio.fd import AsyncFileDescriptor
  from utils.concurrency.aio.watch import (
    IOWatcher, WatchBackend,
  )
  import os
  import tempfile

  # Create a seperate IOWatcher for this test
  io_watcher = IOWatcher()

  # with tempfile.NamedTemporaryFile(mode='r+b') as f:
  #   content = b"Hello, World!"
  #   content_size = len(content)
  #   f.write(content)
  #   f.flush()
  #   os.set_blocking(f.fileno(), False)
  #   fd = os.dup(f.fileno())
  #   fd_closed = False
  #   buffer_size = content_size * 2
  #   buffer = bytes(content * 2)

  #   afd = AsyncFileDescriptor(fd, io_watcher.register(fd, IOEvent.WRITE | IOEvent.ERROR | IOEvent.CLOSE))
  #   await io_watcher.start()

  #   async def _reset_state() -> None:
  #     nonlocal fd
  #     nonlocal afd
  #     nonlocal fd_closed
  #     # nonlocal fd_kind
  #     f.seek(0)
  #     # If the file descriptor is closed, then we need to reopen it
  #     if fd_closed:
  #       fd = os.dup(f.fileno())
  #       fd_closed = False
  #       await io_watcher.stop()
  #       io_watcher.unregister(fd)
  #       afd = AsyncFileDescriptor(afd, io_watcher.register(fd, IOEvent.WRITE | IOEvent.ERROR | IOEvent.CLOSE))
  #       await io_watcher.start()
  #     if fd_kind in ('file',): os.lseek(fd, 0, os.SEEK_SET)
    
  #   async def _cleanup_state() -> None:
  #     await io_watcher.stop()
  #     io_watcher.unregister(fd)
  #     if not fd_closed: os.close(fd)
    
  #   def _dump_file(offset: int = 0) -> bytes:
  #     f.seek(offset)
  #     return f.read(content_size)
  for fd_kind in ('file', 'pty', 'pipe'):
  # for fd_kind in ('pty',):
    _io_cond = IOCondition.BLOCK if fd_kind in ('pty', 'pipe') else IOCondition.EOF
    with _fd_context_factory(fd_kind) as (rfd, wfd):
      content = b"Hello, World!"
      content_size = len(content)
      fd = os.dup(wfd)
      fd_closed = False
      buffer_size = content_size * 2
      buffer = bytes(content * 2)
      await io_watcher.start()
      afd = AsyncFileDescriptor(fd, io_watcher.register(fd, IOEvent.WRITE | IOEvent.ERROR | IOEvent.CLOSE))

      @_log_trapper
      async def _reset_state() -> None:
        nonlocal afd
        nonlocal fd
        nonlocal fd_closed
        io_watcher.unregister(fd)
        # If the file descriptor is closed, then we need to reopen it
        if fd_closed:
          fd = os.dup(wfd)
          fd_closed = False
        if fd_kind in ('file',):
          os.lseek(wfd, 0, os.SEEK_SET)
          os.truncate(wfd, 0)
          os.lseek(fd, 0, os.SEEK_SET)
        if fd_kind in ('pty', 'pipe'): # Reset the buffer if it's a stream
          try:
            os.read(rfd, buffer_size)
          except OSError as e:
            if e.errno == 11: pass
            else: raise
        afd = AsyncFileDescriptor(fd, io_watcher.register(fd, IOEvent.WRITE | IOEvent.ERROR | IOEvent.CLOSE))
      
      @_log_trapper
      async def _cleanup_state() -> None:
        io_watcher.unregister(fd)
        if not fd_closed: os.close(fd)
        await io_watcher.stop()
      
      def _dump_fd() -> bytes:
        assert os.get_blocking(rfd) == False
        buf: bytes | None
        if fd_kind in ("file",): os.lseek(fd, 0, os.SEEK_SET); buf = os.read(rfd, os.stat(rfd).st_size)
        elif fd_kind in ("pty", "pipe"): buf = os.read(rfd, 16 * 1024) 
        else: raise NotImplementedError(fd_kind)
        assert buf is not None
        return buf

    ### Write Tests ###

      try:

        ### Test writing the entire buffer to the file ###
        await _reset_state()
        n = await afd.write(buffer, content_size)
        _fd_bytes = _dump_fd()
        assert n == content_size, f"Expected {content_size}, got {n}"
        assert _fd_bytes[:n] == content, f"Expected {content}, got {_fd_bytes[:n]}"

        ### Test writing the buffer starting at an offset to the file ###
        await _reset_state()
        n = await afd.write(buffer, content_size, 4)
        _fd_bytes = _dump_fd()
        assert n == content_size, f"Expected {content_size}, got {n}"
        assert _fd_bytes == buffer[4:4+content_size], f"Expected {buffer[4:4+content_size]}, got {_fd_bytes}"

        ### Test Proper Exception Handling ###
        await _reset_state()
        for args in [
          # Invalid Write Size
          (-1,),
          (0,),
          # Undersized Buffer
          (content_size, -1),
          (content_size, buffer_size),
          # Out of Bounds Buffer Offset
          (content_size, -1),
          (content_size, buffer_size),
        ]:
          try:
            result = await afd.write(buffer, *args) # Undersized
            assert False, f"Expected ValueError to be raised for `{(buffer, *args)}`: got `{result}`"
          except ValueError as e:
            pass

      except AssertionError as e:
        logger.opt(exception=True).error("Assertion Error")
        return TestResult(TestCode.FAIL, str(e))
      finally:
        await _cleanup_state()

  return TestResult(TestCode.PASS)
    
__all__ = [
  "test_fd_read",
  "test_fd_write",
  "test_AsyncFileDescriptor_read",
  "test_AsyncFileDescriptor_write",
]