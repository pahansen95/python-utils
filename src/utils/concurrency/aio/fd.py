from __future__ import annotations
from dataclasses import dataclass, field, KW_ONLY
import asyncio, os, sys, fcntl
from loguru import logger
from collections.abc import Coroutine, Callable
from typing import TypeVar, Any, Literal, ByteString
from . import (
  aio_backend, AIOBackend,
  IOErrorReason, IOEvent, IOCondition,
  AIOError,
  ItemLog,
  CHUNK_SIZE
)
from .. import _log_trapper

T = TypeVar("T")

if aio_backend == AIOBackend.LINUX:
  from .linux import (
    fd_read,
    fd_write,
  )
else: raise NotImplementedError(aio_backend)

@dataclass(frozen=True)
class AsyncFileDescriptor:
  """An Asynchronous IO Protocol for a File Descriptor. Management of the File Descriptor lifecycle (ie. open & close) is left to the caller."""

  fd: int
  """The (already opened) File Descriptor."""
  kind: Literal['file','stream']
  """The type of FileDescriptor we are read/writing from"""
  event_log: ItemLog[IOEvent] = field(default_factory=ItemLog)
  """The Events that occurred on the File Descriptor."""

  def __post_init__(self):
    if self.kind not in ('file', 'stream'): raise ValueError(f"Invalid FD Kind `{self.kind}`")

  @property
  def closed(self) -> bool:
    """Checks whether the File Descriptor is closed (using fcntl)."""
    try:
      fcntl.fcntl(self.fd, fcntl.F_GETFD)
      return False
    except OSError:
      return True

  async def _read(self, n: int) -> bytes:
    """Read N Bytes returning a bytes string"""
    # Initialize state
    if n <= 0: raise ValueError("n must be greater than 0")
    buffer = bytearray(n)
    err: IOErrorReason = IOErrorReason.NONE
    fd_event: IOEvent = IOEvent.ERROR
    total_bytes_read = 0

    # Start reading
    while True:
      fd_event: IOEvent | None = await self.event_log.peek()
      # Attempt to read
      if fd_event == IOEvent.READ or fd_event == IOEvent.CLOSE:
        assert total_bytes_read < n # NOTE: I don't think we should ever encounter this state
        err, bytes_read = fd_read(self.fd, n - total_bytes_read, buffer, total_bytes_read)
      elif fd_event == IOEvent.ERROR: raise AIOError(IOErrorReason.ERROR, f"error reading from file descriptor {self.fd}")
      else: raise NotImplementedError(fd_event)

      total_bytes_read += bytes_read
      
      ### NOTE: Event Handling
      # Conditions when the IOEvent should be consumed from the Event Log:
      #   - when no bytes are returned during a read
      #   - the fd is no longer ready to be read from
      ###
      if (
        (err == IOErrorReason.NONE and bytes_read <= 0)
        or (err == IOErrorReason.BLOCK or err == IOErrorReason.EOF)
      ): await self.event_log.pop()

      ### NOTE: Flow Control - Break if no more can be read, or we have met instructed requirements
      # 
      # When should we error out?
      # 
      # - If IO Error is ERROR or UNHANDLED: Raise an AIOError
      # 
      # When should we break?
      # 
      # - total_bytes_read >= n: Break
      # - if IOError one of EOF|BLOCK|CLOSED: Break
      # 
      # When should we continue?
      # 
      # - if IOError one of NONE|INTERRUPT: Continue
      #  
      # NOTES:
      # 
      # - For Streams, a BlOCK event is the equivalent of a EOF for a File.
      # - The Most Common Error Reason is NONE followed by EOF/BLOCK
      #
      ###

      if total_bytes_read >= n or (err == IOErrorReason.EOF or err == IOErrorReason.CLOSED): break
      elif err == IOErrorReason.BLOCK or err == IOErrorReason.NONE or err == IOErrorReason.INTERRUPT: continue
      elif err == IOErrorReason.ERROR or err == IOErrorReason.UNHANDLED: raise AIOError(err, f"error writing to file descriptor {self.fd}")
      else: raise NotImplementedError(f"Unhandled State encountered: IOErrorReason={err.name},IOEvent={fd_event.name},TotalBytesRead={total_bytes_read},LastBytesRead={bytes_read}")

    return bytes(buffer[:total_bytes_read])

  async def _read_into(self, n: int, buffer: bytearray | memoryview, buffer_offset: int) -> int:
    """Read N Bytes into a Fixed Size Buffer"""
    # Initialize state
    if n <= 0: raise ValueError("n must be greater than 0")
    err: IOErrorReason = IOErrorReason.NONE
    fd_event: IOEvent = IOEvent.ERROR
    total_bytes_read = 0
    buf_size = len(buffer) - buffer_offset
    if buf_size < n: raise ValueError(f"Buffer is too small: needs {n} bytes but has {buf_size} bytes available after offset of {buffer_offset} bytes")

    # Start reading
    while True:
      fd_event: IOEvent | None = await self.event_log.peek()
      # Attempt to read
      if fd_event == IOEvent.READ or fd_event == IOEvent.CLOSE:
        assert total_bytes_read < n # NOTE: I don't think we should ever encounter this state
        assert n - total_bytes_read <= buf_size - total_bytes_read # NOTE: We shouldn't ever be able to read into the buffer past it's end
        err, bytes_read = fd_read(self.fd, n - total_bytes_read, buffer, buffer_offset + total_bytes_read)
      elif fd_event == IOEvent.ERROR: raise AIOError(IOErrorReason.ERROR, f"error reading from file descriptor {self.fd}")
      else: raise NotImplementedError(fd_event)

      total_bytes_read += bytes_read
      
      ### NOTE: Event Handling
      # Conditions when the IOEvent should be consumed from the Event Log:
      #   - when no bytes are returned during a read
      #   - the fd is no longer ready to be read from
      ###
      if (
        (err == IOErrorReason.NONE and bytes_read <= 0)
        or (err == IOErrorReason.BLOCK or err == IOErrorReason.EOF)
      ): await self.event_log.pop()

      ### NOTE: Flow Control - Break if no more can be read, or we have met instructed requirements
      # 
      # When should we error out?
      # 
      # - If IO Error is ERROR or UNHANDLED: Raise an AIOError
      # 
      # When should we break?
      # 
      # - total_bytes_read >= n: Break
      # - if IOError one of EOF|BLOCK|CLOSED: Break
      # > NOTE: The Buffer can never be smaller than the total amount of bytes being read; so no need to check if the buffer is full
      # 
      # When should we continue?
      # 
      # - if IOError one of NONE|INTERRUPT: Continue
      #  
      # NOTES:
      # 
      # - For Streams, a BlOCK event is the equivalent of a EOF for a File.
      # - The Most Common Error Reason is NONE followed by EOF/BLOCK
      #
      ###

      if total_bytes_read >= n or (err == IOErrorReason.EOF or err == IOErrorReason.CLOSED): break
      elif err == IOErrorReason.BLOCK or err == IOErrorReason.NONE or err == IOErrorReason.INTERRUPT: continue
      elif err == IOErrorReason.ERROR or err == IOErrorReason.UNHANDLED: raise AIOError(err, f"error writing to file descriptor {self.fd}")
      else: raise NotImplementedError(f"Unhandled State encountered: IOErrorReason={err.name},IOEvent={fd_event.name},TotalBytesRead={total_bytes_read},LastBytesRead={bytes_read}")

    return total_bytes_read

  async def _read_until(self, n: IOCondition) -> bytes:
    """Read until a IOCondition returning the bytes read"""
    if self.kind == 'stream' and n == IOCondition.EOF: raise ValueError('A Streaming FD never encounters a EOF')

    # Initialize state
    buffer = bytearray(4 * CHUNK_SIZE) # This Buffer is not fixed size; we will need to continuosly grow it
    err: IOErrorReason = IOErrorReason.NONE
    fd_event: IOEvent = IOEvent.ERROR
    total_bytes_read = 0

    # Start reading
    while True:
      if len(buffer) - total_bytes_read < CHUNK_SIZE: buffer.extend(bytearray(4 * CHUNK_SIZE)) # Grow the Buffer
      fd_event: IOEvent | None = await self.event_log.peek()
      # Attempt to read
      if fd_event == IOEvent.READ or fd_event == IOEvent.CLOSE: err, bytes_read = fd_read(self.fd, CHUNK_SIZE, buffer, total_bytes_read)
      elif fd_event == IOEvent.ERROR: raise AIOError(IOErrorReason.ERROR, f"error reading from file descriptor {self.fd}")
      else: raise NotImplementedError(fd_event)

      total_bytes_read += bytes_read
      
      ### NOTE: Event Handling
      # Conditions when the IOEvent should be consumed from the Event Log:
      #   - when no bytes are returned during a read
      #   - the fd is no longer ready to be read from
      ###
      if (
        (err == IOErrorReason.NONE and bytes_read <= 0)
        or (err == IOErrorReason.BLOCK or err == IOErrorReason.EOF)
      ): await self.event_log.pop()

      ### NOTE: Flow Control - Break if no more can be read, or we have met instructed requirements
      # 
      # When should we error out?
      # 
      # - If IO Error is ERROR or UNHANDLED: Raise an AIOError
      # 
      # When should we break?
      # 
      # - if IO Error matches n: Break
      # - if IO Error is EOF/BLOCK or CLOSED: Break
      # 
      # When should we continue?
      # 
      # - if IOError one of NONE|INTERRUPT: Continue
      #  
      # NOTES:
      # 
      # - For Streams, a BlOCK event is the equivalent of a EOF for a File.
      # - The Most Common Error Reason is NONE followed by EOF/BLOCK
      #
      ###

      if err.value == n.value or (err == IOErrorReason.EOF or err == IOErrorReason.CLOSED): break # We encountered the IOCondition
      elif err == IOErrorReason.BLOCK or err == IOErrorReason.NONE or err == IOErrorReason.INTERRUPT: continue
      elif err == IOErrorReason.ERROR or err == IOErrorReason.UNHANDLED: raise AIOError(err, f"error writing to file descriptor {self.fd}")
      else: raise NotImplementedError(f"Unhandled State encountered: IOErrorReason={err.name},IOEvent={fd_event.name},TotalBytesRead={total_bytes_read},LastBytesRead={bytes_read}")

    return bytes(buffer[:total_bytes_read])
    
  async def _read_until_into(self, n: IOCondition, buffer: bytearray | memoryview, buffer_offset: int) -> int:
    """Read into a Buffer until a IOCondition or the buffer is full"""
    if self.kind == 'stream' and n == IOCondition.EOF: raise ValueError('A Streaming FD never encounters a EOF')

    # Initialize state
    err: IOErrorReason = IOErrorReason.NONE
    fd_event: IOEvent = IOEvent.ERROR
    total_bytes_read = 0
    buf_size = len(buffer) - buffer_offset
    buf_size_left = buf_size

    # Start reading
    while True:
      fd_event: IOEvent | None = await self.event_log.peek()
      # Attempt to read
      if fd_event == IOEvent.READ or fd_event == IOEvent.CLOSE:
        assert total_bytes_read < buf_size # NOTE: I don't think we should ever encounter this state
        # Make sure we don't read past the buffer's capacity
        buf_size_left = buf_size - total_bytes_read
        if CHUNK_SIZE < buf_size_left: _n = CHUNK_SIZE
        else: _n = buf_size_left
        assert _n > 0 # NOTE: We shouldn't ever reach these states
        err, bytes_read = fd_read(self.fd, _n, buffer, buffer_offset + total_bytes_read)
      elif fd_event == IOEvent.ERROR: raise AIOError(IOErrorReason.ERROR, f"error reading from file descriptor {self.fd}")
      else: raise NotImplementedError(fd_event)

      total_bytes_read += bytes_read
      
      ### NOTE: Event Handling
      # Conditions when the IOEvent should be consumed from the Event Log:
      #   - when no bytes are returned during a read
      #   - the fd is no longer ready to be read from
      ###
      if (
        (err == IOErrorReason.NONE and bytes_read <= 0)
        or (err == IOErrorReason.BLOCK or err == IOErrorReason.EOF)
      ): await self.event_log.pop()

      ### NOTE: Flow Control - Break if no more can be read, or we have met instructed requirements
      # 
      # When should we error out?
      # 
      # - If IO Error is ERROR or UNHANDLED: Raise an AIOError
      # 
      # When should we break?
      # 
      # - if IOError == n: Break
      # - if buffer is full: Break
      # - if IOError one of EOF|BLOCK|CLOSED: Break
      # > NOTE: The Buffer can never be smaller than the total amount of bytes being read; so no need to check if the buffer is full
      # 
      # When should we continue?
      # 
      # - if IOError one of NONE|INTERRUPT: Continue
      #  
      # NOTES:
      # 
      # - For Streams, a BlOCK event is the equivalent of a EOF for a File.
      # - The Most Common Error Reason is NONE followed by EOF/BLOCK
      #
      ###

      if err.value == n.value or total_bytes_read >= buf_size or (err == IOErrorReason.EOF or err == IOErrorReason.CLOSED): break
      elif err == IOErrorReason.BLOCK or err == IOErrorReason.NONE or err == IOErrorReason.INTERRUPT: continue
      elif err == IOErrorReason.ERROR or err == IOErrorReason.UNHANDLED: raise AIOError(err, f"error writing to file descriptor {self.fd}")
      else: raise NotImplementedError(f"Unhandled State encountered: IOErrorReason={err.name},IOEvent={fd_event.name},TotalBytesRead={total_bytes_read},LastBytesRead={bytes_read}")

    return total_bytes_read

  async def read(
    self,
    n: int | IOCondition,
    buffer: bytearray | memoryview | None = None,
    buffer_offset: int = 0,
  ) -> bytes | int:
    """Read N Bytes from the File Descriptor or until EOF is reached. Return the buffer as bytes or the total bytes read if a buffer was provided."""
    # Dispatch the call to the underlying read implementation
    if isinstance(n, IOCondition):
      if buffer is None: return await self._read_until(n)
      else: return await self._read_until_into(n, buffer, buffer_offset)
    else:
      if buffer is None: return await self._read(n)
      else: return await self._read_into(n, buffer, buffer_offset)

  async def write(
    self,
    buffer: ByteString,
    n: int,
    buffer_offset: int = 0,
  ) -> int:
    """Write N bytes from buffer starting at the buffer offset into the File Descriptor."""

    total_bytes_written = 0
    err: IOErrorReason = IOErrorReason.NONE
    fd_event: IOEvent = IOEvent.ERROR
    while True:
      fd_event: IOEvent = await self.event_log.peek()
      assert fd_event is not None
      if fd_event == IOEvent.WRITE:
        err, bytes_written = fd_write(self.fd, buffer, n - total_bytes_written, buffer_offset + total_bytes_written)
        if err == IOErrorReason.NONE: assert bytes_written > 0
      elif fd_event == IOEvent.CLOSE: break
      elif fd_event == IOEvent.ERROR: raise AIOError(IOErrorReason.ERROR, f"error writing to file descriptor {self.fd}")
      else: raise NotImplementedError(fd_event)

      total_bytes_written += bytes_written

      # Evaluate if the Event should be consumed
      if fd_event == IOEvent.WRITE and (
        err == IOErrorReason.BLOCK or err == IOErrorReason.CLOSED
      ): await self.event_log.pop()

      # Flow Control - Break if we have written the entire buffer or we encountered some sort of error state
      # NOTE: If a fd blocks, we just have to wait until the buffer is drained before trying to write more
      if total_bytes_written >= n or err == IOErrorReason.CLOSED: break
      elif err == IOErrorReason.BLOCK or err == IOErrorReason.NONE or err == IOErrorReason.INTERRUPT: continue
      elif err == IOErrorReason.ERROR or err == IOErrorReason.UNHANDLED: raise AIOError(err, f"error writing to file descriptor {self.fd}")
      else: raise NotImplementedError(f"Unhandled State encountered: IOErrorReason={err.name},IOEvent={fd_event.name},TotalBytesWritten={total_bytes_written},LastBytesWritten={bytes_written}")
    
    return total_bytes_written
