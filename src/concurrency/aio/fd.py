from __future__ import annotations
from dataclasses import dataclass, field, KW_ONLY
import asyncio, os, sys, fcntl
from loguru import logger
from collections.abc import Coroutine, Callable
from typing import TypeVar, Any
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
  event_log: ItemLog[IOEvent] = field(default_factory=ItemLog)
  """The Events that occurred on the File Descriptor."""

  @property
  def closed(self) -> bool:
    """Checks whether the File Descriptor is closed (using fcntl)."""
    try:
      fcntl.fcntl(self.fd, fcntl.F_GETFD)
      return False
    except OSError:
      return True

  # @_log_trapper
  async def read(
    self,
    n: int | IOCondition,
    buffer: bytearray | memoryview | None = None,
    buffer_offset: int = 0,
  ) -> bytes | int:
    """Read N Bytes from the File Descriptor or until EOF is reached. Return the buffer as bytes or the total bytes read if a buffer was provided."""
    _n_is_flag: bool = isinstance(n, IOCondition)
    if not _n_is_flag and n <= 0: raise ValueError("n must be greater than 0")
    if buffer_offset < 0 or (buffer is not None and buffer_offset >= len(buffer)): raise ValueError("buffer offset out of bounds")
    if (not _n_is_flag and buffer is not None) and n + buffer_offset > len(buffer): raise ValueError(f"Reading up to {n} bytes starting at buffer idx {buffer_offset} will exceed the buffer size of {len(buffer)}")
    logger.trace(f"fd{self.fd} - Reading " + (f"up to {n} bytes" if not _n_is_flag else f"until {n.name}"))
    # fd sanity checks are handled by fd_read
    _bytes_read: int = 0
    if buffer is None: _buffer = bytearray(4 * CHUNK_SIZE if _n_is_flag else n); _buffer_offset = 0
    else: _buffer = buffer; _buffer_offset = buffer_offset

    err: IOErrorReason = IOErrorReason.NONE
    fd_event: IOEvent = IOEvent.ERROR # We are just initializing this variable
    if _n_is_flag: _n = CHUNK_SIZE if buffer is None else (len(buffer) - buffer_offset) # Read the default chunk size or the size of the buffer starting from the offset.
    else: _n = n # Read N bytes as instructed
    
    while True:
      # If the Internal buffer is nearly fully then extend it so that we can always fit at least CHUNK_SIZE
      if buffer is None and len(_buffer) - _bytes_read < CHUNK_SIZE: logger.trace(f"fd{self.fd} - Extending the Internal buffer"); _buffer.extend(bytearray(4 * CHUNK_SIZE)) # Extend by 64K
      logger.trace(f"fd{self.fd} - Bytes Read {_bytes_read}/{len(_buffer)}")
      logger.trace(f"The current IOLog {id(self.event_log)} is {list(self.event_log)}")
      fd_event: IOEvent | None = await self.event_log.peek()
      logger.trace(f"fd{self.fd} - Event: {fd_event.name}")

      if fd_event == IOEvent.READ:
        if _n_is_flag and buffer is None: _n = CHUNK_SIZE # If using an internal Buffer, read the default amount.
        elif _n_is_flag and buffer is not None: _n = len(_buffer) - _buffer_offset - _bytes_read # If using an external Buffer, read up to the remaining space in the buffer
        else: _n = n - _bytes_read # Otherwise Read the remaining bytes instructed irrespective of buffer sizes
        err, _read = fd_read(self.fd, _n, _buffer, _buffer_offset + _bytes_read)
        logger.trace(f"fd{self.fd} - Bytes Read: {_read}, IOErrorReason: {err.name}")
      elif fd_event == IOEvent.CLOSE: break
      elif fd_event == IOEvent.ERROR: raise AIOError(IOErrorReason.ERROR, f"error reading from file descriptor {self.fd}")
      else: raise NotImplementedError(fd_event)
    
      assert fd_event == IOEvent.READ
      assert isinstance(_read, int)
      _bytes_read += _read

      # Evaluate if the Read event should be consumed: when no bytes are returned during a read or the fd is in a non-readable state
      if (
        (err == IOErrorReason.NONE and _read <= 0)
        or err == IOErrorReason.BLOCK
        or err == IOErrorReason.EOF
        or err == IOErrorReason.EOF
      ): await self.event_log.pop()

      # Flow Control - Break if no more can be read, or we have met instructed requirements
      # """NOTE: Flow Control
      # 
      # When should we error out?
      # 
      # - If IO Error is ERROR or UNHANDLED: Raise an AIOError
      # 
      # When should we break?
      # 
      # - if n is flag:
      #   - if IO Error matches n: Break
      #   - if using a Provided Buffer AND buffer is full: Break
      # - if n is int:
      #   - bytes read is n: Break
      #   - if using a Provided Buffer AND buffer is full: Break
      # - if IO Error is EOF/BLOCK or CLOSED: Break
      # 
      # When should we continue?
      # 
      # - If IO Error is NONE or INTERRUPT: Continue
      #  
      # NOTES:
      # 
      # - When Reading, a Block event is the EOF equivalent for a stream
      # - The Most Common Error Reason is NONE followed by EOF/BLOCK
      # 
      # """
      if _n_is_flag and err.value == n.value: break # We encountered the flag condition
      elif _n_is_flag and buffer is not None and _bytes_read + buffer_offset >= len(buffer): break # We met the read quota
      elif (not _n_is_flag) and _bytes_read >= n: assert _bytes_read == n; break # We met the read quota
      elif (not _n_is_flag) and buffer is not None and _bytes_read + buffer_offset >= len(buffer): assert _bytes_read + buffer_offset == len(buffer); break # We met the read quota
      elif (err == IOErrorReason.BLOCK or err == IOErrorReason.EOF) or err == IOErrorReason.CLOSED: break # No More can be read
      elif err == IOErrorReason.NONE or err == IOErrorReason.INTERRUPT: continue
      elif err == IOErrorReason.ERROR or err == IOErrorReason.UNHANDLED: raise AIOError(err, f"error reading from file descriptor {self.fd}")
      else: raise NotImplementedError(f"Unhandled State encountered: IOErrorReason={err.name},IOEvent={fd_event.name},TotalBytesRead={_bytes_read},LastBytesRead={_read}")

      # if err == IOErrorReason.NONE or err == IOErrorReason.INTERRUPT: continue
      # if err == IOErrorReason.BLOCK or err == IOErrorReason.EOF or err == IOErrorReason.CLOSED: break # No More can be read
      # elif _n_is_flag and err.value == n.value: break # We encountered the flag condition
      # elif (not _n_is_flag and _bytes_read >= n) or (buffer is not None and _bytes_read + buffer_offset >= len(buffer)): # We met the read quota
      #   assert _bytes_read == n or _bytes_read + buffer_offset == len(buffer)
      #   break
      # elif err == IOErrorReason.ERROR or err == IOErrorReason.UNHANDLED: # An Error occured
      #   raise AIOError(err, f"error reading from file descriptor {self.fd}")
      # else: raise NotImplementedError(f"Unhandled Read State encountered: IOErrorReason={err.name},IOEvent={fd_event.name},TotalBytesRead={_bytes_read},LastBytesWritten={_read}")
    
    logger.trace(f"fd{self.fd} - Read a total of {_bytes_read} bytes")
    return bytes(_buffer[:_bytes_read]) if buffer is None else _bytes_read

  # @_log_trapper
  async def write(
    self,
    buffer: bytes | bytearray | memoryview,
    n: int,
    buffer_offset: int = 0,
  ) -> int:
    """Write N bytes from buffer starting at the buffer offset into the File Descriptor."""
    if buffer_offset < 0 or buffer_offset >= len(buffer): raise ValueError("buffer offset out of bounds")
    if n < 1: raise ValueError("must write at least 1 byte")
    
    logger.trace(f"fd{self.fd} - Writing {n} bytes")

    _bytes_written: int = 0
    err: IOErrorReason = IOErrorReason.NONE
    fd_event: IOEvent = IOEvent.ERROR
    # while _bytes_written < n:
    while True:
      logger.trace(f"fd{self.fd} - Bytes Written {_bytes_written}/{n}")
      fd_event: IOEvent | None = await self.event_log.peek()
      logger.trace(f"fd{self.fd} - Event: {fd_event.name}")
      if fd_event == IOEvent.WRITE:
        err, _written = fd_write(self.fd, buffer, n - _bytes_written, buffer_offset + _bytes_written)
        logger.trace(f"fd{self.fd} - Error: {err.name}, Bytes Written: {_written}")
      elif fd_event == IOEvent.CLOSE: break
      elif fd_event == IOEvent.ERROR: raise AIOError(IOErrorReason.ERROR, f"error writing to file descriptor {self.fd}")
      else: raise NotImplementedError(fd_event)

      assert fd_event == IOEvent.WRITE
      assert isinstance(_written, int)
      _bytes_written += _written

      # Evaluate if the WRITE Event should be consumed
      if (
        (err == IOErrorReason.NONE and _written == 0)
        or err == IOErrorReason.BLOCK
        or err == IOErrorReason.EOF
        or err == IOErrorReason.CLOSED
      ):
        logger.trace(f"fd{self.fd} - Consuming WRITE Event")
        await self.event_log.pop()

      # Flow Control - Break if we have written the entire buffer or we encountered some sort of error state
      # NOTE: If a fd blocks, we just have to wait until the buffer is drained before trying to write more; this is not like async reads where a BLOCK is the streaming equivalent of a EOF
      if _bytes_written >= n: assert _bytes_written == n; break
      elif err == IOErrorReason.EOF or err == IOErrorReason.CLOSED: break
      elif err == IOErrorReason.BLOCK or err == IOErrorReason.NONE or err == IOErrorReason.INTERRUPT: continue
      elif err == IOErrorReason.ERROR or err == IOErrorReason.UNHANDLED: raise AIOError(err, f"error writing to file descriptor {self.fd}")
      else: raise NotImplementedError(f"Unhandled State encountered: IOErrorReason={err.name},IOEvent={fd_event.name},TotalBytesWritten={_bytes_written},LastBytesWritten={_written}")
    
    logger.trace(f"fd{self.fd} - Wrote a total of {_bytes_written} bytes")
    return _bytes_written
