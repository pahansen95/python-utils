from __future__ import annotations
import pathlib
from utils.extern import active_backend, SystemBackend
from utils.extern.c import calculate_module_fingerprint, BuildSpec, c_registry

if active_backend != SystemBackend.LINUX: raise RuntimeError("Linux IO is only supported on Linux")

_CDEF = """\
void c_read_from_fd_into_buffer(int fd, char *buffer, size_t count, size_t buf_offset, int result[2]);
void c_write_from_buffer_into_fd(int fd, char *buffer, size_t count, size_t buf_offset, int result[2]);
"""
LIB_BUILD_SPEC: BuildSpec = {
  'cdef': _CDEF,
  'include': ['aiolinux.h'],
  'sources': ['src/aiolinux.c'],
}
_lib_path = pathlib.Path(__file__).parent
if 'aio_linux' not in c_registry.modules:
  _lib_fingerprint = calculate_module_fingerprint(_lib_path, LIB_BUILD_SPEC)
  c_registry.add("aio_linux", LIB_BUILD_SPEC)
  build_status =  c_registry.build("aio_linux", _lib_path, _lib_fingerprint)
  assert build_status['fingerprint'] == _lib_fingerprint
c_lib, aio_linux = c_registry.get('aio_linux')

from .. import IOErrorReason, AIOError

_linux_io_errors: dict[int, IOErrorReason] = {
  0: IOErrorReason.NONE,
  1: IOErrorReason.BLOCK,
  2: IOErrorReason.EOF,
  3: IOErrorReason.CLOSED,
  4: IOErrorReason.ERROR,
  5: IOErrorReason.INTERRUPT,
}
"""A Mapping of Linux IO Errors to IO Errors."""

def fd_read(
  fd: int,
  n: int,
  into: bytearray | memoryview | None = None,
  into_offset: int = 0,
  error_map: dict[int, IOErrorReason] = _linux_io_errors,
) -> tuple[IOErrorReason, bytes | int]:
  """Read N bytes from a File Descriptor returning any errors that occur & the buffer as bytes or the total bytes read if a buffer was provided.
  Args:
    fd: The File Descriptor to read from.
    n: The number of bytes to read.
    into: A mutable Buffer to read into; a bytearray or memoryview. Must be large enough to accomodate the bytes read.
    into_offset: The offset index at which to start writing into the Buffer.
    error_map: A Mapping of Linux IO Errors to IO Errors.
  Returns:
    A tuple of:
      0: The IOErrorReason that occurred if any.
      1: A copy of the bytes read or the total bytes read if a buffer was provided.
  """

  if n < 1: raise ValueError("Must read at least 1 byte")
  if into is not None:
    if into_offset < 0 or into_offset >= len(into): raise ValueError("into offset out of bounds")
    if into_offset + n > len(into): raise ValueError("into offset + n out of bounds")

  ### NOTE
  # cffi owns the memory for the buffer & result & will free them once out of scope.
  # We can't declare the buffer & result inline in the function call as they would immediately go out of scope & be freed.
  # We can't directly return them from this function as they would be freed after the function returns.
  # So we copy the data out before returning.
  # If we don't want to copy data then a buffer needs to be provided that we can write into.
  ###
  buffer = c_lib.new("char[]", n) if into is None else c_lib.from_buffer(into, require_writable=True)
  result = c_lib.new("int[]", 2)
  aio_linux.c_read_from_fd_into_buffer(fd, buffer, n, 0 if into is None else into_offset, result)
  ###

  (_errno, _bytes_read) = (result[0], result[1])
  assert isinstance(_errno, int) and isinstance(_bytes_read, int)
  if _errno >= 127: raise AIOError(IOErrorReason.UNHANDLED, f"Unhandled OSError({_errno - 127}) on fd {fd}")
  elif _bytes_read == 0: return (error_map[_errno], b'') if into is None else (error_map[_errno], 0)
  else: return (
    error_map[_errno],
    c_lib.buffer(buffer, _bytes_read)[:] if into is None else _bytes_read, # This is a copy of the buffer as a bytes object
  )

def fd_write(
  fd: int,
  buffer: bytes | bytearray | memoryview,
  n: int,
  buffer_offset: int = 0,
  error_map: dict[int, IOErrorReason] = _linux_io_errors,
) -> tuple[IOErrorReason, int]:
  """Write N bytes from a Buffer into a File Descriptor returning any errors that occur & the total bytes written.
  Args:
    fd: The File Descriptor to write to.
    buffer: The Buffer to write from; a bytes, bytearray, or memoryview.
    n: The number of bytes to write.
    buffer_offset: The offset index at which to start reading from the Buffer.
    error_map: A Mapping of Linux IO Errors to IO Errors.
  Returns:
    A tuple of:
      0: The IOErrorReason that occurred if any.
      1: The total bytes written.
  """

  ### NOTE
  # We need to make sure that we don't try to read beyond the end of the buffer; otherwise we get garbage data or a segfault
  ###
  if len(buffer) < 1: raise ValueError("Buffer must contain at least 1 byte")
  if n < 1: raise ValueError("Must write at least 1 byte")
  if buffer_offset < 0 or buffer_offset >= len(buffer): raise ValueError("buffer offset out of bounds")
  _count = min(n, len(buffer) - buffer_offset)
  assert buffer_offset + _count <= len(buffer)
  ###

  ### NOTE
  # cffi owns the memory for the buffer & result & will free them once out of scope.
  # We can't declare the buffer & result inline in the function call as they would immediately go out of scope & be freed.
  # We can't directly return them from this function as they would be freed after the function returns.
  # So we copy the data out before returning.
  # If we don't want to copy data then a buffer needs to be provided that we can write into.
  ###
  _buffer = c_lib.from_buffer(buffer, require_writable=False)
  result = c_lib.new("int[]", 2)
  aio_linux.c_write_from_buffer_into_fd(fd, _buffer, _count, buffer_offset, result)
  ###
  (_errno, _bytes_written) = (result[0], result[1])
  assert isinstance(_errno, int) and isinstance(_bytes_written, int)
  if _errno >= 127: raise AIOError(IOErrorReason.UNHANDLED, f"Unhandled OSError({_errno - 127}) on fd {fd}")
  else: return (error_map[_errno], _bytes_written)
