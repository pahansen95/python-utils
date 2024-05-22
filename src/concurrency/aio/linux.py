from __future__ import annotations
from . import (
  aio_backend, AIOBackend,
  IOErrorReason,
  AIOError,
)
import cffi, pathlib, os, hashlib, shutil, sys
from typing import Literal

### Immediately Raise an Error if not on Linux ###

if aio_backend != AIOBackend.LINUX: raise RuntimeError("Linux IO is only supported on Linux")

### The C Code ###

_linux_io_errors: dict[int, IOErrorReason] = {
  0: IOErrorReason.NONE,
  1: IOErrorReason.BLOCK,
  2: IOErrorReason.EOF,
  3: IOErrorReason.CLOSED,
  4: IOErrorReason.ERROR,
  5: IOErrorReason.INTERRUPT,
}
"""A Mapping of Linux IO Errors to IO Errors."""

_c_src = """\
#include <unistd.h>
#include <errno.h>

static enum IOErrorReason {
  IOE_NONE = 0,
  IOE_BLOCK = 1,
  IOE_EOF = 2,
  IOE_CLOSED = 3,
  IOE_IOERR = 4,
  IOE_INTERRUPT = 5,
  IOE_UNHANDLED = 127,
};

void c_read_from_fd_into_buffer(
  int fd,
  char *buffer,
  size_t count,
  size_t buf_offset,
  int result[2]
) {
  /*
  Read from a File Descriptor into a Buffer returning any errors that occur & the bytes read.
  Args:
    fd: The File Descriptor to read from.
    buffer: The Buffer to read into; a pointer to a byte array.
    count: The number of bytes to read.
    buf_offset: The offset index at which to start writing into the Buffer.
    result: A "tuple" of (IOErrorReason, bytes_read).
  */

  char *buffer_offset = buffer + buf_offset;
  errno = 0; // Reset errno
  ssize_t n = read(fd, buffer_offset, count);

  if (n >= 0) {
    // Successful read
    if (n == 0) { result[0] = (int)IOE_EOF; }
    else { result[0] = (int)IOE_NONE; }
    result[1] = (int)n;
  } else {
    // An error occurred
    if (errno == EAGAIN || errno == EWOULDBLOCK) { result[0] = (int)IOE_BLOCK; }
    else if (errno == EBADF) { result[0] = (int)IOE_CLOSED; }
    else if (errno == EINTR) { result[0] = (int)IOE_INTERRUPT; }
    else if (errno == EIO) { result[0] = (int)IOE_IOERR; }
    else { result[0] = (int)IOE_UNHANDLED + errno; }
    result[1] = 0;
  }
}

void c_write_from_buffer_into_fd(
  int fd,
  char *buffer,
  size_t count,
  size_t buf_offset,
  int result[2]
) {
  /*
  Write to a File Descriptor from a Buffer returning any errors that occur & the bytes written.
  Args:
    fd: The File Descriptor to write to.
    buffer: The Buffer to write from; a pointer to a byte array.
    count: The number of bytes to write.
    buf_offset: The offset index at which to start reading from the Buffer.
    result: A "tuple" of (IOErrorReason, bytes_written).
  */

  char *buffer_offset = buffer + buf_offset;
  errno = 0; // Reset errno
  ssize_t n = write(fd, buffer_offset, count);

  if (n >= 0) {
    // Successful write
    result[0] = (int)IOE_NONE;
    result[1] = (int)n;
  } else {
    // An error occurred
    if (errno == EAGAIN || errno == EWOULDBLOCK) { result[0] = (int)IOE_BLOCK; }
    else if (errno == EBADF) { result[0] = (int)IOE_CLOSED; }
    else if (errno == EINTR) { result[0] = (int)IOE_INTERRUPT; }
    else if (errno == EIO) { result[0] = (int)IOE_IOERR; }
    else { result[0] = (int)IOE_UNHANDLED + errno; }
    result[1] = 0;
  }
}
"""
_ffi_def = """\
void c_read_from_fd_into_buffer(
  int fd,
  char *buffer,
  size_t count,
  size_t buf_offset,
  int result[2]
);
void c_write_from_buffer_into_fd(
  int fd,
  char *buffer,
  size_t count,
  size_t buf_offset,
  int result[2]
);
"""

### Load the FFI, conditionally compiling if no cache exists ###

_cache_parent_dir: pathlib.Path
if 'CACHE_DIR' in os.environ: _cache_parent_dir = pathlib.Path(os.environ['CACHE_DIR']) / "cffibuild"
else: _cache_parent_dir = pathlib.Path(__file__).parent / ".cffibuild"
_cache_dir: pathlib.Path = _cache_parent_dir / "aio_linux"
if not _cache_dir.exists(): _cache_dir.mkdir(mode=0o755, parents=True)

# Assemble the Lib Cache
_lib_cache: dict[str, dict[Literal['dir', 'src', 'id'], pathlib.Path | str | bool]] = {
  "_utils_aio_linux": {
    "dir": _cache_dir / "_utils_aio_linux",
    "src": _c_src,
    "header": _ffi_def,
  },
}
# Check for invalid Libs
ffi = cffi.FFI()
for _lib, _state in _lib_cache.items():
  sys.path.insert(0, str(_state['dir']))
  _rebuild: bool = True
  _id_file: pathlib.Path = _state['dir'] / ".fingerprint"
  _computed_id: str = hashlib.sha256(_state['src'].encode('utf-8')).hexdigest()
  _cached_id: str | None = _id_file.read_text().lower().strip() if _id_file.exists() else None
  if _computed_id == _cached_id: _rebuild = False

  if _rebuild:
    if _state['dir'].exists(): shutil.rmtree(_state['dir'])
    _state['dir'].mkdir(mode=0o755, parents=True)
    ffi.set_source(_lib, _state['src'])
    ffi.cdef(_state['header'])
    ffi.compile(tmpdir=str(_state["dir"]), verbose=True)
    _id_file.write_text(_computed_id)

# ffi.set_source("_utils_aio_linux", _read_from_fd_into_buffer_c_src)
# ffi.cdef(_read_from_fd_into_buffer_ffi_def)
# ffi.compile(tmpdir=str(_cache_dir), verbose=True)
ffi_alloc = ffi.new_allocator(should_clear_after_alloc=False)
"""An FFI Allocator that doesn't skips memory initialization."""

### Final Imports ###

import _utils_aio_linux # This is the name of my module generated by cffi

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
  buffer = ffi_alloc("char[]", n) if into is None else ffi.from_buffer(into, require_writable=True)
  result = ffi_alloc("int[]", 2)
  _utils_aio_linux.lib.c_read_from_fd_into_buffer(fd, buffer, n, 0 if into is None else into_offset, result)
  ###

  (_errno, _bytes_read) = (result[0], result[1])
  assert isinstance(_errno, int) and isinstance(_bytes_read, int)
  if _errno >= 127: raise AIOError(IOErrorReason.UNHANDLED, f"Unhandled OSError({_errno - 127}) on fd {fd}")
  elif _bytes_read == 0: return (error_map[_errno], b'') if into is None else (error_map[_errno], 0)
  else: return (
    error_map[_errno],
    ffi.buffer(buffer, _bytes_read)[:] if into is None else _bytes_read, # This is a copy of the buffer as a bytes object
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
  _buffer = ffi.from_buffer(buffer, require_writable=False)
  result = ffi_alloc("int[]", 2)
  _utils_aio_linux.lib.c_write_from_buffer_into_fd(fd, _buffer, _count, buffer_offset, result)
  ###
  (_errno, _bytes_written) = (result[0], result[1])
  assert isinstance(_errno, int) and isinstance(_bytes_written, int)
  if _errno >= 127: raise AIOError(IOErrorReason.UNHANDLED, f"Unhandled OSError({_errno - 127}) on fd {fd}")
  else: return (error_map[_errno], _bytes_written)
