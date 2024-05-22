from __future__ import annotations
from dataclasses import dataclass, field, KW_ONLY
import pathlib
import asyncio
import os
import enum
import resource
from typing import AsyncGenerator
from loguru import logger
from utils.concurrency.aio import IOEvent, IOCondition, IOErrorReason
from utils.concurrency.aio.watch import IOWatcher, io_watcher, WatchBackend
from utils.concurrency.aio.fd import AsyncFileDescriptor

import enum
import os

UNBOUND = type('UNBOUND', (), {})
@dataclass(frozen=True, order=True)
class FDLimits:
  """File Descriptor Limits for the Process."""
  soft: int | UNBOUND
  hard: int | UNBOUND
  proc: int = field(init=False, default_factory=os.getpid)
  def __str__(self) -> str:
    return "Proc {pid} has FileDescriptor limit of {soft} (Soft) and {hard} (Hard)".format(
      pid=self.proc,
      soft=self.soft if self.soft is not UNBOUND else "UNBOUND",
      hard=self.hard if self.hard is not UNBOUND else "UNBOUND",
    )
  
  @classmethod
  def get(cls) -> FDLimits:
    """Get the Current File Descriptor Limits for the Process."""
    limits = resource.getrlimit(resource.RLIMIT_NOFILE)
    return cls(
      hard=UNBOUND if limits[1] == resource.RLIM_INFINITY else limits[1],
      soft=UNBOUND if limits[0] == resource.RLIM_INFINITY else limits[0],
    )
  
  def set(self) -> FDLimits:
    """Set the File Descriptor Limits for the Process & Return the New Limits."""
    resource.setrlimit(
      resource.RLIMIT_NOFILE,
      (
        resource.RLIM_INFINITY if self.soft is UNBOUND else self.soft,
        resource.RLIM_INFINITY if self.hard is UNBOUND else self.hard,
      )
    )
    return self.get()

class SeekWhence(enum.IntEnum):
  """Defines the Whence Argument for the `seek` method."""
  SET = os.SEEK_SET
  CUR = os.SEEK_CUR
  END = os.SEEK_END

async def seek(
  fd: AsyncFileDescriptor,
  offset: int,
  whence: SeekWhence = SeekWhence.SET,
) -> int:
  """Change the File Descriptor's Current Position."""
  return os.lseek(fd.fd, offset, whence.value)

async def tell(fd: AsyncFileDescriptor) -> int:
  """Get the File Descriptor's Current Position."""
  return os.lseek(fd.fd, 0, os.SEEK_CUR)

async def truncate(fd: AsyncFileDescriptor, length: int) -> None:
  """Truncate the File Descriptor to the specified length."""
  return os.ftruncate(fd.fd, length)

class FileMode(enum.IntFlag):
  """Defines file opening modes with behaviors as specified by POSIX standards."""
  RDONLY = os.O_RDONLY
  """Open the file in read-only mode. No writing allowed."""
  WRONLY = os.O_WRONLY
  """Open the file in write-only mode. No reading allowed."""
  RDRW = os.O_RDWR
  """Open the file for both reading and writing."""
  CREAT = os.O_CREAT
  """Create a new file if it does not exist."""
  EXCL = os.O_EXCL
  """Must be used with `CREAT`. Ensures the file is created only if it does not already exist, failing otherwise."""
  APPEND = os.O_APPEND
  """Writes done to the file are appended at the end. Effective for logging."""
  TRUNC = os.O_TRUNC
  """If the file already exists and is opened for write or read/write, truncate its size to 0."""

ro_afd_t = tuple[AsyncFileDescriptor, None]
"""Read-Only AsyncFileDescriptor Tuple: (AsyncFileDescriptor, None)"""
wo_afd_t = tuple[None, AsyncFileDescriptor]
"""Write-Only AsyncFileDescriptor Tuple: (None, AsyncFileDescriptor)"""
rw_afd_t = tuple[AsyncFileDescriptor, AsyncFileDescriptor]
"""Read-Write AsyncFileDescriptor Tuple: (AsyncFileDescriptor, AsyncFileDescriptor)"""

async def open_file(
  file: pathlib.Path,
  mode: FileMode,
  watch: IOEvent | None = None,
  io_watcher: IOWatcher = io_watcher,
  return_errors: bool = False,
) -> ro_afd_t | wo_afd_t | rw_afd_t | AsyncFileDescriptor | Exception:
  """Opens a file and returns the Async File Descriptor(s) or an Exception if `return_errors` is True"""
  if not io_watcher.running: raise RuntimeError("The IOWatcher must be running to open a file")
  if not mode & (FileMode.RDONLY | FileMode.WRONLY | FileMode.RDRW | FileMode.CREAT): raise ValueError(f"Invalid FileMode: Expected at least one of {FileMode.RDONLY.name}, {FileMode.WRONLY.name}, {FileMode.RDRW.name} or {FileMode.CREAT.name} to be set")
  _create = mode & FileMode.CREAT
  _read = mode & (FileMode.RDONLY | FileMode.RDRW)
  _write = mode & (FileMode.WRONLY | FileMode.RDRW)
  _extra_mode = mode & ~(FileMode.RDONLY | FileMode.WRONLY | FileMode.RDRW | FileMode.CREAT) | os.O_NONBLOCK
  _watch = watch
  if watch is None:
    _watch = IOEvent.CLOSE | IOEvent.ERROR
    if _read: _watch |= IOEvent.READ
    if _write: _watch |= IOEvent.WRITE
  rfd: int | None = None
  wfd: int | None = None
  fd: int | None = None
  try:
    if _read | _write:
      if _read: rfd = os.open(str(file), FileMode.RDONLY | _create | _extra_mode)
      if _write: wfd = os.open(str(file), FileMode.WRONLY | _create | _extra_mode)
      return (
        AsyncFileDescriptor(
          rfd,
          io_watcher.register(rfd, _watch & ~IOEvent.WRITE, watch_backend=WatchBackend.POLL),
        ) if _read else None,
        AsyncFileDescriptor(
          wfd,
          io_watcher.register(wfd, _watch & ~IOEvent.READ, watch_backend=WatchBackend.POLL),
        ) if _write else None,
      )
    elif _create: # Only Create
      fd = os.open(str(file), FileMode.CREAT | _extra_mode)
      return AsyncFileDescriptor(
        fd,
        io_watcher.register(fd, _watch, watch_backend=WatchBackend.POLL),
      )
    # Catch Bugs
    else: assert False
      
  except Exception as e:
    for fd in [_fd for _fd in (rfd, wfd, fd) if _fd is not None]:
      try:
        os.close(fd)
      except OSError:
        logger.opt(exception=True).debug(f"Failed to close file descriptor {fd} after failed open")
      except:
        pass
    if return_errors: return e
    raise

async def close_file(
  *file: AsyncFileDescriptor,
  io_watcher: IOWatcher = io_watcher,
  return_errors: bool = False,
):
  """Close a File."""
  if not io_watcher.running: raise RuntimeError("The IOWatcher must be running to close a file")
  for afd in file:
    try:
      if afd.fd not in io_watcher: raise ValueError(f"File Descriptor {afd.fd} is not registered with IOWatcher {id(io_watcher)}")
      io_watcher.unregister(afd.fd)
      os.close(afd.fd)
    except Exception as e:
      if return_errors: return e
      raise

async def delete_file(
  file: pathlib.Path,
  return_errors: bool = False,
) -> None | Exception:
  """Delete a File"""
  logger.trace(f"Deleting File: {file}")
  try:
    # logger.critical(f"Delete's disabled for development: {file}")
    file.unlink()
  except Exception as e:
    if return_errors: return e
    raise

async def write_file(
  file: pathlib.Path,
  content: bytes,
  append: bool = False,
  mode: int | None = None,
  owner: tuple[int, int] | None = None,
  io_watcher: IOWatcher = io_watcher,
) -> None | Exception:
  """Write a buffer of bytes to a File (truncating); Returns any Exception that occurs during I/O"""
  logger.trace(f"Writing to File '{file}'")
  if not io_watcher.running: raise RuntimeError("The IOWatcher must be running to write to a file")
  # async with aiofiles.open(file, 'wb+' if append else 'wb') as _file:
  #   try:
  #     bytes_written = await _file.write(content)
  #     assert len(content) == bytes_written
  #   except Exception as e:
  #     return e
  # Create the file if it doesn't exist
  try:
    fd: int | None = None
    if not file.exists():
      fd = os.open(str(file), FileMode.CREAT | FileMode.EXCL, 0o600)
      os.close(fd)
      fd = None
    if mode is not None: os.chmod(file, mode)
    if owner is not None: os.chown(file, *owner)
    try:
      fd = os.open(str(file), FileMode.WRONLY | (FileMode.APPEND if append else FileMode.TRUNC) | os.O_NONBLOCK)
      afd = AsyncFileDescriptor(fd, io_watcher.register(fd, IOEvent.WRITE, watch_backend=WatchBackend.POLL))
      bytes_written = 0
      while bytes_written < len(content):
        bytes_written += await afd.write(content, len(content) - bytes_written, bytes_written)
    finally:
      if fd in io_watcher: io_watcher.unregister(fd)
      if fd is not None: os.close(fd)
  except Exception as e:
    return e

async def read_file(
  file: pathlib.Path,
  io_watcher: IOWatcher = io_watcher,
  return_errors: bool = False,
) -> bytes | Exception:
  """Read a File's Contents into Memory"""
  logger.trace(f"Reading File '{file}'")
  if not io_watcher.running: raise RuntimeError("The IOWatcher must be running to read a file")

  try:
    fd: int | None = None
    try:
      fd = os.open(str(file), FileMode.RDONLY | os.O_NONBLOCK)
      afd = AsyncFileDescriptor(fd, io_watcher.register(fd, IOEvent.READ, watch_backend=WatchBackend.POLL))
      file_contents: bytes = await afd.read(IOCondition.EOF)
      return file_contents
    finally:
      if fd in io_watcher: io_watcher.unregister(fd)
      if fd is not None: os.close(fd)
  except Exception as e:
    if return_errors: return e
    raise

async def create_directory(
  directory: pathlib.Path,
  mode: int,
  owner: tuple[int, int],
  exist_ok: bool = True,
  return_errors: bool = False,
) -> None | Exception:
  """Create a Directory updating the Mode & Owner"""
  logger.trace(f"Creating Directory '{directory}' with Mode '{mode:o}' & Owner '{owner}'")
  try:
    directory.mkdir(mode=mode, exist_ok=exist_ok)
    os.chown(directory, *owner)
  except Exception as e:
    if return_errors: return e
    raise

async def delete_directory(
  directory: pathlib.Path,
  missing_ok: bool = True,
  return_errors: bool = False,
) -> None | Exception:
  """Delete the Directory"""
  logger.trace(f"Deleting Directory: {directory}")
  if not directory.exists() and not missing_ok:
    raise FileNotFoundError(f"Directory does not exist: {directory}")
  elif not directory.exists() and missing_ok:
    return None
  elif not directory.is_dir():
    raise NotADirectoryError(f"Path is not a Directory: {directory}")
  
  try:
    # logger.critical(f"Delete's disabled for development: {directory}")
    directory.rmdir()
  except Exception as e:
    if return_errors: return e
    raise

async def walk_directory_tree(
  root: pathlib.Path,
  ignore_directory_startswith: list[str] | None = None,
  depth: int | None = None,
) -> AsyncGenerator[pathlib.Path, None]:
  """Walk the Directory Tree to the specified depth yielding Paths to Files & Directories.
  If `ignore_directory_startswith` is provided then any directories starting with the specified prefix will be ignored.
  If `depth` is provided then the walk will be limited to that depth. `root` has a depth of 0.
  """
  _search_stack: list[tuple[int, pathlib.Path]] = [(0, root)]
  _seen_dirs: set[pathlib.Path] = set()
  while len(_search_stack) > 0:
    _cur_depth: int; _dir: pathlib.Path
    _cur_depth, _dir = _search_stack.pop()
    logger.trace(f"Popping directory off the search stack: {_dir}")
    if (
      (depth is not None and _cur_depth > depth) \
      or \
      (
        ignore_directory_startswith is not None \
        and \
        any(_dir.name.startswith(_prefix) for _prefix in ignore_directory_startswith)
      )
    ):
      continue
    _true_dir: pathlib.Path = _dir.resolve()
    if _true_dir in _seen_dirs:
      logger.trace(f"Skipping already seen directory: {_dir}")
      continue
    _seen_dirs.add(_true_dir)

    for _file in _dir.iterdir():
      _true_file: pathlib.Path = _file.resolve()
      if _true_file.is_dir():
        if _true_file in _seen_dirs:
          logger.trace(f"Skipping already seen directory: {_file}")
          continue
        logger.trace(f"Pushing directory onto the search stack: {_file}")
        _search_stack.append((_cur_depth + 1, _file))
        yield _file
      elif _true_file.is_file():
        yield _file
      else:
        logger.warning(f"Skipping unexpected file: {_file}")
        continue

async def slurp_directory_tree(
  root: pathlib.Path,
  include_extensions: list[str] = [".yml", ".yaml", ".json"],
  ignore_startswith: list[str] = [".", "_"],
  depth: int | None = None,
  io_watcher: IOWatcher = io_watcher,
) -> list[tuple[pathlib.Path, bytes | Exception]]:
  """Slurp a Directory Tree into a list of (Path, Content) tuples.
  Ignore any files & don't search directories starting with a prefix in `ignore_startswith`.
  Include any files with an extension in `include_extensions`.
  If `depth` is provided then the walk will be limited to that depth. `root` has a depth of 0.
  """

  if not io_watcher.running: raise RuntimeError("The IOWatcher must be running to slurp a directory tree")

  _load_files: list[pathlib.Path] = []

  async for file in walk_directory_tree(
    root=root,
    ignore_directory_startswith=ignore_startswith,
    depth=depth,
  ):
    if (
      any(file.name.startswith(_prefix) for _prefix in ignore_startswith) \
      or \
      file.suffix not in include_extensions \
      or \
      not file.is_file()
    ):
      logger.trace(f"Skipping File: {file}")
      continue
    logger.trace(f"Will load resources from manifest file: {file}")
    _load_files.append(file)

  assert all(_file.is_file() for _file in _load_files)
  logger.trace(f"Loading Files: {[str(_f) for _f in _load_files]}")
  # Load the Files Concurrently
  _load_files = list(set(_load_files)) # Deduplicate the files
  _load_results: list[bytes | Exception] = await asyncio.gather(*(
    read_file(_file, io_watcher=io_watcher, return_errors=True)
    for _file in _load_files
  ), return_exceptions=True)
  logger.debug(f"{len(list(filter(lambda x: isinstance(x, Exception), _load_results)))} Errors while loading Files: {list(str(_path) for _path, _result in zip(_load_files, _load_results) if isinstance(_result, Exception))}")
  logger.trace('\n' + '\n'.join(filter(lambda x: not isinstance(x, Exception), map(lambda x: '########\n' + str(x[0]) + '\n########\n' + x[1].decode("utf-8"), zip(_load_files, _load_results)))))

  return list(zip(_load_files, _load_results))

async def slurp_files(
  files: list[pathlib.Path],
  io_watcher: IOWatcher = io_watcher,
) -> list[tuple[pathlib.Path, bytes | Exception]]:
  """Slurp a list of Files into a list of (Path, Content) tuples."""
  if not io_watcher.running: raise RuntimeError("The IOWatcher must be running to slurp files")
  _load_results: list[bytes | Exception] = await asyncio.gather(*(
    read_file(_file, io_watcher=io_watcher, return_errors=True)
    for _file in files
  ), return_exceptions=True)
  logger.debug(f"{len(list(filter(lambda x: isinstance(x, Exception), _load_results)))} Errors while loading Files: {list(str(_path) for _path, _result in zip(files, _load_results) if isinstance(_result, Exception))}")
  logger.trace('\n' + '\n'.join(filter(lambda x: not isinstance(x, Exception), map(lambda x: '########\n' + str(x[0]) + '\n########\n' + x[1].decode("utf-8"), zip(files, _load_results)))))

  return list(zip(files, _load_results))