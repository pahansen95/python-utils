from __future__ import annotations
from dataclasses import dataclass, field, KW_ONLY, fields
from pathlib import Path
from typing import Any
from collections.abc import ByteString, Callable
import asyncio
import enum
import orjson
from loguru import logger

from .. import KVStore, Key, Value, EmptyValue, PathKey, EMPTY, EMPTY_META
import utils.concurrency.aio.watch as aiow
import utils.filesystem as fs
from utils.itertools import group_by, chain_from_iter

class LockMode(enum.Enum):
  """The Mode of the Lock"""
  READ = enum.auto()
  """Read Lock"""
  WRITE = enum.auto()
  """Write Lock"""

@dataclass
class FileLock:
  """A File Lock"""
  # path: Path
  # """The Path to the File """
  lock: asyncio.Lock = field(default_factory=asyncio.Lock)
  """The Runtime Lock"""
  ### TODO: Implement Advisory FLocks ###

  @property
  def locked(self) -> bool:
    """Is the Lock Acquired"""
    return self.lock.locked()

  async def __aenter__(self):
    # if not self.path.exists(): raise FileNotFoundError(f"File {self.path} does not exist")
    await self.acquire()
    return self

  async def __aexit__(self, exc_type, exc_value, traceback):
    await self.release()
  
  async def acquire(self):
    """Acquire the Lock"""
    await self.lock.acquire()
  
  async def release(self):
    """Release the Lock"""
    self.lock.release()

@dataclass
class _StoreCtx:
  """The Stateful Context of the Disk Store"""

  online: asyncio.Event = field(default_factory=asyncio.Event)
  """Is the KV Store Online"""
  dir_tree_root_mode: int | None = None
  """The Mode of the Directory Tree Root"""
  io_watcher: aiow.IOWatcher = field(default_factory=lambda: aiow.io_watcher)
  """The IO Watcher being used"""
  open_keys: dict[str, dict[str, tuple[fs.AsyncFileDescriptor, fs.AsyncFileDescriptor]]] = field(default_factory=dict)
  """The Current set of Open FileDescriptors associated with a Key in the Store"""
  flocks: dict[str, dict[LockMode, FileLock]] = field(default_factory=dict)
  """The Current set of File Locks associated with a Key in the Store"""
  factory: dict[str, Callable[[ByteString], Value]] = field(default_factory=dict)
  """The Set of Registered Value Factories"""

_metadata_t = dict[str, str | int | float | bool | None | list[str | int | float | bool | None] | dict[str, str | int | float | bool | None]]
"""A Mapping of String Keys to JSON Encodable Values. Recommended not to deeply nest values."""

@dataclass
class DiskStore(KVStore):
  """A Disk Based Hierarchical Key Value Store"""

  dir_tree_root: Path
  """The Path to the Root of the Directory Tree"""

  _: KW_ONLY
  _ctx: _StoreCtx = field(default_factory=_StoreCtx)

  ### Contstructors ###

  @classmethod
  def override_ctx(cls, *args: Any, **kwargs: Any) -> DiskStore:
    """Override the Context of the Store"""
    if "_ctx" in kwargs: raise ValueError("Cannot override the Context of a DiskStore directly; please specify the Context Parameters directly")
    cls_kwargs = {f.name: kwargs.pop(f.name) for f in fields(cls) if f.name in kwargs}
    return cls(
      *args,
      **(cls_kwargs | {"_ctx": _StoreCtx(**kwargs)})
    )

  ### Internal Interface ###

  def _get_fs_modes(self) -> tuple[int, int]:
    """Get the Filesystem Modes for Directories & Files"""
    if self._ctx.dir_tree_root_mode is None: self._ctx.dir_tree_root_mode = self.dir_tree_root.stat().st_mode
    # For the File Mode, just remove the execute bit from the directory mode
    return (self._ctx.dir_tree_root_mode, self._ctx.dir_tree_root_mode & ~0o111)

  def _key_depth(self, key: str) -> int: return key.lstrip('/').count('/')
  def _key_to_disk_path(self, key: str) -> Path: return self.dir_tree_root / key.lstrip('/')
  def _disk_path_to_key(self, path: Path) -> str: return '/' + path.relative_to(self.dir_tree_root).as_posix()
  def _key_exists(self, key: str) -> bool: return self._key_to_disk_path(key).exists()

  async def _get_key_lock(self, key: str, mode: LockMode) -> FileLock:
    """Get the Lock for a Key"""
    if key not in self._ctx.flocks:
      _lock = FileLock() # TODO Allow Multiple Read Locks per key but exclusive Write Locks
      self._ctx.flocks[key] = {
        LockMode.READ: _lock,
        LockMode.WRITE: _lock,
      }
    return self._ctx.flocks[key][mode]

  async def _cleanup_key_lock(self, key: str):
    """Remove the Lock(s) for a Key releasing them if they are held"""
    assert key in self._ctx.flocks
    lock = self._ctx.flocks[key]
    async def _release_lock(lock: FileLock):
      if lock.locked: await lock.release()
    await asyncio.gather(*(_release_lock(l) for l in lock.values() if l.locked))

  async def _open_key(self, key: str):
    """Open the Key's File Handles"""
    if not self.dir_tree_root.exists(): raise FileNotFoundError(f"Directory Tree Root {self.dir_tree_root} does not exist")
    _dir = self._key_to_disk_path(key)
    _val = _dir / "val.bin"
    _meta = _dir / "meta.json"
    if not _val.exists() or not _meta.exists():
      dir_mode, file_mode = self._get_fs_modes()
      _dir.mkdir(mode=dir_mode, parents=True, exist_ok=True)
      _val.touch(mode=file_mode, exist_ok=True)
      _meta.touch(mode=file_mode, exist_ok=True)

    self._ctx.open_keys[key] = {
      "meta": await fs.open_file(_meta, fs.FileMode.RDRW | fs.FileMode.APPEND, io_watcher=self._ctx.io_watcher),
      "val": await fs.open_file(_val, fs.FileMode.RDRW | fs.FileMode.APPEND, io_watcher=self._ctx.io_watcher),
    }
    assert isinstance(self._ctx.open_keys[key], dict), self._ctx.open_keys[key]
    assert isinstance(self._ctx.open_keys[key]["meta"], tuple), self._ctx.open_keys[key]["meta"]
    assert isinstance(self._ctx.open_keys[key]["meta"][0], fs.AsyncFileDescriptor), self._ctx.open_keys[key]["meta"][0]
    assert isinstance(self._ctx.open_keys[key]["meta"][1], fs.AsyncFileDescriptor), self._ctx.open_keys[key]["meta"][1]
    assert isinstance(self._ctx.open_keys[key]["val"][0], fs.AsyncFileDescriptor), self._ctx.open_keys[key]["val"][0]
    assert isinstance(self._ctx.open_keys[key]["val"][1], fs.AsyncFileDescriptor), self._ctx.open_keys[key]["val"][1]

  async def _close_key(self, key: str):
    """Close the Key's open File Handles"""
    assert key in self._ctx.open_keys
    await asyncio.gather(*(
      fs.close_file(*self._ctx.open_keys[key][k], io_watcher=self._ctx.io_watcher)
      for k in self._ctx.open_keys[key]
    ))
    del self._ctx.open_keys[key]
  
  async def _remove_key(self, key: str):
    """Remove the Key from the Store"""
    assert key not in self._ctx.open_keys
    _dir = self._key_to_disk_path(key)
    if not _dir.exists(): return
    await asyncio.gather(*(
      fs.delete_file(_dir / "val.bin"),
      fs.delete_file(_dir / "meta.json"),
    ))
    logger.trace(f"{[f for f in _dir.iterdir()]=}")
    await fs.delete_directory(_dir)

  async def _read_key(self, key: str) -> tuple[_metadata_t, bytes]:
    """Read the Metadata & Value of a Key"""
    assert key in self._ctx.open_keys
    await asyncio.gather(*( # Reset the File Pointers First
      fs.seek(self._ctx.open_keys[key]["meta"][0], 0),
      fs.seek(self._ctx.open_keys[key]["val"][0], 0),
    ))
    _data = tuple(await asyncio.gather(*(
      self._ctx.open_keys[key]["meta"][0].read(fs.IOCondition.EOF),
      self._ctx.open_keys[key]["val"][0].read(fs.IOCondition.EOF)
    )))
    return (
      orjson.loads(_data[0]) if len(_data[0]) > 0 else EMPTY_META,
      _data[1]
    )
  
  async def _write_key(self, key: str, metadata: _metadata_t, value: ByteString):
    """Write the Metadata & Value of a Key"""
    assert key in self._ctx.open_keys
    await asyncio.gather(*( # Truncate the Files First
      fs.truncate(self._ctx.open_keys[key]["meta"][1], 0),
      fs.truncate(self._ctx.open_keys[key]["val"][1], 0),
    ))
    _metadata_bytes = orjson.dumps(metadata)
    _bytes_written = await asyncio.gather(*(
      self._ctx.open_keys[key]["meta"][1].write(_metadata_bytes, len(_metadata_bytes)),
      self._ctx.open_keys[key]["val"][1].write(value, len(value))
    ))
    assert _bytes_written == [len(_metadata_bytes), len(value)]
  
  async def _list_key_tree(self, key: str, depth: int | None = None) -> list[str]:
    """List the keys in the tree rooted at the given key, to the given depth or `None` to walk the entire tree."""
    if depth is not None and depth < 1: raise ValueError("Depth must be at least 1; Depth 0 is the key itself.")
    _path = self._key_to_disk_path(key)
    if not _path.exists(): raise FileNotFoundError(f"Key {key} does not exist on disk")
    _children = []
    async for _f in fs.walk_directory_tree(_path, depth=depth):
      if _f.resolve().is_dir(): _children.append(self._disk_path_to_key(_f))
    return _children

  ### Session Management ###

  @property
  def online(self) -> bool: return self._ctx.online.is_set()

  async def connect(self):
    """Open a session w/ the KVStore"""
    if self._ctx.online.is_set(): raise RuntimeError("KVStore is already online")
    if not self.dir_tree_root.exists(): raise RuntimeError(f"Directory Tree Root {self.dir_tree_root} does not exist")
    if not self.dir_tree_root.is_dir(): raise RuntimeError(f"Directory Tree Root {self.dir_tree_root} is not a directory")
    if not self._ctx.io_watcher.running: raise RuntimeError("IOWatcher is not running")

    self._ctx.online.set()
  
  async def disconnect(self):
    """Close the KVStore Session & cleanup resources"""
    if not self._ctx.online.is_set(): raise RuntimeError("KVStore is already offline")

    _error = False

    # Raise an error if any file is currently locked by this session
    for k, v in self._ctx.flocks.items():
      for l in v.values():
        if l.lock.locked(): raise RuntimeError(f"File {k} is still locked by this session")

    # Close all files
    close_errors: list[Exception | None] = await asyncio.gather(*(
      self._close_key(k)
      for k in self._ctx.open_keys
    ))
    for e, k in zip(close_errors, self._ctx.open_keys):
      if e is not None:
        _error = True
        logger.opt(exception=e).warning(f"Error closing files for key {k}")
    
    # Cleanup all locks
    cleanup_errors: list[Exception | None] = await asyncio.gather(*(
      self._cleanup_key_lock(k)
      for k in self._ctx.flocks
      if k not in self._ctx.open_keys # In case of errors, don't try to cleanup locks for open keys
    ))
    for e, k in zip(cleanup_errors, self._ctx.flocks):
      if e is not None:
        _error = True
        logger.opt(exception=e).warning(f"Error cleaning up locks for key {k}")

    if _error: raise RuntimeError("Error Stopping DiskStore")
    self._ctx.online.clear()

  ### KVStore Interface ###

  def is_registered(self, kind: type[Value]) -> bool:
    """Is a Value Type registered with the Store"""
    if not self._ctx.online.is_set(): raise RuntimeError("KVStore is offline")
    return str(kind) in self._ctx.factory
  
  def register_value(self, kind: type[Value], factory: Callable[[ByteString], Value]) -> None:
    """Register a Value Type with the Store"""
    if not self._ctx.online.is_set(): raise RuntimeError("KVStore is offline")
    self._ctx.factory[str(kind)] = factory
  
  def deregister_value(self, kind: type[Value]) -> None:
    """Deregister a Value Type with the Store"""
    if not self._ctx.online.is_set(): raise RuntimeError("KVStore is offline")
    del self._ctx.factory[str(kind)]

  async def exists(self, key: PathKey) -> bool:
    """Does the Key exist in the Store"""
    if not self._ctx.online.is_set(): raise RuntimeError("KVStore is offline")
    return self._key_exists(key())

  async def list(self, key: PathKey) -> list[PathKey]:
    """List the Keys in the Store"""
    if not self._ctx.online.is_set(): raise RuntimeError("KVStore is offline")
    _key = key()
    if not self._key_exists(_key): raise ValueError(f"Key {_key} does not exist in the Store")
    _keys = await self._list_key_tree(_key, depth=1)
    return [PathKey.new(k) for k in _keys]

  async def get(self, key: PathKey, default: Value = EMPTY) -> Value | EmptyValue:
    """Get a Value from the Store; if the key does not exist or exists but holds no value returns an EmptyValue."""
    if not self._ctx.online.is_set(): raise RuntimeError("KVStore is offline")
    _key = key()
    if not self._key_exists(_key): return default
    lock = await self._get_key_lock(_key, LockMode.READ)
    async with lock:
      if key not in self._ctx.open_keys: await self._open_key(_key)
      _meta, _val = await self._read_key(_key)
      if _meta == EMPTY_META: return default
      _kind = _meta["kind"]
      if _kind not in self._ctx.factory: raise ValueError(f"Value Kind `{_kind}` is not registered with the Store")
      return self._ctx.factory[_kind](_val)
  
  async def set(self, key: PathKey, value: Value) -> None:
    """Set a Value in the Store"""
    if not self._ctx.online.is_set(): raise RuntimeError("KVStore is offline")
    _kind = str(type(value))
    if _kind not in self._ctx.factory: raise ValueError(f"Value Kind `{_kind}` is not registered with the Store")
    _key = key()
    lock = await self._get_key_lock(_key, LockMode.READ)
    async with lock:
      if key not in self._ctx.open_keys: await self._open_key(_key) # This also creates the key if it doesn't exist
      await self._write_key(_key, {"kind": _kind}, value.marshal())
  
  async def _delete(self, key: PathKey, pop: bool, default: Value) -> Value | None:
    """Delete a Value from the Store"""
    _key = key()
    if not self._key_exists(_key): raise KeyError(f"Key {_key} does not exist in the Store")
    # Get all the children of the key
    _all_keys = [_key] + await self._list_key_tree(_key)
    logger.trace(f"All Keys: {_all_keys}")
    # Group & Sort by depth
    _keys_by_depth = [list(keys_iter) for _, keys_iter in sorted(
      group_by(_all_keys, key=self._key_depth),
      key=lambda g: g[0],
      reverse=True, # Sort Descending
    )]
    logger.trace(f"Keys by Depth: {_keys_by_depth}")
    
    # Exclusively lock all the keys
    locks = await asyncio.gather(*(self._get_key_lock(_k, LockMode.WRITE) for _k in _all_keys))
    await asyncio.gather(*(l.acquire() for l in locks))
    async def _unlock_all(): await asyncio.gather(*(l.release() for l in locks))

    # Assign the value to be popped, if any
    _pop_val = None
    if pop:
      if key not in self._ctx.open_keys: await self._open_key(_key)
      _meta, _val = await self._read_key(_key)
      if _val == b"": _pop_val = default
      _kind = _meta["kind"]
      if _kind not in self._ctx.factory: raise ValueError(f"Value Kind `{_kind}` is not registered with the Store")
      _pop_val = self._ctx.factory[_kind](_val)

    try:
      # Close all the keys
      await asyncio.gather(*(self._close_key(_k) for _k in _all_keys if _k in self._ctx.open_keys))
      # Delete all the keys; bottom up
      assert len(_keys_by_depth) > 0
      for _key_set in _keys_by_depth:
        assert len(_key_set) > 0
        logger.trace(f"Deleting Keys: {_key_set}")
        await asyncio.gather(*(self._remove_key(_k) for _k in _key_set))
    except Exception as e:
      logger.opt(exception=e).warning(f"Error Deleting Key {_key}")
      # Unlock all the keys
      await _unlock_all()
      raise e

    for k in _all_keys: assert not self._key_exists(k)
        
    # Cleanup the Key Locks
    await asyncio.gather(*(self._cleanup_key_lock(_k) for _k in _all_keys))

    return _pop_val

  async def delete(self, key: Key) -> None:
    """Delete a Value (& it's children) from the Store"""
    if not self._ctx.online.is_set(): raise RuntimeError("KVStore is offline")
    await self._delete(key, False, None)

  async def pop(self, key: Key, default: Value = EMPTY) -> Value:
    """Delete a Value (& it's children) from the Store and return just the key's value (not any of the children)"""
    if not self._ctx.online.is_set(): raise RuntimeError("KVStore is offline")
    return await self._delete(key, True, default)