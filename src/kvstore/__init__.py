from __future__ import annotations
from typing import Protocol, Any, Generic, TypeVar, runtime_checkable
from abc import abstractmethod
from collections.abc import ByteString, Callable, Iterable, Mapping
from dataclasses import dataclass, field, KW_ONLY
from pathlib import PurePath
import orjson
from frozendict import frozendict

T = TypeVar('T')

@runtime_checkable
class Key(Generic[T], Protocol):
  """A Key in the KV Store"""

  @abstractmethod
  def __call__(self) -> T: ...

  @classmethod
  @abstractmethod
  def new(cls, key: T) -> Key[T]: ...

@runtime_checkable
class Value(Generic[T], Protocol):
  """A Value in the KV Store"""

  @abstractmethod
  def __call__(self) -> T: ...

  @abstractmethod
  def marshal(self) -> bytes: ...

  @classmethod
  @abstractmethod
  def unmarshal(self, data: ByteString) -> Value[T]: ...

  @classmethod
  @abstractmethod
  def new(cls, val: T) -> Value[T]: ...


K = TypeVar('K', bound=Key)
V = TypeVar('V', bound=Value)

@runtime_checkable
class KVStore(Generic[K, V], Protocol):
  """The Key Value Store Protocol"""

  @property
  @abstractmethod
  def online(self) -> bool: ...
  @abstractmethod
  async def connect(self) -> None: ...
  @abstractmethod
  async def disconnect(self) -> None: ...

  @abstractmethod
  def is_registered(self, kind: type[K]) -> bool: ...
  @abstractmethod
  def register_value(self, kind: type[K], factory: Callable[[ByteString], Value[T]]) -> None: ...
  @abstractmethod
  def deregister_value(self, kind: type[K]) -> None: ...

  @abstractmethod
  async def exists(self, key: K) -> bool: ...
  @abstractmethod
  async def list(self, key: K) -> Iterable[K]: ...
  @abstractmethod
  async def get(self, key: K, default: V) -> V: ...
  @abstractmethod
  async def set(self, key: K, value: V) -> None: ...
  @abstractmethod
  async def pop(self, key: K, default: V) -> V: ...
  @abstractmethod
  async def delete(self, key: K) -> None: ...

### Concrete Implementations ###

@dataclass(frozen=True)
class PathKey(Key[str]):
  """A Key Represented by a Path"""
  p: list[str]
  def __hash__(self) -> int:
    return hash(tuple(self.p))
  def __call__(self) -> str: return PurePath('/', *self.p).as_posix()
  def __iter__(self) -> Iterable[str]: return iter(self.p)
  @classmethod
  def from_parts(cls, *part: str) -> PathKey: return cls(list(filter(lambda x: x != '', map(lambda x: x.strip('/'), part))))
  @classmethod
  def new(cls, val: str) -> PathKey: return cls.from_parts(*val.split('/'))

@dataclass(frozen=True)
class EmptyValue(Value[None]):
  """An Empty Value"""
  
  def __call__(self) -> None: return None
  def marshal(self) -> bytes: return b""
  @classmethod
  def unmarshal(cls, data: ByteString = b'') -> Value[None]:
    if data != b'': raise ValueError(f"Invalid Unmarshal Data: {data}")
    return cls()
  @classmethod
  def new(cls) -> Value[None]: return cls()
EMPTY = EmptyValue()
EMPTY_META = {'kind': EMPTY.__class__.__name__}

@dataclass(frozen=True)
class MapValue(Value[Mapping[str, Any]]):
  """A Value that is itself a Mapping of string Keys to Any Value. Should be JSON encodable. When creating the Value, you must use a Frozendict to ensure immutability."""
  obj: frozendict[str, Any]
  
  def __call__(self) -> dict[str, Any]: return dict(self.obj)
  def marshal(self) -> bytes: return orjson.dumps(dict(self.obj), option=orjson.OPT_INDENT_2)
  @classmethod
  def unmarshal(cls, data: ByteString) -> MapValue[dict[str, Any]]: return cls(frozendict(orjson.loads(data)))
  @classmethod
  def new(cls, val: dict[str, Any]) -> MapValue[dict[str, Any]]: return cls(frozendict(val))

@dataclass(frozen=True)
class BytesValue(Value[bytes]):
  """A Value that is a Byte String"""
  obj: bytes
  
  def __call__(self) -> bytes: return self.obj
  def marshal(self) -> bytes: return self.obj
  @classmethod
  def unmarshal(cls, data: ByteString) -> BytesValue: return cls(bytes(data))
  @classmethod
  def new(cls, val: ByteString) -> BytesValue: return cls(bytes(val))