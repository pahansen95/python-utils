import asyncio
from loguru import logger
from typing import Literal, AsyncContextManager, AsyncGenerator, TypedDict
from contextlib import asynccontextmanager
import tempfile
import os
from pathlib import Path
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization, hashes
import utils.filesystem as fs
from utils.concurrency.aio.watch import IOWatcher
from utils.testing import TestResult, TestCode

class VaultKeys(TypedDict):
  privkey: bytes
  pubkey: bytes
  secret: bytes

def gen_vault_keys(secret: bytes | None = None):
  privkey = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048,
    backend=default_backend()
  )
  pubkey = privkey.public_key()
  if secret is None: secret = os.urandom(32)
  return {
    'privkey': privkey.private_bytes(
      encoding=serialization.Encoding.PEM,
      format=serialization.PrivateFormat.TraditionalOpenSSL,
      encryption_algorithm=serialization.NoEncryption()
    ),
    'pubkey': pubkey.public_bytes(
      encoding=serialization.Encoding.PEM,
      format=serialization.PublicFormat.SubjectPublicKeyInfo
    ),
    'secret': pubkey.encrypt(
      secret,
      padding=padding.OAEP(
        mgf=padding.MGF1(algorithm=hashes.SHA256()),
        algorithm=hashes.SHA256(),
        label=None
      )
    )
  }

def kvstore_context_factory(io_watcher: IOWatcher | None = None) -> AsyncContextManager[tuple[str, IOWatcher]]:
  if io_watcher is None: io_watcher = IOWatcher()
  tmp_dir = Path(os.environ.get('CI_PROJECT_DIR', '.')) / '.cache'
  if not tmp_dir.exists(): tmp_dir = None
  else: tmp_dir = str(tmp_dir)
  @asynccontextmanager
  async def _kvstore_context() -> AsyncGenerator[tuple[str, IOWatcher], None]:
    was_running = io_watcher.running
    try:
      if not was_running: await io_watcher.start()
      assert io_watcher.running
      with tempfile.TemporaryDirectory(dir=tmp_dir) as tmpdirname:
        yield (Path(tmpdirname), io_watcher)
        # input("Press Enter to cleanup Temporary Directories") # Uncomment to pause execution to inspect the temporary directory
    finally:
      if not was_running: await io_watcher.stop()
  return _kvstore_context()

async def test_disk_kvstore_session(*args, **kwargs) -> TestResult:
  from utils.kvstore.backends.disk import DiskStore
  async with kvstore_context_factory() as (tmpdirname, io_watcher):
    kvstore = DiskStore.override_ctx(tmpdirname, io_watcher=io_watcher)
    try:
      assert kvstore.online == False
      await kvstore.connect()
      assert kvstore.online == True
    finally:
      if kvstore.online:
        assert kvstore.online == True
        await kvstore.disconnect()
        assert kvstore.online == False
  return TestResult(code=TestCode.PASS)

async def test_disk_kvstore_interface(*args, **kwargs) -> TestResult:
  from utils.kvstore import MapValue
  from utils.kvstore.backends.disk import DiskStore, PathKey, EMPTY
  from frozendict import frozendict
  async with kvstore_context_factory() as (tmpdirname, io_watcher):
    kvstore = DiskStore.override_ctx(tmpdirname, io_watcher=io_watcher)
    try:
      await kvstore.connect()
      kvstore.register_value(MapValue, MapValue.unmarshal)
      key = PathKey.new('test')
      value = MapValue(frozendict({'foo': 'bar'}))
      await kvstore.set(key, value)
      assert await kvstore.get(key) == value
      _value = await kvstore.pop(key)
      assert _value == value
      empty_val = await kvstore.get(key)
      assert empty_val == EMPTY
      def_val = await kvstore.get(key, default=value)
      assert def_val is value
      try:
        await kvstore.delete(key)
        assert False
      except KeyError:
        pass
      
      # Now create a nested key
      keys = [PathKey.new(k) for k in (
        'test0',
        'test0/nested',
        'test0/nested/a',
        'test0/nested/b',
        'test0/nested/c',
      )]
      vals = [MapValue(frozendict({'key': k()})) for k in keys]
      for k, v in zip(keys, vals):
        await kvstore.set(k, v)
      
      # Test listing a Tree
      found_keys = await kvstore.list(keys[0])
      assert frozenset(found_keys) == frozenset(keys[1:])
      
      # Test Pop/Delete of a (sub) Tree removes all children keys
      nested_key_val = await kvstore.pop(keys[1])
      assert nested_key_val == vals[1]
      found_keys = await kvstore.list(keys[0])
      assert len(found_keys) == 0
      empty_val = await kvstore.get(keys[2])
      assert empty_val == EMPTY

      # Test that creation of a key w/out parents automatically creates the parents
      await kvstore.set(keys[2], vals[2])
      found_keys = await kvstore.list(keys[0])
      assert frozenset(found_keys) == frozenset([keys[1], keys[2]])

      # Test that getting the automatically created parent key returns an EMPTY value
      empty_val = await kvstore.get(keys[1])
      assert empty_val == EMPTY
      def_val = await kvstore.get(keys[1], default=vals[1])
      assert def_val is vals[1]
    finally:
      if kvstore.online: await kvstore.disconnect()

  return TestResult(code=TestCode.PASS)

async def test_vault_kvstore_interface(*args, **kwargs) -> TestResult:
  from utils.kvstore import MapValue
  from utils.kvstore.backends.disk import DiskVault, PathKey, EMPTY
  from frozendict import frozendict
  async with kvstore_context_factory() as (tmpdirname, io_watcher):
    vault_keys: VaultKeys = gen_vault_keys()
    (_key_dir := Path(tmpdirname) / 'keys').mkdir(parents=False, exist_ok=True, mode=0o700)
    (_vault_dir := Path(tmpdirname) / 'vault').mkdir(parents=False, exist_ok=True, mode=0o700)
    (privkey_path := _key_dir / 'privkey.pem').write_bytes(vault_keys['privkey'])
    (pubkey_path := _key_dir / 'pubkey.pem').write_bytes(vault_keys['pubkey'])
    (secret_path := _key_dir / 'secret').write_bytes(vault_keys['secret'])
    kvstore = DiskVault.override_ctx(
      dir_tree_root=_vault_dir,
      secret_file=secret_path,
      privkey_file=privkey_path,
      pubkey_file=pubkey_path,
      io_watcher=io_watcher
    )
    assert kvstore._ctx.io_watcher is io_watcher
    assert kvstore._ctx.io_watcher.running
    try:
      await kvstore.connect()
      kvstore.register_value(MapValue, MapValue.unmarshal)
      key = PathKey.new('test')
      value = MapValue(frozendict({'foo': 'bar'}))
      await kvstore.set(key, value)

      # Directly load the Key Value & assert it's encrypted
      # raw_value = await fs.read_file(_vault_dir / key().lstrip('/') / 'val.bin', io_watcher=io_watcher)
      _, raw_val = await (super(DiskVault, kvstore)._read_key)(key())
      assert raw_val != value.marshal()
      assert raw_val.startswith(b'Salted__')
      assert await kvstore.get(key) == value
      _value = await kvstore.pop(key)
      assert _value == value
      empty_val = await kvstore.get(key)
      assert empty_val == EMPTY
      def_val = await kvstore.get(key, default=value)
      assert def_val is value
      try:
        await kvstore.delete(key)
        assert False
      except KeyError:
        pass
      
      # Now create a nested key
      keys = [PathKey.new(k) for k in (
        'test0',
        'test0/nested',
        'test0/nested/a',
        'test0/nested/b',
        'test0/nested/c',
      )]
      vals = [MapValue(frozendict({'key': k()})) for k in keys]
      for k, v in zip(keys, vals):
        await kvstore.set(k, v)
      
      # Test listing a Tree
      found_keys = await kvstore.list(keys[0])
      assert frozenset(found_keys) == frozenset(keys[1:])
      
      # Test Pop/Delete of a (sub) Tree removes all children keys
      nested_key_val = await kvstore.pop(keys[1])
      assert nested_key_val == vals[1]
      found_keys = await kvstore.list(keys[0])
      assert len(found_keys) == 0
      empty_val = await kvstore.get(keys[2])
      assert empty_val == EMPTY

      # Test that creation of a key w/out parents automatically creates the parents
      await kvstore.set(keys[2], vals[2])
      found_keys = await kvstore.list(keys[0])
      assert frozenset(found_keys) == frozenset([keys[1], keys[2]])

      # Test that getting the automatically created parent key returns an EMPTY value
      empty_val = await kvstore.get(keys[1])
      assert empty_val == EMPTY
      def_val = await kvstore.get(keys[1], default=vals[1])
      assert def_val is vals[1]
    finally:
      if kvstore.online: await kvstore.disconnect()

  return TestResult(code=TestCode.PASS)

__all__ = [
  "test_disk_kvstore_session",
  "test_disk_kvstore_interface",
  "test_vault_kvstore_interface"
]
