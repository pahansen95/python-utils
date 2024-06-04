from utils.testing import test_registry

from .tests.itemlog import (
  test_ItemLog_unbounded, test_ItemLog_bounded,
)
from .tests.aio import (
  test_fd_read_file, test_fd_read_stream,
  test_fd_write_file, test_fd_write_stream,
  test_AsyncFileDescriptor_read_file, test_AsyncFileDescriptor_read_stream,
  test_AsyncFileDescriptor_write_file, test_AsyncFileDescriptor_write_stream,
)
from .tests.diskkvstore import (
  test_disk_kvstore_session,
  test_disk_kvstore_interface,
  test_vault_kvstore_interface,
)

test_registry.register("utils.concurrency.log", "ItemLog_unbounded", test_ItemLog_unbounded)
test_registry.register("utils.concurrency.log", "ItemLog_bounded", test_ItemLog_bounded)
test_registry.register("utils.concurrency.aio", "fd_read_file", test_fd_read_file)
test_registry.register("utils.concurrency.aio", "fd_read_stream", test_fd_read_stream)
test_registry.register("utils.concurrency.aio", "fd_write_file", test_fd_write_file)
test_registry.register("utils.concurrency.aio", "fd_write_stream", test_fd_write_stream)
test_registry.register("utils.concurrency.aio", "AsyncFileDescriptor.read_file", test_AsyncFileDescriptor_read_file)
test_registry.register("utils.concurrency.aio", "AsyncFileDescriptor.read_stream", test_AsyncFileDescriptor_read_stream)
test_registry.register("utils.concurrency.aio", "AsyncFileDescriptor.write_file", test_AsyncFileDescriptor_write_file)
test_registry.register("utils.concurrency.aio", "AsyncFileDescriptor.write_stream", test_AsyncFileDescriptor_write_stream)
test_registry.register("utils.kvstore.backends.disk", "DiskStore.SessionMgmt", test_disk_kvstore_session)
test_registry.register("utils.kvstore.backends.disk", "DiskStore.CRUD", test_disk_kvstore_interface)
test_registry.register("utils.kvstore.backends.disk", "Vault.CRUD", test_vault_kvstore_interface)