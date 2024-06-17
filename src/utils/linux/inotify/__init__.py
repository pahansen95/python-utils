from __future__ import annotations
import pathlib
from utils.extern import active_backend, SystemBackend
from utils.extern.c import calculate_module_fingerprint, BuildSpec, c_registry, c_malloc, c_buffer, c_from_buffer

if active_backend != SystemBackend.LINUX: raise RuntimeError("INotify is only supported on Linux")

_CDEF = """\
void c_inotify_init(int flags, int result[2]);
void c_inotify_add_watch(int fd, const char *path, int mask, int result[2]);
void c_inotify_rm_watch(int fd, int watchdesc, int result[1]);
"""
LIB_BUILD_SPEC: BuildSpec = {
  'cdef': _CDEF,
  'include': ['linuxinotify.h'],
  'sources': ['src/linuxinotify.c'],
}
_lib_path = pathlib.Path(__file__).parent
if 'linux_inotify' not in c_registry.modules:
  _lib_fingerprint = calculate_module_fingerprint(_lib_path, LIB_BUILD_SPEC)
  c_registry.add_module("linux_inotify", LIB_BUILD_SPEC)
  c_registry.build_module("linux_inotify", _lib_path, _lib_fingerprint)

### Final Imports ###
import linux_inotify
import weakref, enum, pathlib, time, asyncio, os
from loguru import logger
from copy import deepcopy
from dataclasses import dataclass, field, KW_ONLY
from typing import AsyncGenerator, TypedDict, NotRequired, ByteString
from contextlib import asynccontextmanager
from utils.concurrency.aio.fd import AsyncFileDescriptor, IOCondition, IOEvent
from utils.concurrency.aio.watch import io_watcher, IOWatcher
from utils.concurrency.log import ItemLog
from utils.errors import Error, NO_ERROR, NO_ERROR_T

def _create_event(state: bool) -> asyncio.Event:
  event = asyncio.Event()
  if state: event.set()
  return event

class INotifyError(enum.Enum):
  """INotify Error Codes"""
  ...

class INotifyMask(enum.IntFlag):
  """A Mask of INotify Events."""
  NONE = 0
  ### inotify_add_watch(2) Flags ###
  IN_ACCESS = linux_inotify.IN_ACCESS
  IN_ATTRIB = linux_inotify.IN_ATTRIB
  IN_CLOSE_WRITE = linux_inotify.IN_CLOSE_WRITE
  IN_CLOSE_NOWRITE = linux_inotify.IN_CLOSE_NOWRITE
  IN_CREATE = linux_inotify.IN_CREATE
  IN_DELETE = linux_inotify.IN_DELETE
  IN_DELETE_SELF = linux_inotify.IN_DELETE_SELF
  IN_MODIFY = linux_inotify.IN_MODIFY
  IN_MOVE_SELF = linux_inotify.IN_MOVE_SELF
  IN_MOVED_FROM = linux_inotify.IN_MOVED_FROM
  IN_MOVED_TO = linux_inotify.IN_MOVED_TO
  IN_OPEN = linux_inotify.IN_OPEN
  IN_MOVED = linux_inotify.IN_MOVED
  IN_CLOSE = linux_inotify.IN_CLOSE
  ### inotify_add_watch(2) Flags ###
  IN_DONT_FOLLOW = linux_inotify.IN_DONT_FOLLOW
  IN_EXCL_UNLINK = linux_inotify.IN_EXCL_UNLINK
  IN_MASK_ADD = linux_inotify.IN_MASK_ADD
  IN_ONESHOT = linux_inotify.IN_ONESHOT
  IN_ONLYDIR = linux_inotify.IN_ONLYDIR
  IN_MASK_CREATE = linux_inotify.IN_MASK_CREATE
  ### extra read(2) Flags ###
  IN_IGNORED = linux_inotify.IN_IGNORED
  IN_ISDIR = linux_inotify.IN_ISDIR
  IN_Q_OVERFLOW = linux_inotify.IN_Q_OVERFLOW
  IN_UNMOUNT = linux_inotify.IN_UNMOUNT

def inotify_init(flags: INotifyMask) -> tuple[Error | NO_ERROR_T, int | None]:
  c_err, fd = linux_inotify.c_inotify_init(flags)
  if c_err: return { 'kind': ..., 'msg': ... }, None
  return NO_ERROR, fd

def inotify_add_watch(fd: int, path: str, mask: INotifyMask) -> tuple[Error | NO_ERROR_T, int | None]:
  c_err, watchdesc = linux_inotify.c_inotify_add_watch(fd, path, mask)
  if c_err: return { 'kind': ..., 'msg': ... }, None
  return NO_ERROR, watchdesc

def inotify_rm_watch(fd: int, watchdesc: int) -> Error | NO_ERROR_T:
  c_err = linux_inotify.c_inotify_rm_watch(fd, watchdesc)
  if c_err: return { 'kind': ..., 'msg': ... }
  return NO_ERROR

class FSEvent(enum.IntFlag):
  """A Filesystem Event"""
  UNDEFINED = 0
  """Placeholder for an undefined event."""

  ### Standard CRUD Events ###
  CREATE = 1 << 0
  """A file was created"""
  READ = 1 << 1
  """A File was Read from"""
  WRITE = 1 << 2
  """A File was Written to"""
  DELETE = 1 << 3
  """A File was Deleted"""

  ### File Lifecycle Events ###
  OPEN = 1 << 4
  """A File was openend"""
  CLOSE = 1 << 5
  """A File was closed"""
  MOVE = 1 << 6
  """A File was moved"""
  ATTRIB = 1 << 7
  """A File's metadata attributes where modified"""

class MonitorState(TypedDict):
  """A Snapshot of the Monitor Cache"""
  filepaths: dict[int, pathlib.Path]
  """A Mapping of Watch Descriptors to their File Paths"""
  states: dict[int, FileState]
  """A Mapping of Watch Descriptors to their States"""
  related: dict[int, set[int]]
  """Related events such as a rename: { cookie: set(WatchDescriptor, ...) }"""
  start: int
  """The absolute start time of the monitoring as a monotonic ns timestamp"""

  class FileState(TypedDict):
    """The State of a File"""
    event: FSEvent
    """The Event that occured"""
    when: int
    """The time the event was recorded as a monotonic ns timestamp"""
    cookie: int
    """The Unique Cookie"""

class INotifyEvent(TypedDict):
  """A INotify Event Mapping extracted from the inotify_event struct.
  
  > see inotify(7) for indepth information

  ```c
    struct inotify_event {
      int      wd;       /* Watch descriptor */
      uint32_t mask;     /* Mask describing event */
      uint32_t cookie;   /* Unique cookie associating related
                            events (for rename(2)) */
      uint32_t len;      /* Size of name field */
      char     name[];   /* Optional null-terminated name */
    };
  ```
  
  """
 
  wd: int
  """The Watch Descriptor"""
  mask: INotifyMask
  """The Mask of Events"""
  cookie: int
  """The Unique Cookie"""
  name: str
  """The Name of the File"""

  @staticmethod
  def extract(struct: ByteString) -> INotifyEvent:
    """Extracts an INotifyEvent from a inotify_event struct."""
    ...

  @staticmethod
  async def read_inotify_event_struct(fd: AsyncFileDescriptor) -> bytes:
    """Read a inotify_event struct from a filedescriptor & Extract an INotifyEvent"""
    ... # See: https://github.com/dsoprea/PyInotify/blob/master/inotify/adapters.py#L143

class _Ctx(TypedDict):
  """Stateful Context for the INotify Instance"""
  cache: NotRequired[MonitorState]
  """The Current State of the INotify Event Cache"""
  watchdesc_lookup: dict[pathlib.Path, int]
  """A Lookup Table for Watch Descriptors"""
  listen_log: ItemLog[int]
  """An internal Log to track Watch Descriptors that had an event occur"""
  tasks: dict[str, asyncio.Task]
  """A Mapping of Task Keys to their Tasks"""
  inactive: asyncio.Event
  """An Event to signal nothing is actively being monitored"""

  @staticmethod
  def factory(**kwargs) -> _Ctx:
    return {
      'cache': {
        'filepaths': {},
        'states': {},
        'related': {},
        'start': 0,
      },
      'watchdesc_lookup': {},
      'listen_log': ItemLog[int](),
      'tasks': {},
      'inactive': _create_event(True),
    } | kwargs

@dataclass
class FSMonitor:
  """Provides Notifications of Filesystem Events.
  
  ### TODO

  - Currently, we capture the current state of the watched files; should we
    retain a log of events? What could we use that for?
  
  """

  afd: AsyncFileDescriptor
  """The Asynchronous Handle to the FileDescriptor of the INotify Instance"""

  _: KW_ONLY
  _ctx: _Ctx = field(default_factory=_Ctx.factory)

  @staticmethod
  def factory(flags: INotifyMask = INotifyMask.NONE, io_watcher: IOWatcher = io_watcher, ctx: _Ctx = None) -> FSMonitor:
    """Factory Function that initializes a new INotify instance."""
    err, fd = inotify_init(flags)
    if err is not NO_ERROR: raise NotImplementedError
    afd = AsyncFileDescriptor(
      fd=fd,
      kind='other',
      event_log=io_watcher.register(fd, IOEvent.READ),
    )
    kwargs = { 'afd': afd }
    if ctx is not None: kwargs['_ctx'] = ctx
    return FSMonitor(**kwargs)
  
  @asynccontextmanager
  @staticmethod
  async def session() -> AsyncGenerator[FSMonitor, None]:
    """Context Manager that provides a Session with an INotify Instance. Cleans up resources on exit."""
    monitor = FSMonitor.factory()
    try:
      yield monitor
    finally:
      await monitor.cleanup()
  
  def __post_init__(self):
    ... # Add finalizer to ensure resource cleanup
  
  async def _loop_monitor(self):
    """The Monitoring loop; it purely listens for INotify Events & ensures the cache is updated."""
    while True:
      inotify_event = INotifyEvent.extract(
        await INotifyEvent.read_inotify_event_struct(self.afd)
      )
      when = time.monotonic_ns()

      # Map INotify Events to FSEvents
      event = FSEvent.UNDEFINED
      ### Crud ###
      if INotifyMask.IN_CREATE & inotify_event['mask']: event |= FSEvent.CREATE
      if INotifyMask.IN_ACCESS & inotify_event['mask']: event |= FSEvent.READ
      if INotifyMask.IN_MODIFY & inotify_event['mask']: event |= FSEvent.WRITE
      if INotifyMask.IN_DELETE & inotify_event['mask']: event |= FSEvent.DELETE
      elif INotifyMask.IN_DELETE_SELF & inotify_event['mask']: event |= FSEvent.DELETE
      ### Lifecycle ###
      if INotifyMask.IN_ATTRIB & inotify_event['mask']: event |= FSEvent.ATTRIB
      if INotifyMask.IN_CLOSE & inotify_event['mask']: event |= FSEvent.CLOSE
      if INotifyMask.IN_OPEN & inotify_event['mask']: event |= FSEvent.OPEN
      if INotifyMask.IN_MOVED & inotify_event['mask']: event |= FSEvent.MOVE
      elif INotifyMask.IN_MOVE_SELF & inotify_event['mask']: event |= FSEvent.MOVE

      # Update the Cache
      assert inotify_event['wd'] in self._ctx['cache']['filepaths']
      self._ctx['cache']['states'][inotify_event['wd']] = {
        'event': event,
        'when': when,
        'cookie': inotify_event['cookie'],
      }
      if inotify_event['cookie'] != 0:
        if inotify_event['cookie'] not in self._ctx['cache']['related']:
          self._ctx['cache']['related'][inotify_event['cookie']] = set()
        self._ctx['cache']['related'][inotify_event['cookie']].add(
          inotify_event['wd']
        )

      # Let the listener know about the event
      self._ctx['listen_log'].push(inotify_event['wd'])

      # Ensure adquete yielding to the event loop
      await asyncio.sleep(0)

  async def cleanup(self, close: bool = True):
    """Cleans up a INotify instance; this closes the File Descriptor by default."""
    ### Stop the generation of events ###
    task_key = f'loop_{self.afd.fd}_monitor'
    self._ctx['tasks'][task_key].cancel()
    try: await self._ctx['tasks'][task_key]
    except asyncio.CancelledError: pass
    except: logger.opt(exception=True).debug("Monitoring Loop raised an unexpected exception")
    
    if close:
      # Inotify will cleanup on close
      os.close(self.afd.fd)
    else:
      # Otherwise we need to manually cleanup if the user doesn't want to close the fd
      for filepath, watchdesc in self._ctx['watchdesc_lookup'].items():
        err = inotify_rm_watch(self.afd.fd, watchdesc)
        if err is not NO_ERROR:
          logger.warning(f"Failed to remove watch descriptor for `{filepath.as_posix()}`: {err['kind']}: {err['message']}")
    
    ### Scrub the Cache
    self._ctx['cache'] = {
      'filepaths': {},
      'states': {},
      'related': {},
      'start': 0,
    }
    self._ctx['watchdesc_lookup'] = {}
    async with self._ctx['listen_log'].lock():
      await self._ctx['listen_log'].clear()
  
  def register(self, path: str, mask: INotifyMask):
    """Registers (or update the mask of) a Path for Monitoring."""

    _path = pathlib.Path(path)
    if not _path.is_absolute(): raise NotImplementedError

    err, watchdesc = inotify_add_watch(self.afd.fd, path, mask)
    if err is not NO_ERROR: raise ValueError(f"Failed to add watch descriptor for `{path}`: {err['kind']}: {err['message']}")
    self._ctx['watchdesc_lookup'][_path] = watchdesc

    # Initialize the Cache Item
    self._ctx['cache']['filepaths'][watchdesc] = _path
    self._ctx['cache']['states'][watchdesc] = {
      'event': FSEvent.UNDEFINED,
      'when': time.monotonic_ns(),
      'cookie': 0,
    }

    # If this was the first watch, Initialize State & start the monitoring loop
    if len(self._ctx['watchdesc_lookup'].keys()) == 1:
      self._ctx['cache']['start'] = time.monotonic_ns()
      task_key = f'loop_{self.afd.fd}_monitor'
      self._ctx['tasks'][task_key] = asyncio.create_task(
        self._loop_monitor()
      )
      self._ctx['inactive'].clear()
  
  async def deregisiter(self, path: str):
    """Deregisters a Path from Monitoring."""

    _path = pathlib.Path(path)
    if not _path.is_absolute(): raise NotImplementedError

    if _path not in self._ctx['watchdesc_lookup']: raise NotImplementedError
    watchdesc = self._ctx['watchdesc_lookup'][_path]

    err = inotify_rm_watch(self.afd.fd, watchdesc)
    if err is not NO_ERROR: raise ValueError(f"Failed to remove watch descriptor for `{path}`: {err['kind']}: {err['message']}")

    # Scrub the Cache
    del self._ctx['watchdesc_lookup'][_path]
    del self._ctx['cache']['filepaths'][watchdesc]
    del self._ctx['cache']['states'][watchdesc]

    # If this was the last watch, stop the monitoring loop & cleanup state
    if len(self._ctx['watchdesc_lookup'].keys()) == 0:
      task_key = f'loop_{self.afd.fd}_monitor'
      self._ctx['tasks'][task_key].cancel()
      async with self._ctx['listen_log'].lock():
        await self._ctx['listen_log'].clear()
      self._ctx['inactive'].set()
  
  async def listen(self) -> AsyncGenerator[tuple[pathlib.Path, FSEvent], None]:
    """Listens for INotify Events, yielding them as they occur.
    
    Expects the Monitor to be in an active state. If not it will immediately return.

    Assumes there is only a single listener; if there are multiple listeners, they will race to consume events.
    """
    if self._ctx['inactive'].is_set(): return # Short Circuit if we're not monitoring anything
    wait_for_inactive = asyncio.create_task(self._ctx['inactive'].wait())
    try:
      while True:
        # TODO: Either Pop & yeild or wait for inactivity & return
        done, pending = await asyncio.wait((
          asyncio.create_task(self._ctx['listen_log'].pop()),
          wait_for_inactive
        ), return_when=asyncio.FIRST_COMPLETED)
        if self._ctx['inactive'].is_set():
          for task in pending: task.cancel()
          return
        watchdesc = done[0].result()
        yield (
          self._ctx['cache']['filepaths'][watchdesc],
          self._ctx['cache']['states'][watchdesc]['event']
        )
    finally: wait_for_inactive.cancel()
  
  def snapshot(self) -> MonitorState:
    """Returns a Snapshot of the current Monitor Cache."""
    return deepcopy(self._ctx['cache']) # TODO Is there a better way to do this?
