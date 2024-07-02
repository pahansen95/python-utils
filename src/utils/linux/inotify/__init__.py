from __future__ import annotations
import pathlib, weakref
from loguru import logger
from utils.extern import active_backend, SystemBackend
from utils.extern.c import calculate_module_fingerprint, BuildSpec, c_registry, CType, CData, c_buffer

if active_backend != SystemBackend.LINUX: raise RuntimeError("INotify is only supported on Linux")

_CDEF = """\
struct inotify_event {
  int      wd;
  uint32_t mask;
  uint32_t cookie;
  uint32_t len;
  char     name[];
};
static const int INOTEV_NAME_SIZE;
static const int INOTEV_READ_SIZE;
typedef enum {
  INE_NONE,
  INE_BLOCK,
  INE_UNDEFINED,
  INE_BAD_ARGS, // A Bad Value was passed
  INE_NO_MEM, // Out of Memory
  INE_LIMIT, // Some File or Watch Limit was reached
  INE_NO_READ, // The File Descriptor does not have read permissions
  INE_ALREADY_EXIST, // The Watch for the Path already exists
  INE_BAD_PATH, // The Path is invalid for some reason
  ... // Let the compiler determine the values
} c_INotifyError;
typedef enum {
  INC_ACCESS,
  INC_MODIFY,
  INC_ATTRIB,
  INC_CLOSE_WRITE,
  INC_CLOSE_NOWRITE,
  INC_OPEN,
  INC_MOVED_FROM,
  INC_MOVED_TO,
  INC_CREATE,
  INC_DELETE,
  INC_DELETE_SELF,
  INC_MOVE_SELF,
  INC_UNMOUNT,
  INC_Q_OVERFLOW,
  INC_IGNORED,
  INC_CLOSE,
  INC_MOVE,
  INC_ONLYDIR,
  INC_DONT_FOLLOW,
  INC_EXCL_UNLINK,
  INC_MASK_CREATE,
  INC_MASK_ADD,
  INC_ISDIR,
  INC_ONESHOT,
  INC_ALL_EVENTS,
  INC_NONBLOCK,
  INC_CLOEXEC,
  ... // Let the compiler determine the values
} c_INotifyConst;
void c_inotify_init(const int flags, int result[2]);
void c_inotify_add_watch(const int fd, const char *path, const int mask, int result[2]);
void c_inotify_rm_watch(const int fd, const int watchdesc, int result[1]);
void c_read_event(const int fd, struct inotify_event *event, int result[1]);
"""
LIB_BUILD_SPEC: BuildSpec = {
  'cdef': _CDEF,
  'include': ['linuxinotify.h'],
  'sources': ['src/linuxinotify.c'],
}
_lib_path = pathlib.Path(__file__).parent
if 'linux_inotify' not in c_registry.modules:
  c_registry.add("linux_inotify", LIB_BUILD_SPEC)
  _lib_fingerprint = calculate_module_fingerprint(_lib_path, LIB_BUILD_SPEC)
  build_status = c_registry.build("linux_inotify", _lib_path, _lib_fingerprint)
  assert build_status['fingerprint'] == _lib_fingerprint
c_lib, linux_inotify = c_registry.get('linux_inotify')

### Final Imports ###
import enum, pathlib, time, asyncio, os, collections
from loguru import logger
from copy import deepcopy
from dataclasses import dataclass, field, KW_ONLY
from collections.abc import Callable, Coroutine
from typing import AsyncGenerator, TypedDict, NotRequired
from contextlib import asynccontextmanager
from utils.concurrency.aio.fd import AsyncFileDescriptor, IOCondition, IOEvent
from utils.concurrency.aio.watch import io_watcher, IOWatcher
from utils.concurrency.log import ItemLog
from utils.errors import Error, NO_ERROR, NO_ERROR_T

c_INOTEV_NAME_SIZE: int = linux_inotify.INOTEV_NAME_SIZE
c_INOTEV_READ_SIZE: int = linux_inotify.INOTEV_READ_SIZE

def _create_event(state: bool) -> asyncio.Event:
  event = asyncio.Event()
  if state: event.set()
  return event

class INotifyError(enum.Enum):
  """INotify Error Codes"""
  NONE = linux_inotify.INE_NONE
  BLOCK = linux_inotify.INE_BLOCK
  UNDEFINED = linux_inotify.INE_UNDEFINED
  BAD_ARGS = linux_inotify.INE_BAD_ARGS
  NO_MEM = linux_inotify.INE_NO_MEM
  LIMIT = linux_inotify.INE_LIMIT
  NO_READ = linux_inotify.INE_NO_READ
  ALREADY_EXIST = linux_inotify.INE_ALREADY_EXIST
  BAD_PATH = linux_inotify.INE_BAD_PATH

class INotifyMask(enum.IntFlag):
  """A Mask of INotify Events."""
  NONE = 0
  ### inotify_add_watch(2) Flags ###
  ACCESS = linux_inotify.INC_ACCESS
  ATTRIB = linux_inotify.INC_ATTRIB
  CLOSE_WRITE = linux_inotify.INC_CLOSE_WRITE
  CLOSE_NOWRITE = linux_inotify.INC_CLOSE_NOWRITE
  CREATE = linux_inotify.INC_CREATE
  DELETE = linux_inotify.INC_DELETE
  DELETE_SELF = linux_inotify.INC_DELETE_SELF
  MODIFY = linux_inotify.INC_MODIFY
  MOVE_SELF = linux_inotify.INC_MOVE_SELF
  MOVED_FROM = linux_inotify.INC_MOVED_FROM
  MOVED_TO = linux_inotify.INC_MOVED_TO
  OPEN = linux_inotify.INC_OPEN
  MOVE = linux_inotify.INC_MOVE
  CLOSE = linux_inotify.INC_CLOSE
  ALL_EVENTS = linux_inotify.INC_ALL_EVENTS
  ### extra inotify_add_watch(2) Flags ###
  DONT_FOLLOW = linux_inotify.INC_DONT_FOLLOW
  EXCL_UNLINK = linux_inotify.INC_EXCL_UNLINK
  MASK_ADD = linux_inotify.INC_MASK_ADD
  ONESHOT = linux_inotify.INC_ONESHOT
  ONLYDIR = linux_inotify.INC_ONLYDIR
  MASK_CREATE = linux_inotify.INC_MASK_CREATE
  ### extra Flags set in the `mask` field returned by read(2) ###
  IGNORED = linux_inotify.INC_IGNORED
  ISDIR = linux_inotify.INC_ISDIR
  Q_OVERFLOW = linux_inotify.INC_Q_OVERFLOW
  UNMOUNT = linux_inotify.INC_UNMOUNT
  ### inotify_init1(2) Flags ###
  NONBLOCK = linux_inotify.INC_NONBLOCK
  CLOEXEC = linux_inotify.INC_CLOEXEC

_inotify_fds: set[int] = set()
def _safe_cleanup_inotify_fd(fd):
  try: os.close(fd)
  except OSError as e:
    if e.errno == 9: pass # Bad File Descriptor; it's already closed
    else:
      logger.debug(f"Encountered unhandled OSError while cleaning up INotify Handle `{fd}`: ({e.errno}) {e.strerror}")
      raise
def _cleanup_inotify_fds(fds: set[int]):
  for fd in fds:
    logger.debug(f"INotify Instance `{fd}` was leaked; cleaning up")
    try: _safe_cleanup_inotify_fd(fd)
    except: logger.opt(exception=True).warning(f"Unhandled Error encountered cleaning up Inotify Handle fd: {fd}")
__cleanup_inotify = type('_inotify_finalizer', (object,), {})()
weakref.finalize(__cleanup_inotify, _cleanup_inotify_fds, _inotify_fds)

def inotify_init(flags: INotifyMask) -> tuple[Error | NO_ERROR_T, int | None]:
  """Initializes a new inotify instance"""
  result = c_lib.new("int[2]")
  linux_inotify.c_inotify_init(flags, result)
  c_err, fd = (result[0], result[1])
  del result
  if c_err: return { 'kind': ..., 'message': ... }, None
  _inotify_fds.add(fd)
  return NO_ERROR, fd

def inotify_add_watch(fd: int, path: str, mask: INotifyMask) -> tuple[Error | NO_ERROR_T, int | None]:
  """Add or update a filepath watch to an inotify instance"""
  result = c_lib.new("int[2]")
  _path = c_lib.new("char[]", path.encode())
  linux_inotify.c_inotify_add_watch(fd, _path, mask, result)
  del _path
  c_err, watchdesc = (result[0], result[1])
  del result
  if c_err: return { 'kind': ..., 'message': ... }, None
  return NO_ERROR, watchdesc

def inotify_rm_watch(fd: int, watchdesc: int) -> Error | NO_ERROR_T:
  """Remove a filepath watch from an inotify instance"""
  result = c_lib.new("int[1]")
  linux_inotify.c_inotify_rm_watch(fd, watchdesc, result)
  c_err = result[0]
  del result
  if c_err: return { 'kind': ..., 'message': ... }
  return NO_ERROR

def inotify_cleanup(fd: int) -> tuple[Error | NO_ERROR_T]:
  """Cleanup an Inotify Instance"""
  if fd not in _inotify_fds: raise ValueError(f"fd {fd} doesn't seem to be a INotify Handle")
  try: _safe_cleanup_inotify_fd(fd)
  except: return { 'kind': ..., 'message': ... }
  _inotify_fds.remove(fd)
  return NO_ERROR

class INotifyEvent(TypedDict):
  """Wraps the `inotify_event` struct providing a Pythonic interface"""
  wd: int
  mask: INotifyMask
  cookie: int
  name: NotRequired[str]

  @staticmethod
  def read(fd: int) -> tuple[Error | NO_ERROR_T, INotifyEvent | None]:
    result = c_lib.new("int[1]")
    event_ptr = c_lib.new("struct inotify_event *", {"name": linux_inotify.INOTEV_NAME_SIZE})
    # logger.debug(f"INotify `{fd}`: Read Event")
    linux_inotify.c_read_event(fd, event_ptr, result)
    c_err = result[0]
    del result
    err, inotif_event = (NO_ERROR, None)
    # logger.trace(f"INotify `{fd}`: Parsing Results")
    if c_err:
      _err = INotifyError(c_err)
      if _err == INotifyError.BLOCK: err = { 'kind': 'block', 'message': 'Blocking Read' }
      else: err = { 'kind': 'unhandled', 'message': _err.name }
      # logger.trace(f"INotify `{fd}`: Error: {_err}")
    else:
      inotif_event = INotifyEvent.from_struct(event_ptr)
      # logger.trace(f"INotify `{fd}`: INotifyEvent: {inotif_event}")
    del event_ptr
    return err, inotif_event
  
  @staticmethod
  def from_struct(ptr: CType) -> INotifyEvent:
    """Build a INotifyEvent from a `inotify_event` struct CType (or pointer)"""
    assert c_lib.typeof(ptr) in (c_lib.typeof('struct inotify_event'), c_lib.typeof('struct inotify_event *')), f'Unexpected CType: {c_lib.typeof(ptr)}'
    inotif_event: INotifyEvent = {
      'wd': ptr.wd,
      'mask': INotifyMask(ptr.mask),
      'cookie': ptr.cookie,
    }
    if ptr.len > 0: inotif_event['name'] = c_lib.string(ptr.name, ptr.len).rstrip(b'\x00').decode()
    return inotif_event

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

ALL_CRUD_FSEVENTS = FSEvent.CREATE | FSEvent.READ | FSEvent.WRITE | FSEvent.DELETE
ALL_LIFECYCLE_FSEVENTS = FSEvent.OPEN | FSEvent.CLOSE | FSEvent.MOVE | FSEvent.ATTRIB
ALL_FSEVENTS = ALL_CRUD_FSEVENTS | ALL_LIFECYCLE_FSEVENTS

def convert_fsevent_to_inotif(fsevent: FSEvent) -> INotifyMask:
  m = INotifyMask.NONE

  ### CRUD ###
  if FSEvent.CREATE & fsevent: m |= INotifyMask.CREATE | INotifyMask.MOVED_FROM
  if FSEvent.READ & fsevent: m |= INotifyMask.ACCESS
  if FSEvent.WRITE & fsevent: m |= INotifyMask.MODIFY
  if FSEvent.DELETE & fsevent: m |= INotifyMask.DELETE | INotifyMask.DELETE_SELF | INotifyMask.MOVE_SELF | INotifyMask.MOVED_TO

  ### Lifecycle ###
  if FSEvent.OPEN & fsevent: m |= INotifyMask.OPEN
  if FSEvent.CLOSE & fsevent: m |= INotifyMask.CLOSE
  if FSEvent.MOVE & fsevent: m |= INotifyMask.MOVE | INotifyMask.MOVE_SELF
  if FSEvent.ATTRIB & fsevent: m |= INotifyMask.ATTRIB

  if m == INotifyMask.NONE: raise ValueError(f'INotifyMask could not be determined for {fsevent}')

  return m

def convert_inotif_to_fsevent(inotif: INotifyMask) -> FSEvent:
  e = FSEvent.UNDEFINED
  ### Crud ###
  if (INotifyMask.CREATE | INotifyMask.MOVED_FROM) & inotif: e |= FSEvent.CREATE
  if INotifyMask.ACCESS & inotif: e |= FSEvent.READ
  if INotifyMask.MODIFY & inotif: e |= FSEvent.WRITE
  if (INotifyMask.DELETE | INotifyMask.DELETE_SELF | INotifyMask.MOVE_SELF | INotifyMask.MOVED_TO) & inotif: e |= FSEvent.DELETE
  ### Lifecycle ###
  if INotifyMask.OPEN & inotif: e |= FSEvent.OPEN
  if INotifyMask.CLOSE & inotif: e |= FSEvent.CLOSE
  if (INotifyMask.MOVE | INotifyMask.MOVE_SELF) & inotif: e |= FSEvent.MOVE
  if INotifyMask.ATTRIB & inotif: e |= FSEvent.ATTRIB
  
  if e == FSEvent.UNDEFINED: raise ValueError(f'FSEvent is undefined for {inotif}')

  return e

class WatchEvent(TypedDict):
  """Encapsulates a Filesystem Notifcation for a Watched Path"""
  ...

class WatchSpec(TypedDict):
  """Encapsulates the Watch Configuration for a filesystem path"""
  path: pathlib.Path
  mask: FSEvent
  depth: NotRequired[int | None]

class WatchGraphCtx(TypedDict):
  backend: dict[str, INotifyBackend]
  inactive: asyncio.Event
  watch: WatchState

  class WatchState(TypedDict):
    lookup: dict[int, pathlib.Path]
    nodes: dict[pathlib.Path, WatchNode]

  class INotifyBackend(TypedDict):
    fd: int
    to_mask: Callable[..., ...]
    from_mask: Callable[..., ...]
    add: Callable[..., ...]
    remove: Callable[..., ...]
    read: Callable[..., ...]
    cleanup: Callable[..., ...]

class _FileNode(TypedDict):
  path: pathlib.Path
  real: pathlib.Path
  depth: int
  is_dir: bool

class WatchNode(_FileNode):
  mask: FSEvent
  wd: int

@dataclass
class WatchGraph:
  """A Graph of the Filesystem Paths being watched"""
  nodes: dict[str, MonitorNode]
  """The current set of Watched Items"""
  edges: dict[str, set[str]]
  """The current set of Watch Relationships"""
  log: ItemLog[...] = field(default_factory=ItemLog)
  _: KW_ONLY
  _ctx: WatchGraphCtx = field(default_factory=WatchGraphCtx.factory)

  class MonitorNode(TypedDict):
    """A watched item"""
    path: str
    events: FSEvent

  def __post_init__(self):
    if active_backend == SystemBackend.LINUX:
      if 'backend' not in self._ctx:
        self._ctx['backend'] = {
          'fd': inotify_init(INotifyMask.NONBLOCK),
          'to_mask': convert_fsevent_to_inotif,
          'from_mask': convert_inotif_to_fsevent,
          'add': inotify_add_watch,
          'remove': inotify_rm_watch,
          'read': INotifyEvent.read,
          'cleanup': inotify_cleanup,
        }
    else: raise RuntimeError(f'Unsupported Backend for WatchGraph: {active_backend.name}')

  def _get_watched_parents(self, path: pathlib.Path) -> list[pathlib.Path]:
    """Determines the parent directories being watched for a given path. If no parents are being watched then returns an empty list."""
    if not path.resolve().is_dir(): return None
    """NOTE: How do we determine if this is a child directory

    - We need to determine if any of the child's parents have an associated watch with them
    - Each parent can optionally have a limiting depth

    Therefore, traverse up the parents, checking for available watches that match the depth.
    If an inclusive watch is found then return that parent.
    If no inclusive watch is found then return None.
    
    """
    ### NOTE: The Child Depth is the parent's relative position in the child's list of parents
    # Given /p4/p3/p2/p1/c
    # Then for Parent `p3`, the child has a depth of 3.
    # ie. child_depth = `N` (of parent `pN`) where `N` is the enumerated position
    ###
    watched_parents: list[pathlib.Path] = []
    for child_depth, parent in enumerate(path.parents, start=1):
      if parent not in self._ctx['watch']['nodes']: continue
      parent_depth = self._ctx['watch']['nodes'][parent]['depth']
      if parent_depth is None or child_depth <= parent_depth: watched_parents.append(parent) # Filter out Parent's whose depth doesn't (set) include the child
    return watched_parents

  def _watch_file(self, mask: FSEvent, path: pathlib.Path):
    """Setup a watch for a file"""
    real_path = path.resolve()
    assert real_path.is_file()

    node: _FileNode = {
      'path': path,
      'real': real_path,
      'depth': 0,
      'is_dir': real_path.is_dir(),
    }

    err, wd = self._ctx['backend']['add'](
      fd=self._ctx['backend']['fd'],
      path=node['path'].as_posix(),
      mask=self._ctx['backend']['to_mask'](mask=mask),
    )
    ### TODO: Better handle errors
    if err is not NO_ERROR: raise ValueError(f"{err['kind']}: {err['message']}")
    ###
    self._ctx['watch']['lookup'][wd] = path
    self._ctx['watch']['nodes'][node['path']] = node | {
      'mask': 0 | mask,
      'wd': wd,
    }

    ### TODO: Update the Graph

  def _watch_dir_tree(self, mask: FSEvent, path: pathlib.Path, depth: int | None):
    """Setup watches for a directory tree"""
    assert path.resolve().is_dir()

    ### NOTE: Break recursive Dir Tree Paths
    real_path = path.resolve()
    ... # TODO
    ###

    ### NOTE: Walk the tree up to an optional depth     
    walk_stack: collections.deque[_FileNode] = collections.deque([ { 'path': path, 'depth': 0, 'real': real_path, 'is_dir': real_path.is_dir() } ])
    walk_seen = set()
    nodes: list[_FileNode] = []

    while len(walk_stack) > 0:
      node = walk_stack.popleft()
      assert node['path'].resolve().is_dir()
      for child in node['path'].iterdir():
        child_real_path = child.resolve()
        if child_real_path in walk_seen: continue
        walk_seen.add(child_real_path)

        child_node: _FileNode = { 'path': child, 'depth': node['depth'] + 1, 'real': child_real_path, 'is_dir': child_real_path.is_dir() }
        nodes.append(child_node)
        if child_node['is_dir'] and (
          depth is None or child_node['depth'] < depth # Don't traverse dirs whose children would be at a depth greater than declared
        ): walk_stack.append(child_node)
    ###

    ### NOTE: Setup watches; TODO Breakout into seperate function?
    _mask = 0 | mask
    _dir_mask = _mask | FSEvent.CREATE
    for node in nodes:
      if node['is_dir'] and (depth is None or node['depth'] < depth): self._ctx['backend']['to_mask'](mask=_dir_mask) # Don't additionally watch for CREATE events in dirs whose children would exceed the depth (has no effect if the user specified CREATE in the mask)
      else: node_mask = self._ctx['backend']['to_mask'](mask=_mask)
      err, wd = self._ctx['backend']['add'](
        fd=self._ctx['backend']['fd'],
        path=node['path'].as_posix(),
        mask=node_mask
      )
      ### TODO: Better handle errors
      if err is not NO_ERROR: raise ValueError(f"{err['kind']}: {err['message']}")
      ###
      self._ctx['watch']['lookup'][wd] = path
      self._ctx['watch']['nodes'][node['path']] = node | {
        'mask': _mask,
        'wd': wd,
      }
    ###

    ### TODO: Update the Graph

  # def _list_children(self, path: pathlib.Path, depth: int | None) -> list[pathlib.Path]:
  #   assert path.resolve().is_dir()
  #   paths: list[pathlib.Path] = [ ]
  #   # We are (recursively) watching a Directory's contents too
  #   _depth = 0
  #   _descend: collections.deque[pathlib.Path] = collections.deque([ path ])
  #   _seen: set[pathlib.Path] = []
  #   while len(_descend) > 0:
  #     node = _descend.popleft()
  #     assert node.resolve().is_dir()
  #     for child in node.iterdir():
  #       if child.resolve() in _seen: continue # Break Recursive Directory Tree Paths
  #       _seen.add(child.resolve())
  #       paths.append(child)
  #       if child.is_dir() and (
  #         depth is None
  #         or _depth + 1 <= depth
  #       ): _descend.append(child)
  #   assert path not in paths
  #   return paths
  
  def _watch_child(self, child: pathlib.Path, parent: pathlib.Path):
    assert child.resolve().is_dir()
    assert parent in self._ctx['watch']['nodes']
    parent_node = self._ctx['watch']['nodes'][parent]
    parent_depth = parent_node['depth']
    assert parent in child.parents
    child_depth = list(child.parents).index(parent) + 1
    if parent_depth is None: _depth = None # No depth limit
    else: _depth = parent_depth - child_depth # Otherwise figure out the depth of the child relative to the parent

    self._watch_dir_tree(
      mask=parent_node['mask'],
      path=child,
      depth=_depth,
    )

  async def _loop_handle_backend_events(self):
    """Handles Filesystem events as they occur"""
    try:
      assert active_backend == SystemBackend.LINUX, 'No other backends implemented'
      while True:
        # TODO: Implement Asynchronous Reads
        err, backend_event = self._ctx['backend']['read'](fd=self._ctx['backend']['fd'])
        if err is not NO_ERROR: raise ValueError(f"{err['kind']}: {err['message']}")
        assert isinstance(backend_event, INotifyEvent)
        _path = backend_event['path']
        assert isinstance(_path, pathlib.Path)

        # Recursively Register Paths; TODO: Which parent do we use? The one w/ the "largest" (set) inclusion or the "smallest"? For now, just use the deepest parent.
        if (_parent_paths := self._get_watched_parents(_path)): self._watch_child(child=_path, parent=_parent_paths[-1]) 

        # Assemble the Watch Event
        event: WatchEvent = {
          'mask': self._ctx['backend']['from_mask'](mask=backend_event['mask']),
          ... # TODO: Add remaining Info here
        }

        # TODO: Filter the Watch Event

        # Update the Graph
        if _path.as_posix() not in self.nodes:
          self.nodes[_path.as_posix()] = {
            ... # TODO: Add Monitor Node
          }
          self.edges[_path.as_posix()].add(...) # TODO: Add Edges
        else:
          self.nodes[_path.as_posix()][...] = ... # TODO: Update Node

        # Publish the Watch Event
        await self.log.push(event)
    finally:
      err = self._ctx['backend']['cleanup'](fd=self._ctx['backend']['fd'])
      if err is not NO_ERROR: logger.debug(f"{err['kind']}: {err['message']}")
      self._ctx['inactive'].set()

  def add_watch(self, spec: WatchSpec):
    """Setup a watch as specified"""
    # Schedule the monitor loop if this is the first path
    if self._ctx['inactive'].is_set():
      self._ctx['tasks'][f'watch{id(self)}_monitor_loop'] = asyncio.create_task(
        self._loop_handle_backend_events()
      )
      self._ctx['inactive'].set()
    
    assert 'fd' in self._ctx['backend']

    if spec['path'] in self._ctx['watch']['nodes']: raise NotImplementedError # TODO: Handle duplicate watches
    if (parent_paths := self._get_watched_parents(spec['path'])): raise NotImplementedError # TODO: Handle Overlapping watches

    if spec['path'].resolve().is_dir(): self._watch_dir_tree(spec['mask'], spec['path'], spec['depth'])
    else: self._watch_file(spec['mask'], spec['path'])
  
  def remove_watch(self, spec: WatchSpec):
    """Remove a previously configured watch"""
    raise NotImplementedError

class WatchFilter(TypedDict):
  path: str
  """The Path to include; supports Globbing"""
  mask: FSEvent
  """The FSEvent Mask to watch for"""

class EventMessage(TypedDict):
  path: pathlib.Path
  """The Path of the file associated with the event"""
  mask: FSEvent
  """The Mask of events that occured"""
  when: int
  """When the Event Occured"""
  cookie: int
  """A Cookie used to associate multiple events together with"""

class MonitorCtx(TypedDict):
  ...

  @staticmethod
  def factory(self, **kwargs) -> MonitorCtx:
    ...

@dataclass
class FSMonitor:
  """Monitor the Filesystem for Specific CRUD & Lifecycle Events"""
  graph: WatchGraph = field(default_factory=WatchGraph)
  _: KW_ONLY
  _ctx: MonitorCtx = field(default_factory=MonitorCtx.factory)

  def include(self, *watch: WatchFilter):
    """Monitor the specified Path(s); globbing supported
    
    `/foo/**/*` watches all children under `/foo` recursively adding/removing them as they are created/destroyed
    `/foo/*` watches all immediate children under `/foo` adding/removing them as they are created/destroyed
    `/foo/*/bar` watches the `bar` file for all immediate children under `/foo` adding/removing them as they are created/destroyed
    `/foo/bar` watches a singular file. If a directory, does not watch any of the children.
    
    """
    # First list all filepaths matching the glob pattern
    # For each filepath, merge into the WatchGraph the files & folders to watch
    ### TODO: For a glob pattern, How do we determine what directories to recursively watch?

    # NOTE: We assume the following files & directories are the minimized/simplified set of what's passed to the function
    files = [ { 'path': '/real/file1'}, { 'path': '/real/file2'} ]
    directories = [ { 'path': '/real/dir/1', 'depth': 1 }, { 'path': '/real/dir/2' } ] # Omit depth to indicate no limit

    specs: list[WatchSpec] = []

    for file in files:
      mask = file['mask']
      if file['path'] in self.graph.nodes:
        # TODO: Get the current Mask & Merge it with the supplied one
        ...
      specs.append({
        'kind': 'file',
        'path': file['path'],
        'mask': mask,
      })
    
    for _dir in directories:
      # TODO: Globbing Patterns
      mask = _dir['mask']
      if _dir['path'] in self.graph.nodes:
        # TODO: Get the current Mask & Merge it with the supplied one
        ...
      specs.append({
        'kind': 'dir',
        'path': _dir['path'],
        'depth': _dir.get('depth', 1),
        'mask': mask
      })
    
    for spec in specs: self.graph.watch_path(spec)
  
  def exclude(self, *watch: WatchFilter):
    """Don't Monitor the specified Path(s); globbing supported"""
    # NOTE: Exclusion is like Inclusion except it Filters out Paths/Masks
    #       It also adds post-processors that drops event messages that couldn't otherwise be filtered out
    ...

  def clear(self):
    """Clears the include/exclude filters effectively stopping all current watches"""
    ...
  
  def is_active(self, *path: str) -> list[bool]:
    """Check if the following paths are currently being monitored; Globbing Supported"""
    ...
  
  def get_state(self, *path: str) -> dict[str, EventMessage]:
    """Get the last known state for the following paths; Globbing Supported"""
    ...
  
  async def listen(self) -> AsyncGenerator[EventMessage, None]:
    """Listens for FSEvents; assumes a 1-to-1 relationship between producer/consumer"""
    while True:
      event = await self.graph.log.peek()
      yield ... # TODO: Transform the event
      await self.graph.log.pop()


# class MonitorState(TypedDict):
#   """A Snapshot of the Monitor Cache"""
#   filepaths: dict[int, pathlib.Path]
#   """A Mapping of Watch Descriptors to their File Paths"""
#   states: dict[int, FileState]
#   """A Mapping of Watch Descriptors to their States"""
#   related: dict[int, set[int]]
#   """Related events such as a rename: { cookie: set(WatchDescriptor, ...) }"""
#   start: int
#   """The absolute start time of the monitoring as a monotonic ns timestamp"""

#   class FileState(TypedDict):
#     """The State of a File"""
#     event: FSEvent
#     """The Event that occured"""
#     when: int
#     """The time the event was recorded as a monotonic ns timestamp"""
#     cookie: NotRequired[int]
#     """A Unique Identifier linking two related actions (such as a move)"""
# 
# class _Ctx(TypedDict):
#   """Stateful Context for the INotify Instance"""
#   cache: NotRequired[MonitorState]
#   """The Current State of the INotify Event Cache"""
#   watchdesc_lookup: dict[pathlib.Path, int]
#   """A Lookup Table for Watch Descriptors"""
#   path_lookup: dict[int, pathlib.Path]
#   """A Lookup Table for an actively watched Path"""
#   inactive_paths: dict[pathlib.Path, asyncio.Event]
#   """Events for synchronizing teardown of a path watch"""
#   listen_log: ItemLog[int]
#   """An internal Log to track Watch Descriptors that had an event occur"""
#   tasks: dict[str, asyncio.Task]
#   """A Mapping of Task Keys to their Tasks"""
#   inactive: asyncio.Event
#   """An Event to signal nothing is actively being monitored"""

#   @staticmethod
#   def factory(**kwargs) -> _Ctx:
#     return {
#       'cache': {
#         'filepaths': {},
#         'states': {},
#         'related': {},
#         'start': 0,
#       },
#       'watchdesc_lookup': {},
#       'inactive_paths': {},
#       'listen_log': ItemLog[int](),
#       'tasks': {},
#       'inactive': _create_event(True),
#     } | kwargs

# @dataclass
# class FSMonitor:
#   """Provides Notifications of Filesystem Events.
  
#   ### TODO

#   - How do we track the State association between the IOWatcher & the ItemLog
#   - Currently, we capture the current state of the watched files; should we
#     retain a log of events? What could we use that for?
  
#   """

#   fd: int
#   """The INotify Instance Handle"""
#   io_log: ItemLog[IOEvent]
#   """The Log of IO Events for INotify Instance Handle"""

#   _: KW_ONLY
#   _ctx: _Ctx = field(default_factory=_Ctx.factory)

#   def __post_init__(self):
#     if os.get_blocking(self.fd): raise ValueError(f"The Specified INotify Instance Handle (fd {self.fd}) must be in NonBlocking Mode")

#   @staticmethod
#   def factory(flags: INotifyMask = INotifyMask.NONBLOCK, io_watcher: IOWatcher = io_watcher, cleanup_log: bool = True, ctx: _Ctx = None) -> FSMonitor:
#     """Factory Function that initializes a new INotify instance.
    
#     """
#     err, fd = inotify_init(flags)
#     if err is not NO_ERROR: raise NotImplementedError
#     io_log = io_watcher.register(fd, IOEvent.READ)
#     kwargs = {
#       'fd': fd,
#       'io_log': io_log,
#       '_ctx': _Ctx.factory(**(ctx or {})),
#     }
#     monitor = FSMonitor(**kwargs)
#     if cleanup_log: weakref.finalize(monitor, io_watcher.unregister, fd) # Cleanup resources on GC
#     return monitor
  
#   @asynccontextmanager
#   @staticmethod
#   async def session(**kwargs) -> AsyncGenerator[FSMonitor, None]:
#     """Context Manager that provides a Session with an INotify Instance. Cleans up resources on exit."""
#     monitor = FSMonitor.factory(**kwargs)
#     try: yield monitor
#     finally: await monitor.cleanup()
  
#   async def _loop_monitor(self):
#     """The Monitoring loop; it purely listens for INotify Events & ensures the cache is updated."""
#     assert not os.get_blocking(self.fd)
#     while True:
#       logger.trace(f'FSMonitor `{self.fd}`: Waiting for an available IO Event')
#       await self.io_log.peek() # Block until we can read
#       when = time.monotonic_ns()
#       logger.trace(f'FSMonitor `{self.fd}`: IO Event available @ {when}')
#       inotify_events: list[INotifyEvent] = []
#       while True:
#         logger.trace(f'FSMonitor `{self.fd}`: Reading INotifyEvent')
#         err, _ine = INotifyEvent.read(self.fd)
#         logger.trace(f'FSMonitor `{self.fd}`: Error: {err}, INotifyEvent: {_ine}')
#         if err is NO_ERROR: inotify_events.append(_ine)
#         else:
#           if err['kind'] == 'block':
#             logger.trace(f'FSMonitor `{self.fd}`: Drained READ IOEvent')
#             io_event = await self.io_log.pop()
#             assert io_event == IOEvent.READ
#             break
#           else: raise NotImplementedError(f"{err['kind']}: {err['message']}")
      
#       logger.trace(f'FSMonitor `{self.fd}`: {inotify_events=}')
      
#       for inotify_event in inotify_events:
#         wd_path = self._ctx['watchdesc_lookup'][inotify_event['wd']]
#         if inotify_event['mask'] & INotifyMask.IGNORED:
#           self._ctx['inactive_paths'][wd_path]
#           continue
#         fsevent = convert_inotif_to_fsevent(inotify_event['mask'])
#         # Update the Cache
#         assert inotify_event['wd'] in self._ctx['cache']['filepaths']
#         self._ctx['cache']['states'][inotify_event['wd']] = {
#           'event': fsevent,
#           'when': when,
#         } | ({ 'cookie': inotify_event['cookie'] } if inotify_event['cookie'] > 0 else {} )
#         if inotify_event['cookie'] > 0:
#           if inotify_event['cookie'] not in self._ctx['cache']['related']:
#             self._ctx['cache']['related'][inotify_event['cookie']] = set()
#           self._ctx['cache']['related'][inotify_event['cookie']].add(
#             inotify_event['wd']
#           )

#         # Let the listener know about the event
#         logger.trace(f'FSMonitor `{self.fd}`: Notifying Event Listener')
#         await self._ctx['listen_log'].push(inotify_event['wd'])

#   async def cleanup(self, close: bool = True):
#     """Cleans up a INotify instance; this closes the File Descriptor by default."""
#     ### Stop the generation of events ###
#     task_key = f'loop_{self.fd}_monitor'
#     if task_key in self._ctx['tasks']:
#       self._ctx['tasks'][task_key].cancel()
#       try: await self._ctx['tasks'][task_key]
#       except asyncio.CancelledError: pass
#       except: logger.opt(exception=True).debug("Monitoring Loop raised an unexpected exception")
    
#     if close: inotify_cleanup(self.fd)
#     else:
#       # Otherwise we need to manually cleanup if the user doesn't want to close the fd
#       for filepath, watchdesc in self._ctx['watchdesc_lookup'].items():
#         err = inotify_rm_watch(self.fd, watchdesc)
#         if err is not NO_ERROR:
#           logger.warning(f"Failed to remove watch descriptor for `{filepath.as_posix()}`: {err['kind']}: {err['message']}")
    
#     ### Scrub the Cache
#     self._ctx['cache'] = {
#       'filepaths': {},
#       'states': {},
#       'related': {},
#       'start': 0,
#     }
#     self._ctx['watchdesc_lookup'] = {}
#     async with self._ctx['listen_log'].lock():
#       await self._ctx['listen_log'].clear()
  
#   def register(self, path: str, mask: FSEvent):
#     """Registers (or update the mask of) a Path for Monitoring."""

#     _path = pathlib.Path(path).expanduser().absolute()
#     err, watchdesc = inotify_add_watch(self.fd, path, convert_fsevent_to_inotif(mask))
#     if err is not NO_ERROR: raise ValueError(f"Failed to add watch descriptor for `{path}`: {err['kind']}: {err['message']}")
#     self._ctx['watchdesc_lookup'][_path] = watchdesc

#     # Initialize the Cache Item
#     self._ctx['cache']['filepaths'][watchdesc] = _path
#     self._ctx['cache']['states'][watchdesc] = {
#       'event': FSEvent.UNDEFINED,
#       'when': time.monotonic_ns(),
#     }
#     self._ctx['inactive_paths'][_path] = _create_event(False)

#     # If this was the first watch, Initialize State & start the monitoring loop
#     # if len(self._ctx['watchdesc_lookup'].keys()) == 1:
#     if self._ctx['inactive'].is_set():
#       logger.debug(f"FSMonitor `{self.fd}` state transition: INACTIVE -> ACTIVE")
#       self._ctx['cache']['start'] = time.monotonic_ns()
#       task_key = f'loop_{self.fd}_monitor'
#       self._ctx['tasks'][task_key] = asyncio.create_task(
#         self._loop_monitor()
#       )
#       self._ctx['inactive'].clear()
  
#   async def deregisiter(self, path: str):
#     """Deregisters a Path from Monitoring."""

#     _path = pathlib.Path(path)
#     if not _path.is_absolute(): raise NotImplementedError

#     if _path not in self._ctx['watchdesc_lookup']: raise NotImplementedError
#     watchdesc = self._ctx['watchdesc_lookup'][_path]

#     err = inotify_rm_watch(self.fd, watchdesc)
#     if err is not NO_ERROR: raise ValueError(f"Failed to remove watch descriptor for `{path}`: {err['kind']}: {err['message']}")
#     await self._ctx['inactive_paths'][_path].wait()

#     # If this was the last watch, stop the monitoring loop & cleanup state
#     if len(self._ctx['watchdesc_lookup'].keys()) <= 0:
#       task_key = f'loop_{self.fd}_monitor'
#       self._ctx['tasks'][task_key].cancel()
#       try: await self._ctx['tasks'][task_key]
#       except asyncio.CancelledError: pass
#       except: logger.opt(exception=True).debug(f"Unexpected error encountered in the `{task_key}` Task")
#       async with self._ctx['listen_log'].lock():
#         await self._ctx['listen_log'].clear()
#       self._ctx['inactive'].set()
  
#     # Scrub the Cache
#     del self._ctx['watchdesc_lookup'][_path]
#     del self._ctx['inactive_paths'][_path]
#     del self._ctx['cache']['filepaths'][watchdesc]
#     del self._ctx['cache']['states'][watchdesc]

#   async def listen(self) -> AsyncGenerator[tuple[pathlib.Path, FSEvent], None]:
#     """Listens for INotify Events, yielding them as they occur.
    
#     Expects the Monitor to be in an active state. If not it will immediately return.

#     Assumes there is only a single listener; if there are multiple listeners, they will race to consume events.
#     """
#     if self._ctx['inactive'].is_set(): return # Short Circuit if we're not monitoring anything
#     wait_for_inactive = asyncio.create_task(self._ctx['inactive'].wait())
#     try:
#       while True:
#         # TODO: Either Pop & yeild or wait for inactivity & return
#         _done, _pending = await asyncio.wait((
#           asyncio.create_task(self._ctx['listen_log'].pop()),
#           wait_for_inactive
#         ), return_when=asyncio.FIRST_COMPLETED)
#         done, pending = tuple(_done), tuple(_pending)
#         if self._ctx['inactive'].is_set():
#           pop_task, wait_task = pending[0], done[0]
#           assert wait_task is wait_for_inactive
#           assert isinstance(pop_task, asyncio.Task)
#           pop_task.cancel()
#           return
#         pop_task, wait_task = done[0], pending[0]
#         assert isinstance(pop_task, asyncio.Task)
#         assert wait_task is wait_for_inactive
#         watchdesc = pop_task.result()
#         assert isinstance(watchdesc, int)
#         yield (
#           self._ctx['cache']['filepaths'][watchdesc],
#           self._ctx['cache']['states'][watchdesc]['event']
#         )
#     finally: wait_for_inactive.cancel()
  
#   def snapshot(self) -> MonitorState:
#     """Returns a Snapshot of the current Monitor Cache."""
#     return deepcopy(self._ctx['cache']) # TODO Is there a better way to do this?
