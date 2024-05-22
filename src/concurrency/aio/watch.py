from __future__ import annotations
from dataclasses import dataclass, field, KW_ONLY
import select, enum, time, os
import asyncio
from . import (
  aio_backend, AIOBackend,
  IOErrorReason, IOEvent,
  BackoffCurve, common_backoff_curves,
  ItemLog,
)
from .. import _log_trapper
from loguru import logger

class WatchBackend(enum.Enum):
  """Which Backend is being used for watching."""
  POLL = "poll"
  EPOLL = "epoll"

  def factory(self) -> select.poll | select.epoll:
    """Get the Factory for the Watch Backend."""
    _poll = None
    if self == WatchBackend.EPOLL: _poll = select.epoll()
    elif self == WatchBackend.POLL: _poll = select.poll()
    else: raise NotImplementedError(self)
    return _poll

_backend_priority = [WatchBackend.EPOLL, WatchBackend.POLL]
"""The Priority of Watch Backends to use in descending order."""
HIGHEST_PRIORITY_WATCH_BACKEND = _backend_priority[0]
"""The Highest Priority Watch Backend to use."""

_epoll_event_map: dict[IOEvent, int] = {
  IOEvent.READ: select.EPOLLIN | select.EPOLLPRI,
  IOEvent.WRITE: select.EPOLLOUT,
  IOEvent.ERROR: select.EPOLLERR,
  IOEvent.CLOSE: select.EPOLLHUP | select.EPOLLRDHUP,
}
_poll_event_map: dict[IOEvent, int] = {
  IOEvent.READ: select.POLLIN | select.POLLPRI,
  IOEvent.WRITE: select.POLLOUT,
  IOEvent.ERROR: select.POLLERR,
  IOEvent.CLOSE: select.POLLHUP,
}

def ioevent_to_mask(io_event: IOEvent, watch_backend: WatchBackend) -> int:
  """Converts an IOEvent to a Mask for the specified Watch Backend."""
  mask = 0
  if watch_backend == WatchBackend.EPOLL: _map = _epoll_event_map
  elif watch_backend == WatchBackend.POLL: _map = _poll_event_map
  else: raise NotImplementedError(watch_backend)
  for m in io_event: mask |= _map[m]
  return mask

@dataclass
class _IOWatcherCtx:
  event_logs: dict[int, ItemLog[IOEvent]] = field(default_factory=dict)
  """The Log of IO Events for each File Descriptor"""
  watch_task: asyncio.Task | None = field(default=None)
  """The Task that is watching for Events on File Descriptors"""

_now = time.monotonic_ns

@dataclass
class IOWatcher:
  """An Asynchronous Interface for notifying on File Descriptor IO Events.
  
  Favors epoll but will fallback to poll if necessary
  """

  fds: dict[int, tuple[WatchBackend, IOEvent]] = field(default_factory=dict)
  """The File Descriptors being watched & the IOEvents they are being watched for."""
  epoll: select.epoll = field(default_factory=WatchBackend.EPOLL.factory)
  """The Epoll Instance for monitoring File Descriptors."""
  poll: select.poll = field(default_factory=WatchBackend.POLL.factory)
  """The (fallback) Poll Instance for monitoring File Descriptors."""
  backoff: BackoffCurve = field(default_factory=common_backoff_curves)
  _ctx: _IOWatcherCtx = field(default_factory=_IOWatcherCtx)

  @property
  def running(self) -> bool:
    """Is the IO Watcher running?"""
    return self._ctx.watch_task is not None
  
  def __contains__(self, fd: int) -> bool:
    """Check if a File Descriptor is being watched."""
    return fd in self.fds

  def __post_init__(self):
    if aio_backend != AIOBackend.LINUX: raise RuntimeError("IO Watching is only supported on Linux")
    if len(self.fds) > 0: raise NotImplementedError
    # for fd, events in self.fds: self.epoll.register(fd, events)
  
  def _fd_smart_register(self, fd: int, eventmask: IOEvent, watch_backend: WatchBackend) -> WatchBackend:
    """Registers a FileDescriptor with some NonBlocking Polling Interface or Async IO Interface
    
    This method will determine the best interface available & use it, starting w/ the watch_backend specified.
    """
    try:
      start_idx = _backend_priority.index(watch_backend)
    except ValueError:
      raise NotImplementedError(watch_backend)
    for idx in range(start_idx, len(_backend_priority)):
      try:
        if _backend_priority[idx] == WatchBackend.EPOLL: self.epoll.register(fd, ioevent_to_mask(eventmask, WatchBackend.EPOLL) | select.EPOLLET)
        elif _backend_priority[idx] == WatchBackend.POLL: self.poll.register(fd, ioevent_to_mask(eventmask, WatchBackend.POLL))
        else: raise NotImplementedError(_backend_priority[idx])
        return _backend_priority[idx]
      except PermissionError: continue
    raise ValueError("No available backend could be registered with")
  
  def get_iolog(self, fd: int) -> ItemLog[IOEvent]:
    """Get the Log of IO Events for a File Descriptor."""
    if fd not in self.fds: raise ValueError("fd is not registered")
    return self._ctx.event_logs[fd]

  def register(
    self,
    fd: int,
    eventmask: IOEvent,
    watch_backend: WatchBackend = HIGHEST_PRIORITY_WATCH_BACKEND,
  ) -> ItemLog[IOEvent]:
    """Register a File Descriptor with the Epoll; returns the Item Log of IO Events for the File Descriptor.
    Args:
      fd: int - The File Descriptor to watch
      eventmask: IOEvent - The IO Events to watch for
      watch_backend: WatchBackend - Which Backend to use for watching; if it fails then the next best backend will be tried until one suceeeds or all fail; Defaults to the highest priority backend
    """
    
    if os.get_blocking(fd): raise ValueError("File Descriptor must be non-blocking")

    if fd in self.fds:
      _backend, _mask = self.fds[fd]
      if _backend != watch_backend: logger.warning(f"fd {fd} has already been registered with {watch_backend} instead of {_backend}")
      if _mask != eventmask:
        if _backend == WatchBackend.EPOLL: self.epoll.modify(fd, ioevent_to_mask(eventmask, WatchBackend.EPOLL) | select.EPOLLET)
        elif _backend == WatchBackend.POLL: self.poll.modify(fd, ioevent_to_mask(eventmask, WatchBackend.POLL))
        else: raise NotImplementedError(_backend)
      self.fds[fd] = (watch_backend, eventmask)
    else:
      selected_backend = self._fd_smart_register(fd, eventmask, watch_backend)
      if selected_backend != watch_backend: logger.warning(f"fd {fd} was registered with {selected_backend} instead of {watch_backend}")
      self.fds[fd] = (selected_backend, eventmask)
      self._ctx.event_logs[fd] = ItemLog()

    logger.trace(f"fd {fd} was registered with {watch_backend.name}")
    return self._ctx.event_logs[fd]
  
  def unregister(self, fd: int) -> ItemLog[IOEvent]:
    """Unregister a File Descriptor from the Epoll; returns the IO Event Log containing any remaining Events for the File Descriptor."""
    if fd not in self.fds: raise ValueError("fd is not registered")
    logger.trace(f"Unregistering File Descriptor {fd}")
    try:
      if self.fds[fd][0] == WatchBackend.EPOLL: self.epoll.unregister(fd)
      elif self.fds[fd][0] == WatchBackend.POLL: self.poll.unregister(fd)
      else: raise NotImplementedError(self.fds[fd][0])
    except OSError as e:
      if e.errno == 9: logger.warning(f"Can't unregister fd {fd} from the {self.fds[fd][0].name} instance b/c it was already closed")
      else: raise
    del self.fds[fd]
    return self._ctx.event_logs.pop(fd)

  # @_log_trapper
  async def watch(self) -> None:
    """Watch Loop for Monitoring Events on File Descriptors."""
    # @_log_trapper
    async def _set_events(iolog: ItemLog[IOEvent], eventmask: int, backend: WatchBackend) -> int:
      # logger.trace(f"Setting Events {eventmask=}, {backend.name=} on the IO Log")
      _len = len(iolog)
      if backend == WatchBackend.EPOLL: # EPoll is edge triggered (notify on change) so we want to push all events
        # if eventmask & _epoll_event_map[IOEvent.READ]: await iolog.push(IOEvent.READ)
        # if eventmask & _epoll_event_map[IOEvent.WRITE]: await iolog.push(IOEvent.WRITE)
        # if eventmask & _epoll_event_map[IOEvent.ERROR]: await iolog.push(IOEvent.ERROR)
        # if eventmask & _epoll_event_map[IOEvent.CLOSE]: await iolog.push(IOEvent.CLOSE)
        if eventmask & _epoll_event_map[IOEvent.READ]:  logger.trace(f"{backend.name}::READ"); await iolog.push(IOEvent.READ)
        if eventmask & _epoll_event_map[IOEvent.WRITE]: logger.trace(f"{backend.name}::WRITE"); await iolog.push(IOEvent.WRITE)
        if eventmask & _epoll_event_map[IOEvent.ERROR]: logger.trace(f"{backend.name}::ERROR"); await iolog.push(IOEvent.ERROR)
        if eventmask & _epoll_event_map[IOEvent.CLOSE]: logger.trace(f"{backend.name}::CLOSE"); await iolog.push(IOEvent.CLOSE)
      elif backend == WatchBackend.POLL: # Poll isn't edge triggered so we only want to push any events once.
        # if eventmask & _poll_event_map[IOEvent.READ] and IOEvent.READ not in iolog: await iolog.push(IOEvent.READ)
        # if eventmask & _poll_event_map[IOEvent.WRITE] and IOEvent.WRITE not in iolog: await iolog.push(IOEvent.WRITE)
        # if eventmask & _poll_event_map[IOEvent.ERROR] and IOEvent.ERROR not in iolog: await iolog.push(IOEvent.ERROR)
        # if eventmask & _poll_event_map[IOEvent.CLOSE] and IOEvent.CLOSE not in iolog: await iolog.push(IOEvent.CLOSE)
        if eventmask & _poll_event_map[IOEvent.READ] and IOEvent.READ not in iolog:  logger.trace(f"{backend.name}::READ"); await iolog.push(IOEvent.READ)
        if eventmask & _poll_event_map[IOEvent.WRITE] and IOEvent.WRITE not in iolog: logger.trace(f"{backend.name}::WRITE"); await iolog.push(IOEvent.WRITE)
        if eventmask & _poll_event_map[IOEvent.ERROR] and IOEvent.ERROR not in iolog: logger.trace(f"{backend.name}::ERROR"); await iolog.push(IOEvent.ERROR)
        if eventmask & _poll_event_map[IOEvent.CLOSE] and IOEvent.CLOSE not in iolog: logger.trace(f"{backend.name}::CLOSE"); await iolog.push(IOEvent.CLOSE)
      else: raise NotImplementedError(backend)
      # logger.trace(f"Set {len(iolog) - _len} Events on the IO Log")
      # assert backend == WatchBackend.EPOLL and len(iolog) > _len or backend == WatchBackend.POLL and len(iolog) >= _len
      return len(iolog) - _len
    
    # datum: int = time.monotonic_ns()
    count: int = 0
    try:
      while True:
        # logger.trace("Polling for IO Events")
        if count > 0: # If we polled more than once w/out any new events then backoff
          # if count == 0: datum = time.monotonic_ns()
          _duration = self.backoff.remaining_wait_time(_now(), count)
          # logger.trace(f"Waiting for {_duration}ns")
          await asyncio.sleep(_duration / 1E9)
          # datum = time.monotonic_ns()
        
        try:
          # assert isinstance(self.epoll, select.epoll)
          _epoll_events = self.epoll.poll(timeout=0)
          # logger.trace(f"Received {len(_epoll_events)} EPoll Events")
          # assert isinstance(self.poll, select.poll)
          _poll_events = self.poll.poll(0)
          # _poll_events = []
          # logger.trace(f"Received {len(_poll_events)} Poll Events")
        except:
          logger.opt(exception=True).trace("Error while polling for IO Events")
          raise

        epoll_sum = sum(await asyncio.gather(*(
          _set_events(
            self._ctx.event_logs[fd],
            events,
            self.fds[fd][0]
          ) for fd, events in _epoll_events if fd in self.fds
        ))) if len(_epoll_events) > 0 else 0
        # logger.trace(f"A Total of {epoll_sum} EPoll Events were recorded")
        poll_sum = sum(await asyncio.gather(*(
          _set_events(
            self._ctx.event_logs[fd],
            events,
            self.fds[fd][0]
          ) for fd, events in _poll_events if fd in self.fds
        ))) if len(_poll_events) > 0 else 0
        # logger.trace(f"A Total of {poll_sum} Poll Events were recorded")
        if epoll_sum + poll_sum > 0: count = 0
        else: count += 1

        # fd_events: list[tuple[int, int]] = [*_epoll_events, *_poll_events]
        # # logger.trace(f"{fd_events=}")
        # # fd_events: list[tuple[int, int]] = [*self.epoll.poll(timeout=0), *self.poll.poll(timeout=0)]
        # # If no events were returned, then wait some before polling again
        # if len(fd_events) < 1:
        #   # logger.trace("No IO Events")
        #   if count == 0: datum = time.monotonic_ns()
        #   _duration = self.backoff.remaining_wait_time(datum, count)
        #   # logger.trace(f"Waiting for {_duration}ns")
        #   await asyncio.sleep(_duration / 1E9)
        #   datum = time.monotonic_ns()
        #   count += 1
        #   continue
        # # logger.trace(f"Received {len(fd_events)} IO Events")
        # count = 0
        # added_counts = await asyncio.gather(*(_set_events(self._ctx.event_logs[fd], events, self.fds[fd][0]) for fd, events in fd_events if fd in self.fds))
        # # epoll_sum = sum(filter(lambda fd: self.fds[fd][0] == WatchBackend.EPOLL, zip(added_counts, (fd for fd, _ in fd_events))))
        # poll_sum = sum(filter(lambda fd: self.fds[fd][0] == WatchBackend.POLL, zip(added_counts, (fd for fd, _ in fd_events))))
        # if poll_sum < 1:
          
        # await asyncio.sleep(0) # Yield to the Event Loop
    except asyncio.CancelledError:
      raise
    except:
      logger.opt(exception=True).error("Unhandled Error in IO Watch Loop")
      raise
  
  async def start(self) -> None:
    """Start watching for Events on File Descriptors."""
    if self._ctx.watch_task is not None: raise RuntimeError("Epoll is already started")
    self._ctx.watch_task = asyncio.create_task(self.watch())

  async def stop(self) -> None:
    """Stop watching for Events on File Descriptors."""
    if self._ctx.watch_task is None: raise RuntimeError("Epoll is not started")
    try:
      if self._ctx.watch_task.cancel(): await self._ctx.watch_task
    except asyncio.CancelledError:
      logger.trace("Watch Task Cancelled")
    logger.trace("Epoll Stopped")
    self._ctx.watch_task = None
  
  async def __aenter__(self) -> IOEvent:
    await self.start()
    return self
  
  async def __aexit__(self, exc_type, exc_value, traceback):
    if self._ctx.watch_task is not None: await self.stop()

io_watcher = IOWatcher()
"""The shared IO Watcher instance."""
