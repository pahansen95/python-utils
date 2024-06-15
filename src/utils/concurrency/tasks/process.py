"""

# The Process Utility

Spawn & Manage new OS Processes

"""

from __future__ import annotations
from dataclasses import dataclass, field, KW_ONLY
from ..aio.watch import IOWatcher, io_watcher
from ..aio.fd import AsyncFileDescriptor
from ..aio import IOEvent
from .. import ItemLog
from utils.itertools import chain
from loguru import logger
import os
import sys
import fcntl
import tempfile
import gc
from typing import Never, Any
import asyncio
import signal
import threading

from . import Task, TaskState, TaskBackend, ConcurrencyBackend, active_backend
from .mgmt import MgmtMsg, MgmtMsgKind, MgmtMsgPayload
from ..channels.pipe import Pipe

### TODO: Replace these imports w/ concrete implementations
from ..channels import Channel
from ..messages import Message
from ..messages.errors import Error
###

if active_backend not in (ConcurrencyBackend.LINUX,): raise RuntimeError(f"The Current Active Backend is Unsupported: {active_backend}")

@dataclass
class _ProcCtx:
  """The Stateful Context for the Process"""
  uid: int | None
  """Parent & Child: The User ID for the Child Process"""
  gid: int | None
  """Parent & Child: The Group ID for the Child Process"""
  workdir: str | None = None
  """Parent & Child: The Working Directory for the Child Process"""
  io_watcher: IOWatcher = field(default_factory=lambda: io_watcher)
  """Parent Only: The IO Watcher for the Async IO Streams"""
  pid: int | None = None
  """Parent & Child: The Process ID of the spawned Process; 0 if the child"""
  returncode: int | None = None
  """The Return Code of the Process"""
  stdin: tuple[int, int] | None = None
  """Parent & Child: The Read/Write File Descriptors for the child's Stdin"""
  stdout: tuple[int, int] | None = None
  """Parent & Child: The Read/Write File Descriptors for the child's Stdout"""
  stderr: tuple[int, int] | None = None
  """Parent & Child: The Read/Write File Descriptors for for the child's Stderr"""
  mgmt_chan: Pipe[MgmtMsg]
  """The Management Channel for the Task (RW); implementation is backend dependent"""
  data_chan: Pipe[Message]
  """The Data Channel for the Task (RW); implementation is backend dependent"""
  error_chan: Pipe[Error]
  """The Error Sink for the Task (RO); implementation is backend dependent"""
  capture_thread: threading.Thread | None = None
  """Parent Only: The Thread for capturing the Child's Return Code"""
  capture_error: Exception | None = None
  """Parent Only: An Error that occurred while capturing the Child's Return Code"""
  capture_complete: asyncio.Event = field(default_factory=asyncio.Event)
  """Parent Only: An Event to signal that the Child's Return Code has been captured"""

@dataclass(frozen=True)
class Process(Task):
  """A Task spawned as a new OS Process"""

  id: str
  """A unique identifier for the Task"""
  cmd: str
  """The Command to execute"""
  argv: list[str]
  """The Command Arguments"""
  env: dict[str, str]
  """The Environment Variables"""

  _: KW_ONLY

  backend: TaskBackend = TaskBackend.OS_EXEC
  """Spawns an OS Process using the `exec` system calls"""
  _ctx: _ProcCtx = field(default_factory=_ProcCtx)
  """The Stateful Context for the Process"""
  
  @staticmethod
  def factory(self, **kwargs) -> Process:
    """A Factory function for creating a Process with a Custom Context"""
    raise NotImplementedError()

  def _exec(self) -> Never:
    """Exec the Task in the Child Process; this will replace the current process with the new process."""
    if self._ctx.pid != 0: raise RuntimeError(f"_exec can only be executed by the child!")
    """TODO
    - File Descriptors
      - Get a List of every single open file descriptor
      - Close all file descriptors except for the ones we need
        - stdin, stdout, stderr
      - Make sure all remaining file descriptors are blocking
    - ...
    """
    raise NotImplementedError()
  
  def _get_state(self) -> TaskState:
    """Get the Current State of the Task"""
    raise NotImplementedError()

  def _capture_child_exit(self) -> None:
    """Capture the Child's Exit Status; run this in a seperate thread"""
    if self._ctx.pid == 0: raise RuntimeError(f"_capture_child_exit can only be executed by the parent!")

    # TODO: Use `pidfd_open` to manage the Child Process

    try:
      _pid, _status = os.waitpid(self._ctx.pid, 0)
      if _pid != self._ctx.pid: raise RuntimeError(f"Unexpected Child Process ID: {_pid}")
      _sig = _status & 0x7F # Get the High Byte but ignore the High Bit (which indicates a core dump)
      self._ctx.returncode = (_status >> 8) & 0xFF if _sig == 0 else _sig  # Get the High byte of the 16bit status if the signal is 0; otherwise use the signal
    except ChildProcessError:
      self._ctx.returncode = -256 # Set a value outside the range of a 8-bit signed integer
    except Exception as e:
      self._ctx.capture_error = e
    finally:
      self._ctx.capture_complete.set()

  async def _kill_child_process(self, sig: int) -> None:
    """Kill the Child Process"""
    if self._ctx.pid == 0: raise RuntimeError(f"_kill_child_process can only be executed by the parent!")
    try:
      os.kill(self._ctx.pid, sig)
      await self._ctx.capture_complete.wait()
    except ProcessLookupError:
      logger.warning(f"Child Process {self._ctx.pid} is already dead")
    except Exception as e:
      logger.opt(exception=e).error(f"An error occurred while killing the Child Process {self._ctx.pid}")
      raise e

  async def _cleanup_child(self) -> None:
    """Cleanup the Child Process"""
    if self._ctx.pid == 0: raise RuntimeError(f"_cleanup_child can only be executed by the parent!")
    if not self._ctx.capture_complete.is_set(): raise RuntimeError(f"Child Process {self._ctx.pid} has not completed yet!")

    _error = False

    # Remove the Child's Working Directory
    if self._ctx.workdir is not None:
      try:
        os.rmdir(self._ctx.workdir)
      except FileNotFoundError:
        pass
      except Exception as e:
        logger.opt(exception=e).warning(f"An error occurred while cleaning up the Child's Working Directory {self._ctx.workdir}")
        _error = True

    # Close the Parent End of the Channels
    for _chan in (self._ctx.mgmt_chan, self._ctx.error_chan, self._ctx.data_chan):
      try:
        await _chan.stop()
      except Exception as e:
        logger.opt(exception=e).warning(f"An error occurred while stopping the {type(_chan).__name__} for the Child Process {self._ctx.pid}")
        _error = True

    # Unregister all of the File Descriptors from the IO Watcher
    for _fd in chain(self._ctx.stdin, self._ctx.stdout, self._ctx.stderr):
      if _fd is None: continue
      assert isinstance(_fd, int)
      try:
        self._ctx.io_watcher.unregister(_fd)
      except Exception as e:
        logger.opt(exception=e).warning(f"An error occurred while unregistering the File Descriptor {_fd} for the Child Process {self._ctx.pid}")
        _error = True

    # Close all of the File Descriptors we opened
    for _fd in chain(self._ctx.stdin, self._ctx.stdout, self._ctx.stderr):
      if _fd is None: continue
      assert isinstance(_fd, int)
      try:
        os.close(_fd)
      except Exception as e:
        logger.opt(exception=e).warning(f"An error occurred while closing the File Descriptor {_fd} for the Child Process {self._ctx.pid}")
        _error = True
    
    if _error: raise RuntimeError(f"An error occurred while cleaning up the Child Process {self._ctx.pid}")

  async def spawn(self):
    """Spawn the Task"""
    if self._ctx.pid is not None: raise RuntimeError(f"Task {self.id} has already been spawned!")
    if self._ctx.pid == 0: raise RuntimeError(f"spawn can only be executed by the parent!")
    if not self._ctx.io_watcher.running: raise RuntimeError(f"The IO Watcher is not running")

    # Fill in all of the Ctx Vars
    self._ctx.capture_complete = asyncio.Event()

    # Setup the Child's User & Group IDs
    if self._ctx.uid is None: self._ctx.uid = os.geteuid()
    if self._ctx.gid is None: self._ctx.gid = os.getegid()

    # Setup the Child's Working Directory
    if self._ctx.workdir is None:
      self._ctx.workdir = tempfile.mkdtemp()
      os.chmod(self._ctx.workdir, 0o750)
      os.chown(self._ctx.workdir, self._ctx.uid, self._ctx.gid)

    # Setup the Child's Stdin, Stdout & Stderr
    self._ctx.stdin, self._ctx.stdout, self._ctx.stderr = os.pipe(), os.pipe(), os.pipe()
    (_stdin_r, _stdin_w), (_stdout_r, _stdout_w), (_stderr_r, _stderr_w) = self._ctx.stdin, self._ctx.stdout, self._ctx.stderr
    os.set_inheritable(_stdin_r, True); os.set_blocking(_stdin_r, False)
    os.set_inheritable(_stdin_w, False); os.set_blocking(_stdin_w, False)
    os.set_inheritable(_stdout_r, False); os.set_blocking(_stdout_r, False); 
    os.set_inheritable(_stdout_w, True); os.set_blocking(_stdout_w, False)
    os.set_inheritable(_stderr_r, False); os.set_blocking(_stderr_r, False)
    os.set_inheritable(_stderr_w, True); os.set_blocking(_stderr_w, False)

    # Setup the Parent End of the Channels
    # MgmtChan (RW): Stdin + Stdout
    # ErrorSink (RO): Stderr
    # DataChan (RW): TODO

    self._ctx.mgmt_chan = Pipe(
      # The Parent Reads Mgmt Messages from the Child's Stdout
      source=AsyncFileDescriptor(
        fd=_stdout_r,
        event_log=self._ctx.io_watcher.register(_stdout_r, IOEvent.READ),
      ),
      # The Parent Writes Mgmt Messages to the Child's Stdin
      sink=AsyncFileDescriptor(
        fd=_stdin_w,
        event_log=self._ctx.io_watcher.register(_stdin_w, IOEvent.WRITE),
      ),
    )
    self._ctx.error_chan = Pipe(
      # The Parent Reads Errors from the Child's Stderr
      source=AsyncFileDescriptor(
        fd=_stderr_r,
        event_log=self._ctx.io_watcher.register(_stderr_r, IOEvent.READ),
      ),
      # The Error Channel is one-way; the parent can't write anything back to the child
      sink=None,
    )
    # TODO: How exactly do we we want to setup the Data Channel?
    self._ctx.data_chan = Channel(
      source=None,
      sink=None,
    )

    await self._ctx.mgmt_chan.start()
    await self._ctx.error_chan.start()
    await self._ctx.data_chan.start()

    # Spawn the Child
    if self._ctx.uid == 0: logger.warning("Running Child as Root User")
    if self._ctx.gid == 0: logger.warning("Running Child as Root Group")
    logger.debug(f"""\
Spawning Child Process
  - Command: {self.cmd}
  - Arguments: {self.argv}
  - Environment: {self.env}
  - Working Directory: {self._ctx.workdir}
  - User ID: {self._ctx.uid}
  - Group ID: {self._ctx.gid}
  - Stdin: {_stdin_r} -> {_stdin_w}
  - Stdout: {_stdout_r} -> {_stdout_w}
  - Stderr: {_stderr_r} -> {_stderr_w}
"""
    )

    gc.freeze() # Freeze Python's GC so we don't Copy-On-Write in the Child
    self._ctx.pid = os.fork()
    if self._ctx.pid == 0: self._exec() # If Child then Execute the Task
    # Continue w/ the Parent's Execution
    gc.unfreeze()

    # Spawn a Thread to capture the Child's Return Code
    self._ctx.capture_thread = threading.Thread(target=self._capture_child_exit, daemon=True)
    self._ctx.capture_thread.start()

    # Wait for the child to tell us it's ready; TODO: How long should we wait?
    try:
      async with asyncio.timeout(1):
        await self._ctx.mgmt_chan.wait() # Wait for the Channel to be ready before we attempt to read from it
        async for msg in self._ctx.mgmt_chan:
          assert isinstance(msg, MgmtMsg)
          if msg.kind == MgmtMsgKind.READY:
            logger.debug(f"Child Process {self._ctx.pid} is Ready")
            break
          else: raise RuntimeError(f"Unexpected Management Message: {msg}")
    except asyncio.TimeoutError:
      logger.warning(f"Child Process {self._ctx.pid} is taking too long to start; forcing termination")
      await self._kill_child_process(signal.SIGKILL)
      # The Thread will be cleaned up on when exiting Python
      raise RuntimeError(f"Child Process {self._ctx.pid} took too long to start")
    
  async def cancel(self):
    """Cancel execution of the Task"""
    if self._ctx.pid is None: raise RuntimeError(f"Task {self.id} has not been spawned yet!")
    if self._ctx.pid == 0: raise RuntimeError(f"cancel can only be executed by the parent!")

    if self._ctx.returncode is not None: return # The Child has already completed

    try:
      # Send the Cancel Message to the Child but don't wait for it to be sent
      await self._ctx.mgmt_chan.send(MgmtMsg(kind=MgmtMsgKind.CANCEL, payload=MgmtMsgPayload()), block=False)

      async with asyncio.timeout(10):
        async for msg in self._ctx.mgmt_chan:
          assert isinstance(msg, MgmtMsg)
          if msg.kind == MgmtMsgKind.COMPLETE:
            logger.debug(f"Child Process {self._ctx.pid} is Shutting Down")
            break
          else:
            logger.debug(f"Ignoring Management Message: {msg}")
        await self._ctx.capture_complete.wait()
    except asyncio.TimeoutError:
      logger.warning(f"Child Process {self._ctx.pid} is taking too long to cancel; forcing termination")
      try:
        await self._kill_child_process(signal.SIGTERM)
      except Exception as e:
        logger.opt(exception=e).error(f"An error occurred while Terminating the Child Process {self._ctx.pid}")
        await self._kill_child_process(signal.SIGKILL)
    
    self._cleanup_child()
  
  async def wait(self) -> TaskState:
    """Wait for the Task to complete, cancel or error out. Returns the final state of the Task"""
    if self._ctx.pid is None: raise RuntimeError(f"Task {self.id} has not been spawned yet!")
    if self._ctx.pid == 0: raise RuntimeError(f"wait can only be executed by the parent!")

    if self._ctx.returncode is not None: return self._get_state()
    await self._ctx.capture_complete.wait()
    return self._get_state()
