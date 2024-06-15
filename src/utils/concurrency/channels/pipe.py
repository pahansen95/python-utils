"""

# Pipe based Channels

"""

from __future__ import annotations
from dataclasses import dataclass, field, KW_ONLY
from typing import Any
from collections.abc import Callable, ByteString, AsyncIterator
from . import Channel, ChannelMode
from ..messages import Message
from ..aio.fd import AsyncFileDescriptor
from .. import ItemLog
import asyncio
from loguru import logger

@dataclass
class _PipeCtx:

  tx_task: asyncio.Task | None = None
  """The Task for Transmitting Messages on the Channel"""
  rx_task: asyncio.Task | None = None
  """The Task for Receiving Messages on the Channel"""
  peer_online: asyncio.Event = field(default_factory=asyncio.Event)
  """An Event to signal when the peer's end of the channel is ready"""
  started: asyncio.Event = field(default_factory=asyncio.Event)
  """An Event to signal when the Channel has been started"""

  rx_queue: ItemLog[Message] = field(default_factory=ItemLog)
  """The Queue of Messages received on the Channel."""
  tx_queue: ItemLog[Message] = field(default_factory=ItemLog)
  """The Queue of Messages to be sent on the Channel."""
  
  ### See this ASCII Table: https://www.asciitable.com/
  ready_byte: bytes = bytes([0x00]) # Null Byte

@dataclass
class Pipe(Channel):
  """One end of a Pipe based Channel for Task Communication
  
  There is a general order of messages for a Pipe Channel:
  
  1. Write a Ready Byte
  2. Read a Ready Byte
  3. Write the Peer Channel Metadata
  4. Read the Peer Channel Metadata
  5. Write a ACK/ERR Byte
  6. Read a ACK/ERR Byte
  7. Write & Read Messages Concurrently
  8. Write a Unready Byte
  9. Read a Unready Byte

  The Ready Byte indicates that the peer's end of the channel is ready.
  The Peer Channel Metadata is the first message sent by the peer channel to declare it's operating mode.
  If the Peer's declared operating mode is incompatible then...
    - a ERR Byte is sent (best-effort) to the peer
    - this channel will raise an error
  Otherwise an ACK Byte is sent to the peer.
  At this point normal Tx/Rx occurs:
    - Messages are sent to the peer as a pair of frames
      1. The first frame is the length of the message as a 64bit unsigned integer in Big Endian
      2. The second frame is the message itself as a ByteString
  At any point, either end of the peer can send a Unready Byte to signal that it has been stopped.
  In this case, the Channel will automatically exit the Tx/Rx Loops and raise a PeerStopped Error.
      
  The Channel will monitor for Close fdevents on the pipe.
  In the event of a Close fdevent, the Channel will automatically exit the Tx/Rx Loops and
  raise a PipeClosed Error.
  """
  
  source: AsyncFileDescriptor
  """The source to read from"""
  sink: AsyncFileDescriptor
  """The sink to write to"""
  mode: ChannelMode
  """The Mode of the Channel"""
  
  unmarshal: Callable[[ByteString], Message]
  """Dependency Injection; The Factory Function to Unmarshal a Message from a ByteSTring"""

  _ : KW_ONLY

  _ctx: _PipeCtx = field(default_factory=_PipeCtx)
  
  async def __aiter__(self) -> AsyncIterator[Message]:
    """Return an Async Iterator for the Channel"""
    if not self._ctx.started.is_set(): raise RuntimeError("The Channel has not been started")
    if self.source is None: raise RuntimeError("Cannot receive messages on a One-Way WO Channel")
    return self

  async def __anext__(self) -> Message:
    """Return the Next Message from the Channel until the Channel is stopped."""
    if self.source is None: raise RuntimeError("Cannot receive messages on a One-Way WO Channel")
    if self._ctx.rx_queue.empty and not self._ctx.started.is_set(): raise StopAsyncIteration() # If there are no messages & the channel isn't running then we're done
    return self.unmarshal(await self._ctx.rx_queue.pop())
  
  async def _rx_loop(self) -> None:
    """The Loop for Receiving Messages on the Channel
    
    """
    try:
      if self.source is None: raise RuntimeError("The Channel is One-Way WO Channel")
      assert self.source is not None

      _ready = await self.source.read(1) # Read a single byte to check if the peer is ready
      if _ready != self._ctx.ready_byte: raise RuntimeError(f"The peer's end of the channel did not send the expected ready byte: {_ready}")
      self._ctx.peer_online.set()
      
      _frame_size_buf: bytearray = bytearray(8)
      while True:
        # Read Frames from the Source
        # The First Frame indicates the Length of the Message as a 64bit unsigned integer in Big Endian
        await self.source.read(8, buffer=_frame_size_buf)
        msg_size = int.from_bytes(_frame_size_buf, "big", signed=False)
        # The Second Frame is the Message itself
        await self._ctx.rx_queue.push(self.unmarshal(await self.source.read(msg_size)))
    except asyncio.CancelledError:
      logger.trace(f"Channel {id(self)}: RX Loop Cancelled")
      raise
    except:
      # Make sure to trap & log any errors
      logger.opt(exception=True).warning(f"Channel {id(self)}: Fatal Error")
      raise

  async def _tx_loop(self) -> None:
    """The Loop for Transmitting Messages on the Channel
    
    There is a general order formessages:

    - The Ready Byte: A single byte to signal that the peer's end of the channel is ready
    - Peer Channel Metadata: The first message sent by the peer channel to declare it's operating mode
      - If the Peer's declared operating mode is incompatible then...
        - a notification is sent (best-effort) to the peer
        - this channel will raise an error
      - Otherwise an ACK is sent to the peer
    - Messages: Messages are sent to the peer as a pair of frames
      1. The first frame is the length of the message as a 64bit unsigned integer in Big Endian
      2. The second frame is the message itself as a ByteString
    
    """
    try:
      # Send the Ready Byte
      await self.sink.write(self._ctx.ready_byte, len(self._ctx.ready_byte))

      while True:
        # Transmit Frames to the Sink
        msg = (await self._ctx.tx_queue.pop()).marshal()
        msg_size = len(msg)
        await self.sink.write(msg_size.to_bytes(8, "big", signed=False), 8)
        await self.sink.write(msg, msg_size)
    except asyncio.CancelledError:
      logger.trace(f"Channel {id(self)}: TX Loop Cancelled")
      raise
    except:
      # Make sure to trap & log any errors
      logger.opt(exception=True).warning(f"Channel {id(self)}: Fatal Error")
      raise
  
  async def start(self) -> None:
    """Start Handling Communication on the Channel"""
    if self._ctx.started.is_set(): raise RuntimeError("The Channel has already been started")

    self._ctx.tx_task = asyncio.create_task(self._tx_loop())
    self._ctx.rx_task = asyncio.create_task(self._rx_loop())

  async def stop(self)  -> None:
    """Stop Handling Communication on the Channel"""
    if not self._ctx.started.is_set(): raise RuntimeError("The Channel has not been started")
    assert self._ctx.tx_task is not None
    assert self._ctx.rx_task is not None

    self._ctx.tx_task.cancel()
    self._ctx.rx_task.cancel()

    await asyncio.wait((
      _task for _task in [self._ctx.tx_task, self._ctx.rx_task]
      if not _task.done()
    ), return_when=asyncio.ALL_COMPLETED)

    self._ctx.peer_online.clear()
    self._ctx.started.clear()
  
  async def reset(self) -> None:
    """Reset the Channel"""
    if self._ctx.started.is_set(): raise RuntimeError("Can't reset the channel while it's running")
    self._ctx.rx_queue.clear()
    self._ctx.tx_queue.clear()
    self._ctx.peer_online.clear()

  async def wait(self) -> None:
    """Wait until the peer's end of the channel is ready"""
    if not self._ctx.started.is_set(): raise RuntimeError("The Channel has not been started")
    await self._ctx.peer_online.wait()

  async def send(self, message: Message, block: bool = True) -> None:
    """Enqueue a Message to be sent on the Channel. In Blocking Mode, the method will block until the Message is sent."""
    if not self._ctx.started.is_set(): raise RuntimeError("The Channel has not been started")
    if self.mode == ChannelMode.RO: raise RuntimeError("Cannot send messages on a One-Way RO Channel")
    await self._ctx.tx_queue.push(message)

    ### TODO: Need to refactor ItemLog to better track items in the log
    if block:
      while True:
        await self._ctx.tx_queue.item_removed()
        if message not in self._ctx.tx_queue:
          self._ctx.tx_queue.item_removed.clear()
          break
        else: await asyncio.sleep(0) # Immediately yield to the event loop so the other send tasks can eval too
        # Otherwise it's not our item so don't touch the flag
  
  async def recv(self) -> Message:
    """Receive a Message from the Channel."""
    if not self._ctx.started.is_set(): raise RuntimeError("The Channel has not been started")
    if self.mode == ChannelMode.WO: raise RuntimeError("Cannot receive messages on a One-Way WO Channel")
    return await self._ctx.rx_queue.pop()