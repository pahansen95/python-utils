from __future__ import annotations

import enum
import asyncio
import ssl
import aiohttp.web
import logging
import inspect
from dataclasses import dataclass, field, KW_ONLY
from collections.abc import Callable, Coroutine
from pathlib import PurePath
from yarl import URL
from loguru import logger

### HTTP WebServer

class InterceptHandler(logging.Handler):
  def emit(self, record: logging.LogRecord) -> None:
    # logger.debug(f"Intercepted Log Record")
    # Get corresponding Loguru level if it exists.
    level: str | int
    try:
      level = logger.level(record.levelname).name
    except ValueError:
      level = record.levelno

    # Find caller from where originated the logged message.
    frame, depth = inspect.currentframe(), 0
    while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
      frame = frame.f_back
      depth += 1

    logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())  

_app_logger = logging.getLogger("utils.webserver")
_app_logger.addHandler(InterceptHandler())
_app_logger.setLevel(logging.DEBUG)
_app_logger.propagate = False
# _app_logger = logging.getLogger("utils.webserver")
# _app_logger.setLevel(logging.DEBUG)
# _app_logger.debug("Initialized Logger for WebServer")

class HttpMethod(enum.Enum):
  """https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods"""
  GET = "GET"
  HEAD = "HEAD"
  POST = "POST"
  PUT = "PUT"
  DELETE = "DELETE"
  CONNECT = "CONNECT"
  OPTIONS = "OPTIONS"
  TRACE = "TRACE"
  PATCH = "PATCH"

class Lifecycle(enum.Enum):
  INIT = enum.auto()
  """The Web Application is Initializing"""
  RUNNING = enum.auto()
  """The Web Application is Running"""
  STOPPING = enum.auto()
  """The Web Application is Stopping"""
  STOPPED = enum.auto()
  """The Web Application is Stopped"""

@dataclass
class RouteSpec:
  method: HttpMethod
  path: PurePath
  handler_factory: Callable[..., Coroutine]

  def __str__(self) -> str: return f"{self.method.value} {self.path.as_posix()}"
  def __hash__(self) -> int: return hash((self.method.value, self.path.as_posix()))

@dataclass
class WebServerCtx:
  """
  The Stateful Context of the Web Application
  """

  app: aiohttp.web.Application | None = None
  runner: aiohttp.web.BaseRunner | None = None
  site: aiohttp.web.BaseSite | None = None
  lfcl_events: dict[Lifecycle, asyncio.Event] = field(default_factory=lambda: {e: asyncio.Event() for e in Lifecycle})

@dataclass
class WebServer:
  listen: URL
  ssl_context: ssl.SSLContext
  routes: set[RouteSpec] = field(default_factory=set)

  _: KW_ONLY

  _ctx: WebServerCtx = field(default_factory=WebServerCtx)

  @property
  def online(self) -> bool: return self._ctx.lfcl_events[Lifecycle.INIT].is_set()

  async def wait_for(self, stage: Lifecycle):
    if stage not in self._ctx.lfcl_events: raise ValueError(f"Unknown Lifecycle Stage: {stage}")
    logger.debug(f"Waiting for Web Application to reach Lifecycle Stage: {stage}")
    await self._ctx.lfcl_events[stage].wait()

  async def start(self):
    if self._ctx.lfcl_events[Lifecycle.INIT].is_set(): raise RuntimeError("Web Application has already been started")
    logger.info(f"Starting Web Application at {self.listen}")
    self._ctx.lfcl_events[Lifecycle.STOPPED].clear()
    self._ctx.lfcl_events[Lifecycle.INIT].set()
    self._ctx.app = aiohttp.web.Application(
      logger=_app_logger,
    )
    logger.debug("Adding Routes to Application")
    for route in self.routes:
      logger.debug(f"Adding Route: {route}")
      assert route.path.is_absolute()
      self._ctx.app.router.add_route(route.method.value, route.path.as_posix(), route.handler_factory())
    self._ctx.runner = aiohttp.web.AppRunner(
      self._ctx.app,
      handle_signals=False,
      logger=_app_logger,
      access_log=_app_logger,
      access_log_class=aiohttp.web.AccessLogger,
      access_log_format=aiohttp.web.AccessLogger.LOG_FORMAT,
    )
    logger.debug("Setting up Application Runner")
    await self._ctx.runner.setup()
    if self.listen.scheme == "tcp":
      if self.listen.path is not None and self.listen.path.lstrip('/') != "": raise ValueError("TCP URLs should not have a path")
      self._ctx.site = aiohttp.web.TCPSite(runner=self._ctx.runner, host=self.listen.host, port=self.listen.port, ssl_context=self.ssl_context)
    elif self.listen.scheme == "unix": self._ctx.site = aiohttp.web.UnixSite(runner=self._ctx.runner, host=self.listen.path, ssl_context=self.ssl_context)
    else: raise NotImplementedError(self.listen.scheme)
    logger.debug("Starting Application Site")
    await self._ctx.site.start()
    self._ctx.lfcl_events[Lifecycle.RUNNING].set()
    logger.success(f"Web Application is Online at {self.listen}")
  
  async def stop(self):
    if not self._ctx.lfcl_events[Lifecycle.INIT].is_set(): raise RuntimeError("Web Application is not Online")
    if self._ctx.lfcl_events[Lifecycle.STOPPING].is_set(): raise RuntimeError("Web Application is already stopping")
    logger.info("Taking Web Application Offline")
    self._ctx.lfcl_events[Lifecycle.STOPPING].set()
    if self._ctx.runner is not None: await self._ctx.runner.cleanup()
    self._ctx.lfcl_events[Lifecycle.RUNNING].clear()
    self._ctx.lfcl_events[Lifecycle.STOPPING].clear()
    self._ctx.lfcl_events[Lifecycle.STOPPED].set()
    self._ctx.app, self._ctx.runner, self._ctx.site = None, None, None
    logger.success("Web Application has been taken Offline")
  
  async def reset(self):
    if self._ctx.lfcl_events[Lifecycle.RUNNING].is_set(): raise RuntimeError("Web Application is currently running")
    self.routes.clear()
    for e in self._ctx.lfcl_events.values(): e.clear()
    self._ctx.app, self._ctx.runner, self._ctx.site = None, None, None

  def route_exists(self, route: RouteSpec) -> bool: return route in self.routes

  def register_route(self, route: RouteSpec) -> None:
    if self._ctx.lfcl_events[Lifecycle.INIT].is_set() and not self._ctx.lfcl_events[Lifecycle.STOPPED].is_set(): raise RuntimeError("Cannot Register Routes when the Web Application is Online")
    if route in self.routes: raise KeyError(f"`{route}` is already registered")
    self.routes.add(route)
    logger.success(f"Registered Route: {route}")
  
  def deregister_route(self, route: RouteSpec) -> None:
    if self._ctx.lfcl_events[Lifecycle.INIT].is_set() and not self._ctx.lfcl_events[Lifecycle.STOPPED].is_set(): raise RuntimeError("Cannot Deregister Routes when the Web Application is Online")
    if route not in self.routes: raise KeyError(f"Route `{route}` is not registered")
    self.routes.remove(route)
    logger.success(f"Deregistered Route: {route}")

###
