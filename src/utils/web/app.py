from __future__ import annotations
from dataclasses import dataclass, field, KW_ONLY, fields
import htbuilder as h
from htbuilder import HtmlElement
from pathlib import Path, PurePath
import asyncio
import os
import sys
import orjson
from loguru import logger
from typing import Any, Protocol, runtime_checkable, TypedDict, NotRequired
from abc import abstractmethod
import aiohttp.web

### Relative Module Imports

from . import WebServer, RouteSpec, HttpMethod, Lifecycle

### Local src/ Imports

import utils.filesystem as fs
from utils.filesystem import IOWatcher, io_watcher

from utils.kvstore import KVStore, PathKey, MapValue

###

@dataclass(frozen=True)
class BuildSpec:
  """The Build Specification for an Application"""

  class LinkSpec(TypedDict):
    """The Link Specification for an Asset"""
    rel: NotRequired[str]
    """The Relationship of the Link; ie. 'stylesheet'"""

  class AssetSpec(TypedDict):
    """The Asset Specification for an Application"""
    src: Path
    """The Source Path of the Asset"""
    dst: Path
    """The Destination Path of the Asset"""
    link: NotRequired[BuildSpec.LinkSpec]
    """The Link Specification for the Asset if it should be linked in the page header"""

  src_dir: Path
  """The Source Directory of the Application"""
  app_root: Path
  """The Root HTML Page of the Application; must be relative to the src_dir"""
  entrypoints: list[Path]
  """The JS/TS Scripts that serve as Entrypoints for the Application; all must be relative to the src_dir"""
  assets: list[AssetSpec]
  """The Assets of the Application; all must be relative to the src_dir"""
  root_page_kwargs: dict = field(default_factory=dict)
  """Static Overrides for the Root Page of the Application"""

  async def build(self, workdir: Path, minify: bool = True) -> Path:
    """Build the Application returning the Build Directory"""
    
    if not workdir.exists(): raise ValueError(f"Work Directory {workdir} does not exist")
    if not workdir.resolve().is_dir(): raise ValueError(f"Work Directory {workdir} is not a directory")
    
    # # Symlink everything from the True Source Directory to the Runtime Source Directory
    # async for f in fs.walk_directory_tree(
    #   self.src_dir,
    #   ignore_directory_startswith=("__pycache__",),
    # ):
    #   _f = workdir / f.relative_to(self.src_dir)
    #   logger.trace(f"Resolved Path: {f} -> {_f}")
    #   if f.is_dir(): # Create Directories
    #     logger.trace(f"Creating Directory: {_f}")
    #     _f.mkdir(exist_ok=True)
    #     continue
    #   elif f.is_symlink(): # Propagate Symlinks
    #     logger.trace(f"Propagating Symlink: {_f}")
    #     if not _f.exists(): _f.symlink_to(f)
    #     continue
    #   elif not f.is_file():
    #     logger.trace(f"Skipping unsupported File Type: {f}")
    #   assert f.is_file() and not f.is_symlink() and not f.is_dir()
    #   logger.trace(f"SymLinking File: {f} -> {_f}")
    #   # Otherwise Handle Files
    #   # Skip certain files
    #   if f.suffix in (
    #     ".py",
    #   ): continue
    #   if not _f.exists(): _f.symlink_to(f)

    if len(self.entrypoints) == 0: raise ValueError("No Entrypoints specified")
    entrypoints: list[str] = []
    for e in self.entrypoints:
      if e.is_absolute(): raise ValueError(f"Entrypoint {e} must be relative (to the Source Directory)")
      _e = self.src_dir / e
      if not _e.exists(): raise FileNotFoundError(f"Entrypoint {e} does not exist")
      if not _e.resolve().is_file(): raise NotADirectoryError(f"Entrypoint {e} is not a file")
      entrypoints.append(e.as_posix())
    assert len(entrypoints) > 0

    _argv = [x for x in (
      [
        "bun", "build",
          "--target=browser",
          f"--outdir={workdir.resolve().as_posix()}",
          "--minify" if minify else None,
        *entrypoints,
      ]
    ) if x is not None]
    logger.debug(f"Building Application: `{' '.join(_argv)}`")

    default_env = {
      "USER": os.geteuid(),
      "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
      "TMP": "/tmp",
      "PYTHONPATH": ":".join(sys.path),
    }
    _env = {k: v for k, v in {
      k: os.environ.get(k, default_env.get(k, None))
      for k in (
        "PATH",
        "TMP",
        "USER",
        "HOME",
        "CI_PROJECT_DIR",
        "CI_PROJECT_NAME",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "VIRTUAL_ENV",
        "PYTHONPATH",
      )
    }.items() if v is not None }
    logger.debug(f"Using Environment: {', '.join(f'{k}={v}' for k, v in _env.items())}")
    
    bun = await asyncio.create_subprocess_exec(
      *_argv,
      # "pwd", "-L",
      stdin=asyncio.subprocess.DEVNULL,
      stdout=asyncio.subprocess.PIPE,
      stderr=asyncio.subprocess.PIPE,
      env=_env,
      cwd=self.src_dir.resolve().as_posix()
    )
    try:
      async with asyncio.timeout(10):
        stdout, stderr = await bun.communicate()
    except asyncio.TimeoutError:
      logger.warning("Build Process Timed Out")
      bun.kill()
      await bun.wait()
    finally:
      logger.debug(f"Build Process Completed with Status: {bun.returncode}\n{stdout.decode()}\n{stderr.decode()}")
    if bun.returncode != 0: raise RuntimeError(f"Build Process Failed: {stderr.decode()}")    

    for e in self.entrypoints:
      _e = workdir / (e.name.split('.', maxsplit=1)[0] + ".js")
      if not _e.exists(): raise FileNotFoundError(f"Entrypoint {_e} was not built!")
    
    for a in self.assets:
      _src = self.src_dir / a['src']
      _dst = workdir / a['dst']
      if not _src.exists(): raise FileNotFoundError(f"Asset Source {_src} does not exist")
      if not _src.resolve().is_file(): raise NotADirectoryError(f"Asset Source {_src} is not a file")
      if _dst.exists(): _dst.unlink()
      _dst.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
      _dst.symlink_to(_src)

    return workdir

@runtime_checkable
class InstanceTemplate(Protocol):

  workdir: Path
  """The Working Directory of the Application Instance; all paths & links (such as assets) are relative to this"""
  
  @property
  def route_specs(self) -> frozenset[RouteSpec]:
    """The Route Specifications for the Application"""
    return frozenset([
      RouteSpec(
        method=HttpMethod.GET,
        path=PurePath("/"),
        handler_factory=lambda: self.root,
      ),
      RouteSpec(
        method=HttpMethod.GET,
        path=PurePath("/assets/{asset:.*}"),
        handler_factory=lambda: self.asset,
      ),
      RouteSpec(
        method=HttpMethod.PUT,
        path=PurePath("/api/stream"),
        handler_factory=lambda: self.establish_stream,
      ),
      RouteSpec(
        method=HttpMethod.GET,
        path=PurePath("/api/stream/{stream_id}"),
        handler_factory=lambda: self.establish_stream,
      ),
      RouteSpec(
        method=HttpMethod.GET,
        path=PurePath("/ws"),
        handler_factory=lambda: self.establish_websocket,
      ),
    ])
  
  @property
  @abstractmethod
  def build_spec(self) -> BuildSpec:
    """The Build Specification for the Application"""
    ...
  
  @property
  @abstractmethod
  def title(self) -> str:
    """The Title of the Application"""
    ...
  
  @property
  @abstractmethod
  def description(self) -> str:
    """The Description of the Application"""
    ...
    
  async def root(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
    """Client is fetching the App; respond w/ the root page"""
    if request.method != HttpMethod.GET.value: return aiohttp.web.HTTPMethodNotAllowed(request.method, [HttpMethod.GET.value])
    return aiohttp.web.FileResponse(self.workdir / "index.html")
  async def asset(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
    """Client is fetching an Asset; respond w/ the asset"""
    if request.method != HttpMethod.GET.value: return aiohttp.web.HTTPMethodNotAllowed(request.method, [HttpMethod.GET.value])
    return aiohttp.web.FileResponse(self.workdir / request.match_info["asset"])
  @abstractmethod
  async def establish_stream(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
    """Client is establishing a new streaming session (multiplexed on a websocket); reply with a URL the client can connect to"""
    ...
  @abstractmethod
  async def establish_websocket(self, request: aiohttp.web.Request) -> aiohttp.web.WebSocketResponse:
    """Client is establishing a websocket session; reply w/ a websocket response"""
    ...
  
@dataclass
class AppCtx:
  """
  The Context of the Application
  """
  io_watcher: IOWatcher = field(default_factory=lambda: io_watcher)
  asset_dir: Path | None = None
  built: asyncio.Event = field(default_factory=asyncio.Event)
  instance: InstanceTemplate | None = None

@dataclass
class WebApp:
  srv: WebServer
  kb: KVStore
  workdir: Path
  factory: type[InstanceTemplate]

  _: KW_ONLY

  route_prefix: PurePath | None = None
  _ctx: AppCtx = field(default_factory=AppCtx)

  ### Constructors ###

  @classmethod
  def override_ctx(cls, *args: Any, **kwargs: Any) -> WebApp:
    """Override the Context of the Store"""
    if "_ctx" in kwargs: raise ValueError("Cannot override the Context of a DiskStore directly; please specify the Context Parameters directly")
    cls_kwargs = {f.name: kwargs.pop(f.name) for f in fields(cls) if f.name in kwargs}
    return cls(
      *args,
      **(cls_kwargs | {"_ctx": AppCtx(**kwargs)})
    )
  
  ###

  @staticmethod
  def json_dumps(obj) -> str:
    """Convenvience Function to Dump a JSON Object as a UTF-8 encoded string"""
    return orjson.dumps(obj).decode()
  
  async def root_page(
    self,
    title: str,
    desc: str,
    app_root: str,
    links: list[dict[str, str]] = [],
  ) -> h.HtmlElement:
    """The Root Page of the Application"""
    return (
      h.html(lang="en")(
        h.head()(
          h.meta(charset="utf-8"),
          h.meta(name="viewport", content="width=device-width, initial-scale=1"),
          h.title()(title),
          h.meta(name="description", content=desc),
          h.script(src="assets/app.js", type="module"),
          *[h.link(**kv) for kv in links]
        ),
        h.body()(app_root)
      )
    )

  async def build(self) -> Path:
    """Build the Application returning the Path to the Asset Directory"""

    """TODO

    - Create a Build Directory
    - Use Bun to Transpile the TSX Files into the Build Dir
    - Assemble the Root Page into the Build Dir

    """
    if self._ctx.built.is_set(): raise RuntimeError("Application has already been built")

    if not self.workdir.exists(): raise FileNotFoundError(f"Workdir {self.workdir} does not exist")
    if not self.workdir.resolve().is_dir(): raise NotADirectoryError(f"Workdir {self.workdir} is not a directory")

    self._ctx.instance = self.factory(
      kb=self.kb,
      workdir=self.workdir,
    )
    assert isinstance(self._ctx.instance, InstanceTemplate)
    # Load the Root Page of the App
    _app_root_page = self._ctx.instance.build_spec.app_root
    if not _app_root_page.exists(): raise FileNotFoundError(f"App Root Page {_app_root_page} does not exist")
    if not _app_root_page.resolve().is_file(): raise NotADirectoryError(f"App Root Page {_app_root_page} is not a file")
    app_root_page: bytes = await fs.read_file(_app_root_page, io_watcher=self._ctx.io_watcher)
    asset_dir = await self._ctx.instance.build_spec.build(self.workdir)
    assert asset_dir.exists() and asset_dir.resolve().is_dir()
    _root_page_kwargs = self._ctx.instance.build_spec.root_page_kwargs.copy()
    if 'links' not in _root_page_kwargs: _root_page_kwargs['links'] = []
    if _root_page_kwargs.pop('title', None): logger.debug("Title is set in the HTML Page kwargs; it will be overridden by the Instance Title")
    if _root_page_kwargs.pop('desc', None): logger.debug("Description is set in the HTML Page kwargs; it will be overridden by the Instance Description")
    if _root_page_kwargs.pop('app_root', None): logger.debug("App Root is set in the HTML Page kwargs; it will be overridden by the Instance App Root")
    # Parse the Build Spec for links to add
    for a in self._ctx.instance.build_spec.assets:
      _link = a.get('link')
      if _link is None: continue
      _root_page_kwargs['links'].append({ k: v for k, v in {
        'href': f"assets/{a['dst'].as_posix().lstrip('/')}",
        'rel': _link.get('rel', None),
      }.items() if v is not None })
    root_page = await self.root_page(
      title=self._ctx.instance.title,
      desc=self._ctx.instance.description,
      app_root=app_root_page.decode(),
      **_root_page_kwargs
    )
    await fs.write_file(asset_dir / "index.html", str(root_page).encode(), io_watcher=self._ctx.io_watcher)

    self._ctx.asset_dir = asset_dir
    self._ctx.built.set()
    return asset_dir

  async def run(self, block: bool = True):
    """Run the Application"""
    if not self._ctx.built.is_set(): raise RuntimeError("Application has not been built")

    # Setup the KV Store
    if not self.kb.is_registered(MapValue): self.kb.register_value(MapValue, MapValue.unmarshal)

    # Register the Routes
    if self.route_prefix is not None: raise NotImplementedError
    for route in self._ctx.instance.route_specs:
      self.srv.register_route(route)

    # Start the Server
    await self.srv.start()
    if block: await self.srv.wait_for(Lifecycle.STOPPED)
  
  