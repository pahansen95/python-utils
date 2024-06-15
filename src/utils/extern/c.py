"""

Extend the Utils Package with C Source using the `cffi` package.

"""
from __future__ import annotations
import cffi, pathlib, blake3, sys
from typing import TypedDict
from loguru import logger
from dataclasses import dataclass, field, KW_ONLY

import cffi.commontypes

_cffibuilder = cffi.FFI()
"""The Global CFFI Instance for building modules."""

def c_new_malloc(*args, **kwargs): return _cffibuilder.new_allocator(*args, **kwargs)
c_malloc = c_new_malloc(should_clear_after_alloc=False)
"""The default Global Memory Allocator"""
c_buffer = _cffibuilder.buffer
"""The Global CFFI `buffer` Type"""
c_from_buffer = _cffibuilder.from_buffer
"""The Global CFFI `from_buffer` function"""
c_cast = _cffibuilder.cast
"""The Global CFFI `cast` function"""
c_release = _cffibuilder.release
"""The Global CFFI `release` function"""

class BuildSpec(TypedDict):
  """A Specification for Building a CFFI Module."""
  headers: list[pathlib.Path]
  """The Headers to include in the Module; will be concatenated in supplied order as the `cdef` for the cffi builder."""
  source: list[pathlib.Path]
  """The Module Source Code; will be concatenated in supplied order as the `source` for the cffi builder."""

class BuildStatus(TypedDict):
  """The Build Status of a Module."""
  cache: pathlib.Path
  """The Cache Directory of the built Module."""
  fingerprint: str
  """The Fingerprint identifying the build."""

class _Ctx(TypedDict):
  ffibuilder: cffi.FFI
  """The FFI Instance to use for building Modules."""
  status: dict[str, BuildStatus | None]
  """The Build Status of each Module in the Registry. None if the Module has not been built."""

  @staticmethod
  def default_factory() -> _Ctx:
    return {"ffibuilder": _cffibuilder, "status": {}}

def _default_cache_dir() -> pathlib.Path:
  default_cache_dir = pathlib.Path(__file__).parent / ".cffibuild"
  if not default_cache_dir.exists(): default_cache_dir.mkdir(mode=0o755, parents=False)
  return default_cache_dir

@dataclass
class Registry:
  """A Registry of CFFI Modules."""
  modules: dict[str, BuildSpec] = field(default_factory=dict)
  cache_dir: pathlib.Path = field(default_factory=_default_cache_dir)
  _: KW_ONLY
  _ctx: _Ctx = field(default_factory=_Ctx.default_factory)

  def add_module(self, name: str, build_spec: BuildSpec) -> None:
    """Add a Module to the Registry."""
    if name in self.modules: raise ValueError(f"Module {name} already exists in the Registry.")
    self.modules[name] = build_spec
    self._ctx["status"][name] = None
  
  def remove_module(self, name: str) -> None:
    """Remove a Module from the Registry."""
    if name not in self.modules: raise ValueError(f"Module {name} does not exist in the Registry.")
    del self.modules[name]
    del self._ctx["status"][name]
    
  def compile_module(self, name: str, module_cache: pathlib.Path) -> None:
    """Compiles the Module Source Code into a shared library"""
    if name not in self.modules: raise ValueError(f"Module {name} does not exist in the Registry.")
    if not (module_cache.exists() and module_cache.is_dir()): raise ValueError(f"Module {name} has not been built yet.")
    _ffibuilder = self._ctx["ffibuilder"]
    _ffibuilder.cdef((module_cache / 'src' / f"{name}.h").read_text())
    _ffibuilder.set_source(name, (module_cache / 'src' / f"{name}.c").read_text())
    _ffibuilder.compile(tmpdir=module_cache.as_posix(), target=f'{name}.*', verbose=True)

  def assemble_module(self, name: str) -> pathlib.Path:
    """Assembles the Module for compilation"""
    if name not in self.modules: raise ValueError(f"Module {name} does not exist in the Registry.")

    _header: str = ""
    for header in self.modules[name]["headers"]: _header += header.read_text()
    _src: str = ""
    for source in self.modules[name]["source"]: _src += source.read_text()

    (module_cache := self.cache_dir / name).mkdir(mode=0o755, parents=False, exist_ok=True)
    (_module_src := module_cache / 'src').mkdir(mode=0o755, parents=False, exist_ok=True)
    (_module_src / f"{name}.h").write_text(_header)
    (_module_src / f"{name}.c").write_text(_src)

    return module_cache

  def calculate_module_fingerprint(self, name: str, module_cache: pathlib.Path) -> tuple[bool, str, pathlib.Path]:
    """Calculates the Fingerprint of a Module returning (diff, fingerprint, fingerprint_file)"""
    if name not in self.modules: raise ValueError(f"Module {name} does not exist in the Registry.")
    if not (module_cache.exists() and module_cache.is_dir()): raise ValueError(f"Module {name} has not been built yet.")
    fingerprint_file = module_cache / ".fingerprint"

    hasher = blake3.blake3()
    hasher.update((module_cache / f"src/{name}.h").read_bytes())
    hasher.update((module_cache / f"src/{name}.c").read_bytes())
    new_fingerprint = hasher.hexdigest()
    old_fingerprint = fingerprint_file.read_text().strip() if fingerprint_file.exists() else ''
    return (
      new_fingerprint != old_fingerprint,
      new_fingerprint,
      fingerprint_file
    )

  def build_module(self, name: str, add_to_path: bool = True) -> BuildStatus:
    """Intelligently builds the Module"""
    if name not in self.modules: raise ValueError(f"Module {name} does not exist in the Registry.")

    module_cache = self.assemble_module(name)
    diff, fingerprint, fingerprint_file = self.calculate_module_fingerprint(name, module_cache)
    if diff:
      fingerprint_file.write_text(fingerprint)
      self.compile_module(name, module_cache)
    
    self._ctx['status'][name] = {"cache": module_cache, "fingerprint": fingerprint}
    if add_to_path: sys.path.insert(0, module_cache.as_posix())
    else: logger.debug(f'External C Module `{name}` was not added to Python Search Path')

    return {} | self._ctx['status'][name] # Return a Copy

c_registry = Registry()
"""The Global C Module Registry."""
