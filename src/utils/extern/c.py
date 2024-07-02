"""

Extend the Utils Package with C Source using the `cffi` package.

"""
from __future__ import annotations
import cffi, pathlib, blake3, sys, shutil, importlib, importlib.util
from typing import TypedDict, Any
from types import ModuleType
from loguru import logger
from dataclasses import dataclass, field, KW_ONLY
from itertools import chain

### Typehints
_ffi = cffi.FFI()
CType = _ffi.CType
CData = _ffi.CData
c_buffer = _ffi.buffer
del _ffi
###

class BuildSpec(TypedDict):
  """A Specification for Building a CFFI Package.
  
  Write your C Library rooted at the package directory.

  When the CFFI Package is compiled, all the declared headers will be
  injected into the CFFI Package (via the `ffibuilder.set_source`) function.

  Compilation will target all included C Source Files.
  """
  cdef: str
  """The CFFI cdef used to generate the API Integrated Python Module"""
  include: list[str]
  """The Module Header Files to inject into the CFFI Package; must be relative to the package's `include` folder."""
  sources: list[str]
  """The Module Source Code Files to compile; must be relative to the package's root folder. ie. takes the form ['src/foo.c', 'src/bar.c']"""

def calculate_module_fingerprint(pkgdir: pathlib.Path, build_spec: BuildSpec, glob: list[str] = ['src/**/*.c', 'include/**/*.h']) -> str:
  """Calculates an identity fingerprint for the Library + BuildSpec"""
  lib_files = sorted(chain.from_iterable(
    pkgdir.glob(pattern)
    for pattern in glob
  ))
  assert len(lib_files) > 0
  hasher = blake3.blake3()
  hasher.update(build_spec['cdef'].encode())
  hasher.update(''.join(build_spec['include']).encode())
  hasher.update(''.join(build_spec['sources']).encode())
  for f in lib_files: hasher.update_mmap(f.as_posix())
  return hasher.hexdigest()

class BuildStatus(TypedDict):
  """The Build Status of a Module."""
  module: ModuleType
  """The C Library Module"""
  cache: pathlib.Path
  """The Cache Directory of the built Module."""
  fingerprint: str
  """The Fingerprint identifying the build."""

class _Ctx(TypedDict):
  ffibuilder: dict[str, cffi.FFI]
  """The FFI Instance, per Module, to use for building the Module."""
  status: dict[str, BuildStatus | None]
  """The Build Status of each Module in the Registry. None if the Module has not been built."""

  @staticmethod
  def default_factory() -> _Ctx: return {"ffibuilder": {}, "status": {}}

def _default_cache_dir() -> pathlib.Path:
  default_cache_dir = pathlib.Path(__file__).parent / ".cffibuild"
  if not default_cache_dir.exists(): default_cache_dir.mkdir(mode=0o755, parents=False)
  return default_cache_dir

@dataclass
class Registry:
  """A Registry of C Libraries."""
  modules: dict[str, BuildSpec] = field(default_factory=dict)
  cache_dir: pathlib.Path = field(default_factory=_default_cache_dir)
  _: KW_ONLY
  _ctx: _Ctx = field(default_factory=_Ctx.default_factory)

  def add(self, name: str, build_spec: BuildSpec) -> None:
    """Add a C Library to the Registry."""
    if name in self.modules: raise ValueError(f"C Library {name} already exists in the Registry.")
    self.modules[name] = build_spec
    self._ctx["status"][name] = None
    self._ctx["ffibuilder"][name] = cffi.FFI()
    (self.cache_dir / name).mkdir(mode=0o755, parents=False, exist_ok=True)
  
  def remove(self, name: str) -> None:
    """Remove a C Library from the Registry."""
    if name not in self.modules: raise ValueError(f"C Library {name} does not exist in the Registry.")
    module_cache = self.cache_dir / name
    if module_cache.exists(): shutil.rmtree(module_cache.as_posix())
    del self.modules[name]
    del self._ctx["status"][name]

  def assemble(self, name: str, pkgdir: pathlib.Path, workdir: pathlib.Path) -> None:
    """Assembles a well formed directory structure for compiling the C Library. Links in the expected
    
    Args:
      name (str): The Name of the C Library
      pkgdir (Path): The Root of the (External) Package Directory
      workdir (Path): The Working Directory we are assembling the C Library into
    
    """

    for subdir, optional in (
      ('include', True),
      ('src', False),
      ('lib', True),
    ):
      targetdir = pkgdir / subdir
      if not targetdir.exists():
        if not optional: raise ValueError(f'Missing required Package Directory `{subdir.as_posix()}`')
        logger.debug(f'Package Directory `{subdir}` does not exist; skipping...')
        continue
      linkdir = workdir / subdir
      if linkdir.exists():
        if linkdir.is_symlink() and linkdir.readlink() == targetdir: continue
        elif linkdir.is_symlink(): linkdir.unlink()
        elif linkdir.is_dir(): shutil.rmtree(linkdir.as_posix())
        else: raise RuntimeError(f'Invalid File Kind for `{linkdir.as_posix()}`')
      linkdir.symlink_to(targetdir, target_is_directory=True)
    
    (workdir / 'build').mkdir(mode=0o755, parents=False, exist_ok=True)

  def compile(self, name: str, workdir: pathlib.Path) -> None:
    """Compiles the C Library Source Code into a shared library"""
    if name not in self.modules: raise ValueError(f"C Library {name} does not exist in the Registry.")
    if not (workdir.exists() and workdir.is_dir()): raise ValueError(f"Working Directory does not exist: {workdir.as_posix()}")
    include_dir = workdir / 'include'
    lib_dir = workdir / 'lib'
    build_spec = self.modules[name]
    _src = '\n'.join(f'#include "{h}"' for h in build_spec['include'])
    logger.debug(f'C Library `{name}` Generated Source...\n{_src}')
    _ffibuilder = self._ctx["ffibuilder"][name]
    _ffibuilder.cdef(build_spec['cdef'])
    _ffibuilder.set_source(
      module_name=name,
      source=_src,
      ### setuptools KWARGS: https://setuptools.pypa.io/en/latest/deprecated/distutils/apiref.html#distutils.core.Extension
      include_dirs=[ include_dir.relative_to(workdir).as_posix() ] if include_dir.exists() else [],
      sources=build_spec['sources'],
      libraries=[ lib_dir.relative_to(workdir).as_posix() ] if lib_dir.exists() else [],
    )
    _ffibuilder.compile(tmpdir=workdir.as_posix(), target=f'{name}.*', verbose=True)

  def _import(
    self,
    name: str,
    workdir: pathlib.Path,
  ) -> ModuleType:
    """Imports the C Library Module

    Args:
      name (str): The Name of the Module
      workder (Path): The working directory of the module.
    """
    sys.path.insert(0, workdir.as_posix())
    try: return importlib.import_module(name)
    finally: sys.path.pop(0)

  def build(
    self,
    name: str,
    pkgdir: pathlib.Path,
    fingerprint: str,
    workdir: pathlib.Path = None
  ) -> BuildStatus:
    """Intelligently builds the C Library
    
    Args:
      name (str): The Name of the C Library
      lib_dir (Path): The Root of the C Library Directory
      fingerprint (str): A unique build identity for the current library; a diff from last build triggers a recompilation.
      workder (Opt[Path]): Override the expected working directory for a build; shouldn't be used normally.
    """
    if name not in self.modules: raise ValueError(f"C Library {name} does not exist in the Registry.")
    if workdir is None: workdir = self.cache_dir / name
    if not workdir.exists(): raise ValueError(f"Working Directory does not exist: {workdir.as_posix()}")

    fingerprint_file = workdir / '.fingerprint'
    cached_fingerprint = fingerprint_file.read_text() if fingerprint_file.exists() else ''

    if fingerprint != cached_fingerprint:
      self.assemble(name, pkgdir, workdir)
      self.compile(name, workdir)
      fingerprint_file.write_text(fingerprint)
    self._ctx['status'][name] = {
      "cache": workdir,
      "fingerprint": fingerprint,
      "module": self._import(name, workdir)
    }
    assert self._ctx['status'][name] is not None
    return {} | self._ctx['status'][name] # Return a Copy

  def get(self, name: str) -> tuple[cffi.FFI, ModuleType]:
    """Return the C Library Interface"""
    if self._ctx['status'][name] is None: raise ValueError(f"The C Library {name} hasn't been built")
    c_module = self._ctx['status'][name]['module']
    return c_module.ffi, c_module.lib
  
c_registry = Registry()
"""The Global C Library Registry."""
