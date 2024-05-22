"""

Build the Utility Package

"""
from __future__ import annotations
import pathlib, shutil, itertools
from dataclasses import dataclass
from typing import TypedDict, NotRequired, Literal
from collections.abc import Callable, Generator
from collections import deque
from loguru import logger

### Local Imports
from . import BuildError
from .utils import RequirementSpec
###

class PythonFileSpec(TypedDict):
  """A Python File in a Package/Module/SubModule"""
  kind: Literal['pythonModule', 'pythonInit', 'pythonMain']
  """The Kind of File"""
  src: str
  """The Source Path of the File"""
  dst: str
  """The Destination Path of the File; relative to the Package/Module/SubModule"""

  @staticmethod
  def validate(spec: PythonFileSpec):
    if not isinstance(spec, dict): raise BuildError("Python File Spec must be a dictionary")
    if 'kind' not in spec: raise BuildError("Python File Spec must contain a 'kind' section")
    if spec['kind'] not in ('pythonModule', 'pythonInit', 'pythonMain'): raise BuildError("Invalid Python File Kind")
    if 'src' not in spec: raise BuildError("Python File Spec must contain a 'src' section")
    if 'dst' not in spec: raise BuildError("Python File Spec must contain a 'dst' section")

class BundledFileSpec(TypedDict):
  """A Bundled File in a Package/Module/SubModule"""
  kind: Literal['file', 'link', 'directory']
  """The Kind of File"""
  src: str
  """The Source Path of the File"""
  dst: str
  """The Destination Path of the File; relative to the Package/Module/SubModule"""

  @staticmethod
  def validate(spec: BundledFileSpec):
    if not isinstance(spec, dict): raise BuildError("Bundled File Spec must be a dictionary")
    if 'kind' not in spec: raise BuildError("Bundled File Spec must contain a 'kind' section")
    if spec['kind'] not in ('file', 'link', 'directory'): raise BuildError("Invalid Bundled File Kind")
    if 'src' not in spec: raise BuildError("Bundled File Spec must contain a 'src' section")
    if 'dst' not in spec: raise BuildError("Bundled File Spec must contain a 'dst' section")

FileSpec = PythonFileSpec | BundledFileSpec

class ModuleSpec(TypedDict):
  """A Python Module Spec"""
  module: FileSpec
  """The actual Python File of the Module"""
  files: NotRequired[list[FileSpec]]
  """Extra Files to include in the Module"""
  requirements: NotRequired[str]
  """Path to the Requirements File for the Module"""

  @staticmethod
  def validate(spec: ModuleSpec):
    if not isinstance(spec, dict): raise BuildError("Module Spec must be a dictionary")
    if 'module' not in spec: raise BuildError("Module Spec must contain a 'module' section")
    if not isinstance(spec['module'], dict): raise BuildError(f"Module section must be a dictionary, got {type(spec['module']).__name__}")
    if not isinstance(spec.get('files', []), list): raise BuildError("Files section must be a list")
    PythonFileSpec.validate(spec['module'])
    for idx, file in enumerate(spec.get('files', [])):
      logger.debug(f"Validating File Spec: {idx}")
      FileSpec.validate(file)

class PackageSpec(TypedDict):
  modules: NotRequired[dict[str, ModuleSpec]]
  """The set of modules to include in the package; referenced by name"""
  pkgs: NotRequired[dict[str, PackageSpec]]
  """The set of sub packages to include in the package; referenced by name"""
  init: NotRequired[FileSpec]
  """The `__init__.py` file for the Package"""
  main: NotRequired[FileSpec]
  """The `__main__.py` file for the Package"""
  files: NotRequired[list[FileSpec]]
  """Files to bundle with the Package"""
  requirements: NotRequired[str]
  """Path to the Requirements File for the Package"""

  @staticmethod
  def validate(spec: PackageSpec):
    if not any(k in spec for k in ('modules', 'pkgs', 'init')): raise BuildError("Package Spec must contain at least a 'modules', 'pkgs', and 'init' section")
    if 'main' in spec and not 'init' in spec: raise BuildError("Package Spec must contain an 'init' section if it contains a 'main' section")
    for module_name, module_spec in spec.get('modules', {}).items():
      logger.debug(f"Validating Module Spec: {module_name}")
      ModuleSpec.validate(module_spec)
    for pkg_name, pkg_spec in spec.get('pkgs', {}).items():
      logger.debug(f"Validating Package Spec: {pkg_name}")
      PackageSpec.validate(pkg_spec)
    if 'init' in spec: PythonFileSpec.validate(spec['init'])
    if 'main' in spec: PythonFileSpec.validate(spec['main'])
    for idx, file in enumerate(spec.get('files', [])):
      logger.debug(f"Validating File Spec: {idx}")
      FileSpec.validate(file)

class BuildSpec(TypedDict):
  name: str
  """The Name of the Package"""
  pkg: PackageSpec
  """The Package Specification"""

  @staticmethod
  def validate(spec: BuildSpec):
    logger.debug("Validating the Build Spec")
    if not isinstance(spec, dict): raise BuildError("Build Spec must be a dictionary")
    if 'name' not in spec: raise BuildError("Build Spec must contain a 'name' section")
    if not isinstance(spec['name'], str): raise BuildError("Name section must be a string")
    if 'pkg' not in spec: raise BuildError("Build Spec must contain a 'pkg' section")
    if not isinstance(spec['pkg'], dict): raise BuildError("Package section must be a dictionary")
    logger.debug("Validating the Root Package Spec")
    PackageSpec.validate(spec['pkg'])

class AssemblyOutput(TypedDict):
  """The Output of the Assembly Process"""
  pkgdir: str
  """The Path to the Package Directory"""
  requirements: NotRequired[str]
  """The Merged Requirements File for the Package"""

Spec = PackageSpec | ModuleSpec

class _TreeNode(TypedDict):
  kind: Literal['pkg', 'module', 'file']
  name: str
  spec: Spec
  ops: list[tuple[Callable, list, dict]]
  children: dict[str, _TreeNode]

  @staticmethod
  def factory(kind: Literal['pkg', 'module', 'file'], name: str, spec: Spec) -> _TreeNode:
    return { 'kind': kind, 'name': name, 'spec': spec, 'ops': [], 'children': {} }
  
  @staticmethod
  def walk_tree(node: _TreeNode) -> Generator[_TreeNode, None, None]:
    yield node
    for child in node['children'].values():
      yield from _TreeNode.walk_tree(child)

class _StackFrame(TypedDict):
  kind: Literal['pkg', 'module']
  """What Kind of Node to generate"""
  name: str
  """The Name of the Node"""
  spec: Spec
  """The Spec to parse"""
  parent: _TreeNode
  """The Parent Node this Stack Frame is a child to"""

@dataclass
class PkgOps:
  """Packaging Operations"""
  workdir: pathlib.Path
  """The Working Directory assumed for all relative source paths"""
  pkgdir: pathlib.Path
  """The Package Directory assumed for all relative destination paths"""

  def create_parents(self, path: pathlib.Path):
    if not path.is_absolute(): path = self.workdir / path
    path.parent.mkdir(mode=0o755, exist_ok=True, parents=True)

  def add_file(self, src: pathlib.Path, dst: pathlib.Path):
    """Copy a source file to the destination path in the package directory"""
    if not src.is_absolute(): src = self.workdir / src
    if not (src.exists() and src.is_file()): raise BuildError(f"Source File does not exist: {src}")
    if not dst.is_absolute(): dst = self.pkgdir / dst
    assert dst.relative_to(self.pkgdir)
    if str(dst).endswith('/'): dst = dst / src.name
    self.create_parents(dst)
    shutil.copyfile(src, dst)
  
  def add_directory(self, dir: pathlib.Path, parents: bool = True):
    """Add a directory (tree) to the package directory"""
    if not dir.is_absolute(): dir = self.pkgdir / dir
    assert dir.relative_to(self.pkgdir)
    dir.mkdir(mode=0o755, exist_ok=True, parents=parents)

  def add_link(self, link: pathlib.Path, target: pathlib.Path):
    """Adds the Symlink `link` in the Package Directory targeting `target` in the Package Directory"""
    if not link.is_absolute(): link = self.pkgdir / link
    if not (link.exists() and link.is_file()): raise BuildError(f"Source File does not exist: {link}")
    if not target.is_absolute(): target = self.pkgdir / target
    assert target.relative_to(self.pkgdir)
    self.create_parents(link)
    link.symlink_to(target)
  
  def parse_requirements_file(self, src: pathlib.Path) -> list[RequirementSpec]:
    """Parse a Requirements File and return the list of Requirement Specs"""
    if not src.is_absolute(): src = self.workdir / src
    assert src.relative_to(self.workdir)
    requirements: list[RequirementSpec] = []
    for line in src.read_text().splitlines():
      _line = line.split('#', maxsplit=1)[0].strip()
      if not _line: continue
      requirements.append(RequirementSpec.parse_requirement_line(_line))
    return requirements

def _file_op_factory(
  spec: FileSpec,
  OPS: PkgOps,
) -> tuple[Callable, list, dict]:
  # TODO: Should we do something more with `python*` files?
  if spec['kind'] in ('file', 'pythonModule', 'pythonInit', 'pythonMain'):
    return (
      OPS.add_file,
      [pathlib.Path(spec['src']), pathlib.Path(spec['dst'])],
      {}
    )
  elif spec['kind'] == 'link':
    return (
      OPS.add_link,
      [pathlib.Path(spec['src']), pathlib.Path(spec['dst'])],
      {}
    )
  elif spec['kind'] == 'directory':
    return (
      OPS.add_directory,
      [pathlib.Path(spec['dst'])],
      { 'parents': True }
    )
  else: raise NotImplementedError(spec['kind'])

def _assemble_pkg_ops(
  frame: _StackFrame,
  OPS: PkgOps,
) -> list[tuple[Callable, list, dict]]:
  assert frame['kind'] == 'pkg'
  ops = []
  if 'init' in frame['spec']: ops.append(_file_op_factory(frame['spec']['init'], OPS))
  if 'main' in frame['spec']: ops.append(_file_op_factory(frame['spec']['main'], OPS))
  if 'files' in frame['spec']: ops.extend([
    _file_op_factory(file, OPS)
    for file in frame['spec']['files']
  ])
  return ops

def _assemble_module_ops(
  frame: _StackFrame,
  OPS: PkgOps,
) -> list[tuple[Callable, list, dict]]:
  assert frame['kind'] == 'module'
  ops = []
  if 'module' in frame['spec']: ops.append(_file_op_factory(frame['spec']['module'], OPS))
  if 'files' in frame['spec']: ops.extend([
    _file_op_factory(file, OPS)
    for file in frame['spec']['files']
  ])
  return ops

def _merge_requirements(
  requirements: list[RequirementSpec],
) -> dict[str, RequirementSpec]:
  merged = {}
  for req in requirements:
    pkg_name = req['name']    
    if pkg_name in merged:
      if sorted(tuple(req)) != sorted(tuple(merged[pkg_name])): raise NotImplementedError(f"Duplicate Requirement: {req['pkg']}")
    else:
      merged[pkg_name] = req
  return merged

def assemble(
  build_spec: BuildSpec,
  srcdir: pathlib.Path,
  workdir: pathlib.Path,
) -> AssemblyOutput:
  """Assemble a Utility Package into the Package Directory from the Source Directory based on the passed Build Spec
  
  Args:
    build_spec (BuildSpec): The Build Specification to use
    srcdir (pathlib.Path): The Source Directory to build the package from
    workdir (pathlib.Path): The Working Directory to build the package in
  
  Returns:
    pathlib.Path: The Path to the Package's Directory
  
  """
  
  if not (workdir.exists() and workdir.is_dir()): raise BuildError(f"Working Directory does not exist: {workdir}")
  if not (srcdir.exists() and srcdir.is_dir()): raise BuildError(f"Source Directory does not exist: {srcdir}")

  (pkgdir := workdir / build_spec['name']).mkdir(mode=0o755, exist_ok=False, parents=False)

  # Create an in memory tree of all the files to be included in the package
  OPS = PkgOps(workdir=srcdir, pkgdir=pkgdir)
  collected_requirements: list[RequirementSpec] = []
  def _parse_requirements(*args, **kwargs): collected_requirements.extend(OPS.parse_requirements_file(*args, **kwargs))
  pkg_tree: _TreeNode = _TreeNode.factory('pkg', build_spec['name'], build_spec['pkg'])
  walk_stack: deque[_StackFrame] = deque([{ 'kind': 'pkg', 'name': build_spec['name'], 'ops': [], 'spec': build_spec['pkg'], 'parent': pkg_tree }])
  while len(walk_stack) > 0 :
    frame: _StackFrame = walk_stack.popleft()
    if frame['kind'] == 'pkg': frame['parent']['ops'].extend(_assemble_pkg_ops(frame, OPS))
    elif frame['kind'] == 'module': frame['parent']['ops'].extend(_assemble_module_ops(frame, OPS))
    else: raise NotImplementedError(frame['kind'])

    if 'requirements' in frame['spec']:
      frame['parent']['ops'].append((
        _parse_requirements,
        [pathlib.Path(frame['spec']['requirements'])],
        {}
      ))

    for pkg_name, pkg_spec in frame['spec'].get('pkgs', {}).items():
      if pkg_name in frame['parent']['children']: raise BuildError(f"Duplicate Subpackage Name: {pkg_name}")
      _node = _TreeNode.factory('pkg', pkg_name, pkg_spec)
      frame['parent']['children'][pkg_name] = _node
      walk_stack.append({ 'kind': 'pkg', 'name': pkg_name, 'spec': pkg_spec, 'parent': _node })
    for module_name, module_spec in frame['spec'].get('modules', {}).items():
      if module_name in frame['parent']['children']: raise BuildError(f"Duplicate Module Name: {module_name}")
      _node = _TreeNode.factory('module', module_name, module_spec)
      frame['parent']['children'][module_name] = _node
      walk_stack.append({ 'kind': 'module', 'name': module_name, 'spec': module_spec, 'parent': _node })

  # Execute the operations on the package directory
  for op, args, kwargs in itertools.chain.from_iterable(node['ops'] for node in _TreeNode.walk_tree(pkg_tree)):
    logger.debug(f"Executing Operation: {op.__name__}({args}, {kwargs})")
    try: op(*args, **kwargs)
    except Exception as e: raise BuildError(f"Error while executing Operation: {op.__name__}({args}, {kwargs})") from e
    logger.debug(f"Operation Completed: {op.__name__}({args}, {kwargs})")
  
  # Merge all the requirements files
  merged_requirements: dict[str, RequirementSpec] = _merge_requirements(collected_requirements)
  if merged_requirements:
    with (requirements_file := workdir / 'requirements.txt').open('w+') as f:
      for req in merged_requirements.values():
        f.write(RequirementSpec.render_requirement_line(req))
        f.write('\n')
  
  return {k: v for k, v in {
    'pkgdir': pkgdir.as_posix(),
    'requirements': requirements_file.as_posix() if merged_requirements else None
  }.items() if v}
      
