from __future__ import annotations
import pathlib, yaml, fnmatch, itertools
from typing import Literal, TypedDict, NotRequired
from collections import deque
from loguru import logger
from . import requirements

class PkgSpec(TypedDict):
  name: str
  kind: Literal['namespace', 'python', 'extern']
  """What Kind of Package is it
  
  - `namespace`: A Folder without a `__init__.py` file, cannot be directly imported but has importable children
  - `python`: A Folder with a `__init__.py` file that can be directly imported
  - `extern`: A superset of a `python` package incorporating external, non-Python source code
  """
  executable: NotRequired[bool]
  metadata: NotRequired[PkgSpec.PkgMetadata]
  requirements: NotRequired[dict[str, requirements.RequirementSpec]]
  data: NotRequired[str]
  modules: NotRequired[list[str]]
  subpkgs: NotRequired[dict[str, PkgSpec]]
  extern: NotRequired[list[str]]
  """External Non-Python Source Files"""

  class PkgMetadata(TypedDict):
    extern: list[Literal['c']]
    """Marks if the package integrates external non-python source files"""

    @staticmethod
    def validate(metadata: PkgSpec.PkgMetadata):
      if not isinstance(metadata, dict): raise ValueError("PkgMetadata must be a dictionary")
      if 'extern' in metadata:
        if not isinstance(metadata['extern'], list): raise ValueError("PkgMetadata 'extern' must be a list")
        if not all(isinstance(e, str) for e in metadata['extern']): raise ValueError("PkgMetadata 'extern' must be a list of strings")

  @staticmethod
  def validate(spec: PkgSpec):
    if not isinstance(spec, dict): raise ValueError("PkgSpec must be a dictionary")
    if 'name' not in spec: raise ValueError("PkgSpec must contain a 'name' key")
    if 'kind' not in spec: raise ValueError("PkgSpec must contain a 'kind' key")
    if spec['kind'] not in ('python', 'extern', 'namespace'): raise ValueError("PkgSpec 'kind' must be one of 'package', 'executable', 'namespace")
    if 'metadata' in spec:
      if not isinstance(spec['metadata'], dict): raise ValueError("PkgSpec 'metadata' must be a dictionary")
      if 'extern' in spec['metadata']:
        if not isinstance(spec['metadata']['extern'], list): raise ValueError("PkgSpec 'metadata.extern' must be a list")
        if not all(isinstance(e, str) for e in spec['metadata']['extern']): raise ValueError("PkgSpec 'metadata.extern' must be a list of strings")
    if 'requirements' in spec:
      if not isinstance(spec['requirements'], dict): raise ValueError("PkgSpec 'requirements' must be a dictionary")
      for k, v in spec['requirements'].items(): requirements.RequirementSpec.validate(v)
    if 'data' in spec:
      if not isinstance(spec['data'], str): raise ValueError("PkgSpec 'data' must be a string")
    if 'modules' in spec:
      if not isinstance(spec['modules'], list): raise ValueError("PkgSpec 'modules' must be a list")
      if not all(isinstance(m, str) for m in spec['modules']): raise ValueError("PkgSpec 'modules' must be a list of strings")
    if 'subpkgs' in spec:
      if not isinstance(spec['subpkgs'], dict): raise ValueError("PkgSpec 'subpkgs' must be a dictionary")
      for k, v in spec['subpkgs'].items(): PkgSpec.validate(v)
    if 'extern' in spec: PkgSpec.PkgMetadata.validate(spec['metadata'])

def filter_pkg(pkg_name: str, include: list[str], exclude: list[str]) -> bool:
  if len(include) <= 0: raise ValueError('At least 1 inclusion Glob Pattern must be passed')
  _pkg_name = pkg_name.replace('.', '/')
  logger.trace(f"Evaluating Pkg Name: {_pkg_name}")
  should_include = any(fnmatch.fnmatch(_pkg_name, pattern) for pattern in include)
  should_exclude = any(fnmatch.fnmatch(_pkg_name, pattern) for pattern in exclude) if exclude else False
  match = should_include and not should_exclude
  if not match: logger.debug(f"EXCLUDE `{_pkg_name}`")
  return match

def parse_pkg(
  pkg_path: pathlib.Path,
  parent_pkg: str,
  include_pkg: list[str],
  exclude_pkg: list[str],
) -> tuple[PkgSpec, list[pathlib.Path]]:
  """Parse a Package Directory
  
  Args:
    path (pathlib.Path): The Path to the Package Directory
    parent_pkg (str): The Parent Package Name
    include_pkg (list[str]): List of Package Glob Names to Include
    exclude_pkg (list[str]): List of Package Glob Names to Exclude
  """
  if parent_pkg: pkg_name = parent_pkg + '.' + pkg_path.name
  else: pkg_name = pkg_path.name
  spec: PkgSpec = {
    'name': pkg_name,
  }
  children = [p.name for p in pkg_path.glob('*')]
  logger.trace("Found Children...\n" + '\n'.join(children))

  # Load Package Metadata
  if '.metadata' in children: spec['metadata'] = yaml.safe_load((pkg_path / '.metadata').read_text())

  # Determine the Python Package Type
  if '__init__.py' in children:
    spec['executable'] = '__main__.py' in children
    if spec.get('metadata', {}).get('extern', []): spec['kind'] = 'extern'
    else: spec['kind'] = 'python'
  else: spec['kind'] = 'namespace'

  # Package Requirements
  if 'requirements.txt' in children: spec['requirements'] = requirements.parse_requirements_file(pkg_path / 'requirements.txt')

  # Bundled Package Data
  if '.data' in children: spec['data'] = (pkg_path / '.data').as_posix()

  # Package Modules
  modules = sorted(list(set(itertools.chain.from_iterable(
    (
      p.stem
      for p in pkg_path.glob(pattern)
      if (
        p.name.count('.') == 1
        and p.stem not in (
          '__init__', '__main__',
        ) and filter_pkg(f"{pkg_name}.{p.stem}", include_pkg, exclude_pkg)
      )
    ) for pattern in (
      '*.py',
      '*.pi',
      '*.pyc',
    )
  ))))
  logger.debug(f"Found Modules...\n" + '\n'.join(modules))
  if modules: spec['modules'] = modules

  # Extern Package Files
  if spec['kind'] == 'extern':
    logger.trace('Searching for External Source Files')
    extern: list[pathlib.Path] = []
    if 'c' in spec['metadata']['extern']:
      logger.trace('Looking for C & C++ Source Files')
      extern.extend(itertools.chain.from_iterable(
        (
          p for p in pkg_path.glob(pattern)
        ) for pattern in (
          'src/**/*.c',
          'src/**/*.cpp',
          'include/**/*.h',
          'include/**/*.hpp',
          'lib/**/*.a',
          'lib/**/*.so',
        )
      ))
    
    if extern:
      spec['extern'] = sorted(
        p.relative_to(pkg_path).as_posix()
        for p in set(extern)
      )
      logger.trace("Found the following External Source Files...\n" + '\n'.join(spec['extern']))
    else: logger.warning(f"Package `{spec['name']}` declares itself as External but contains no non-Python source files")
  
  # Potential Subpackages
  maybe_subpkg = sorted(set(filter(
    lambda p: (
      p.is_dir()
      and p.name.count('.') <= 0
      and (True if spec['kind'] != 'extern' else (
        # The following conditions must hold true when this is an External Package
        p.name not in ('src', 'include', 'lib') # Folders reserved for External C/C++ Modules
      ))
      and p.name not in ('.data', '__pycache__')
      and filter_pkg(f"{pkg_name}.{p.name}", include_pkg, exclude_pkg)
    ),
    pkg_path.iterdir(),
  )))
  logger.debug(f"Found Potential Subpackages...\n" + '\n'.join(map(str, maybe_subpkg)))

  return (spec, maybe_subpkg)

def assemble_spec(pkg_dir: str, include_pkg: list[str], exclude_pkg: list[str]) -> PkgSpec:
  """Assemble the Package Spec from the given package directory

  Args:
    pkg_dir (str): The Path to the Root of the Package's Directory Tree
    pkg_name (str): The Name of the Root Package
    ignore_pkg (list[str]): List of Package Names to Ignore  

  """
  logger.debug(f"Package Inclusion Filters: {include_pkg}")
  logger.debug(f"Package Exclusion Filters: {exclude_pkg}")

  root_dir = pathlib.Path(pkg_dir)
  
  class WalkNode(TypedDict):
    dir: pathlib.Path
    parent: pathlib.Path

  logger.debug(f"Parsing Package at `{root_dir}`")
  root_spec, root_children = parse_pkg(root_dir, '', include_pkg, exclude_pkg) # Parse the Root Directory
  logger.info(f"Found Package `{root_spec['name']}` of kind `{root_spec['kind']}`")
  pkg_specs = {
    root_dir: root_spec,
  }
  walk_stack: deque[WalkNode] = deque([
    {
      'dir': child_dir,
      'parent': root_dir,
    } for child_dir in root_children
  ])
  while walk_stack:
    node = walk_stack.popleft()
    logger.debug(f"Parsing Package at `{node['dir']}`")
    assert node['dir'] not in pkg_specs
    parent_spec = pkg_specs[node['parent']]
    node_spec, children = parse_pkg(node['dir'], parent_spec['name'], include_pkg, exclude_pkg)
    logger.info(f"Found Package `{node_spec['name']}` of kind `{node_spec['kind']}`")
    pkg_specs[node['dir']] = node_spec
    if 'subpkgs' not in parent_spec: parent_spec['subpkgs'] = {}
    parent_spec['subpkgs'][node['dir'].name] = node_spec
    if children: walk_stack.extendleft(
      {
        'dir': child_dir,
        'parent': node['dir'],
      } for child_dir in children
    )  

  return root_spec

def list_pkg_files(pkg_dir: pathlib.Path, pkg_spec: PkgSpec) -> list[pathlib.Path]:
  """List the Package Files for the given Package Spec"""
  class WalkNode(TypedDict):
    pkg_dir: pathlib.Path
    pkg_spec: PkgSpec

  contents: list[pathlib.Path] = []
  walk_stack: list[WalkNode] = [{
    'pkg_dir': pkg_dir,
    'pkg_spec': pkg_spec,
  }]
  while walk_stack:
    node = walk_stack.pop()
    if node['pkg_spec']['kind'] == 'python':
      contents.append(node['pkg_dir'] / '__init__.py')
      if node['pkg_spec']['executable']: contents.append(node['pkg_dir'] / '__main__.py')
    elif node['pkg_spec']['kind'] == 'extern':
      contents.append(node['pkg_dir'] / '__init__.py')
      if node['pkg_spec']['executable']: contents.append(node['pkg_dir'] / '__main__.py')
      contents.extend(node['pkg_dir'] / p for p in node['pkg_spec']['extern'])
    if 'modules' in node['pkg_spec']:
      contents.extend(
        (node['pkg_dir'] / (module.replace('.', '/') + '.py'))
        for module in node['pkg_spec']['modules']
      )
    if 'data' in node['pkg_spec']: contents.extend([p for p in (node['pkg_dir'] / node['pkg_spec']['data']).glob('**/*')])
    if 'subpkgs' in node['pkg_spec']: walk_stack.extend([
      {
        'pkg_dir': node['pkg_dir'] / subpkg_name.rsplit('.', maxsplit=1)[-1],
        'pkg_spec': subpkg_spec,
      } for subpkg_name, subpkg_spec in node['pkg_spec']['subpkgs'].items()
    ])
  return contents

def merge_requirements(pkg_spec: PkgSpec) -> dict[str, requirements.RequirementSpec]:
  """Merges each packages Requirements (if any) into a single table"""
  class WalkNode(TypedDict):
    pkg_spec: PkgSpec
  merged_reqs: dict[str, requirements.RequirementSpec] = {}
  walk_stack: deque[WalkNode] = deque([{
    'pkg_spec': pkg_spec,
  }])
  while walk_stack:
    node = walk_stack.popleft()
    # Add SubPackages to the Walk Stack
    walk_stack.extendleft({ 'pkg_spec': v } for v in node['pkg_spec'].get('subpkgs', {}).values())
    for k, v in node['pkg_spec'].get('requirements', {}).items():
      if k in merged_reqs:
        logger.debug('Found Duplicate Requirement')
        if sorted(tuple(v)) != sorted(tuple(merged_reqs[k])):
          logger.warning('TODO: Merging Requirements is not intelligent; this could break things')
          merged_reqs[k] = merged_reqs[k] | v
        else: logger.debug('Duplicate Requirement is identical; skipping')
      else:
        logger.debug('Adding New Requirement')
        merged_reqs[k] = v

  return merged_reqs
