from __future__ import annotations
from typing import TypedDict, NotRequired, Literal
import pathlib, subprocess, sys

### Local Imports
from . import BuildError
###

class TarSpec(TypedDict):
  kind: Literal['tar.gz', 'tar.xz', 'tar.bz2']
  dst: str
  include: list[str]

  @staticmethod
  def validate(spec: TarSpec):
    if not isinstance(spec, dict): raise BuildError("TarSpec must be a dictionary")
    if 'kind' not in spec: raise BuildError("TarSpec must contain a 'kind' key")
    if spec['kind'] not in ('tar.gz', 'tar.xz', 'tar.bz2'): raise BuildError("TarSpec 'kind' must be one of 'tar.gz', 'tar.xz', 'tar.bz2'")
    if 'dst' not in spec: raise BuildError("TarSpec must contain a 'dst' key")
    if 'include' not in spec: raise BuildError("TarSpec must contain an 'include' key")
    if not isinstance(spec['include'], list): raise BuildError("TarSpec 'include' must be a list")

class PkgSpec(TypedDict):
  kind: Literal['wheel', 'sdist']
  # TODO

  @staticmethod
  def validate(spec: PkgSpec):
    if not isinstance(spec, dict): raise BuildError("PkgSpec must be a dictionary")
    if 'kind' not in spec: raise BuildError("PkgSpec must contain a 'kind' key")
    if spec['kind'] != 'wheel': raise BuildError("PkgSpec 'kind' must be 'wheel'")
    raise NotImplementedError("PkgSpec is not currently implemented")

ArtifactSpec = TarSpec | PkgSpec
def validate_artifact_spec(spec: ArtifactSpec):
  if 'kind' not in spec: raise BuildError("ArtifactSpec must contain a 'kind' key")
  if spec['kind'].startswith('tar'): TarSpec.validate(spec)
  elif spec['kind'] == 'wheel': PkgSpec.validate(spec)
  else: raise NotImplementedError(f"Artifact Kind '{spec['kind']}' is not supported")

class AssemblyOutput(TypedDict):
  artifact: str
  """Path to the Artifact"""

def create(
  spec: ArtifactSpec,
  workdir: pathlib.Path,
) -> AssemblyOutput:
  """Create the Artifact"""
  
  if spec['kind'] in ('tar.gz', 'tar.xz', 'tar.bz2'):
    # Create the Tarball
    (dst := pathlib.Path(spec['dst']))
    if not dst.is_absolute(): dst = workdir / dst
    if dst.exists(): raise BuildError(f"Artifact '{dst}' already exists")
    proc: subprocess.CompletedProcess = subprocess.run([
      'tar',
      '-czvf', dst.as_posix(),
      '-C', workdir.as_posix(),
      *spec['include']
    ], stdin=subprocess.DEVNULL, stdout=sys.stderr, stderr=sys.stderr, check=True)
    if proc.returncode != 0: raise BuildError(f"Failed to create Artifact '{dst}'")
    return { 'artifact': dst.as_posix() }
  else: raise NotImplementedError(f"Artifact Kind '{spec['kind']}' is not supported")