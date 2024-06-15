from __future__ import annotations
import os, sys, orjson, yaml, time, pathlib
from typing import TypedDict, NotRequired, Any, Literal
from collections.abc import Generator
from loguru import logger

### Local Imports
from . import pkg, artifact, requirements
###

def _generate_utils_pkg_spec(
  cfg: Config,
  workdir: pathlib.Path
) -> int:
  """Generates a Package Spec given a GenerationConfig"""
  pkg_dir = workdir / cfg['spec']['source'].lstrip('/')
  logger.debug(f"Checking for the declared Package Directory: {pkg_dir.as_posix()}")
  if not (pkg_dir.exists() and pkg_dir.resolve().is_dir()): raise CLIError(f"Package Source directory '{pkg_dir}' does not exist")
  pkg_spec = pkg.assemble_spec(pkg_dir, cfg['spec']['build']['include'], cfg['spec']['build']['exclude'])
  sys.stdout.buffer.write(orjson.dumps(pkg_spec, option=orjson.OPT_APPEND_NEWLINE))

def _build_utils_pkg(
  pkg_spec: pkg.PkgSpec,
  cfg: Config,
  workdir: pathlib.Path
) -> int:
  pkg_dir = workdir / cfg['spec']['source'].lstrip('/')
  data_dir = pkg_dir.parent
  logger.debug(f"Checking for the declared Package Directory: {pkg_dir.as_posix()}")
  if not (pkg_dir.exists() and pkg_dir.resolve().is_dir()): raise CLIError(f"Package Source directory '{pkg_dir}' does not exist")

  logger.info('Assembling the Utils Package into an Artifact')
  pkg_files = pkg.list_pkg_files(pkg_dir, pkg_spec)
  logger.debug(f"Found Package Files...\n{orjson.dumps(sorted(p.relative_to(workdir).as_posix() for p in pkg_files), option=orjson.OPT_INDENT_2).decode()}")
  pkg_reqs = pkg.merge_requirements(pkg_spec)
  logger.debug(f"Merged Requirements...\n{orjson.dumps(pkg_reqs, option=orjson.OPT_INDENT_2).decode()}")
  (req_file := pkg_dir.parent / 'requirements.txt').write_text('\n'.join(
    requirements.RequirementSpec.render_requirement_line(req)
    for _, req in sorted(
      pkg_reqs.items(),
      key=lambda item: item[0],
    )
  ))

  artifact_files: list[str] = [p.relative_to(data_dir).as_posix() for p in pkg_files]
  if len(artifact_files) <= 0: raise CLIError("The Package Spec didn't generate any files to package; double check your include/exclude filters in the Config")
  artifact_files.insert(0, req_file.relative_to(data_dir).as_posix())
  logger.debug(f"Will assemble the following Files...{orjson.dumps(artifact_files, option=orjson.OPT_INDENT_2).decode()}")
  
  if cfg['spec']['artifact']['kind'].startswith('tar'):
    artifact_spec: artifact.TarSpec = cfg['spec']['artifact'] | {
      'include': artifact_files,
    }
  else: raise NotImplementedError(cfg['spec']['artifact']['kind'])
  artifact.validate_artifact_spec(artifact_spec)
  logger.debug("Creating the Artifact")
  assembly = artifact.create(artifact_spec, data_dir, workdir)
  logger.success(f"Artifact is located at {assembly['artifact']}")

  sys.stdout.buffer.write(orjson.dumps(assembly, option=orjson.OPT_APPEND_NEWLINE))
  return 0

class Config(TypedDict):
  metadata: dict[str, Any]
  spec: Config.Spec

  @staticmethod
  def validate(cfg: Config):
    logger.debug("Validating the Configuration")
    if not isinstance(cfg, dict): raise CLIError("Configuration must be a dictionary")
    if 'metadata' not in cfg: raise CLIError("Configuration must contain a 'metadata' section")
    if not isinstance(cfg['metadata'], dict): raise CLIError("Metadata section must be a dictionary")
    if 'spec' not in cfg: raise CLIError("Configuration must contain a 'spec' section")
    if not isinstance(cfg['spec'], dict): raise CLIError("Spec section must be a dictionary")
    Config.Spec.validate(cfg['spec'])

  class Spec(TypedDict):
    source: str
    """Path to the Source Directory containing the Package; if relative it is taken relative to the working directory"""
    artifact: Config.PartialArtifactSpec
    build: Config.BuildSpec
    """The Build Spec"""
    
    @staticmethod
    def validate(spec: Config.Spec):
      logger.debug("Validating the config.spec")
      if not isinstance(spec, dict): raise CLIError("Spec must be a dictionary")
      if 'build' not in spec: raise CLIError("Spec must contain a 'build' section")
      if not isinstance(spec['build'], dict): raise CLIError("Build section must be a dictionary")
      if 'source' not in spec: raise CLIError("Spec must contain a 'source' section")
      if not isinstance(spec['source'], str): raise CLIError("Source section must be a string")
      Config.BuildSpec.validate(spec['build'])
      Config.PartialArtifactSpec.validate(spec['artifact'])
  
  class PartialArtifactSpec(TypedDict):
    kind: Literal['tar.gz', 'tar.xz', 'tar.bz2', 'wheel']
    dst: str

    @staticmethod
    def validate(spec: Config.PartialArtifactSpec):
      logger.debug("Validating config.spec.artifact")
      if not isinstance(spec, dict): raise CLIError("Artifact Spec must be a dictionary")
      if 'kind' not in spec: raise CLIError("Artifact Spec must contain a 'kind' key")
      if spec['kind'] not in ('tar.gz', 'tar.xz', 'tar.bz2', 'wheel'): raise CLIError("Artifact Spec 'kind' must be one of 'tar.gz', 'tar.xz', 'tar.bz2', 'wheel")
      if 'dst' not in spec: raise CLIError("Artifact Spec must contain a 'dst' key")

  class BuildSpec(TypedDict):
    include: list[str]
    """List of packages to include as Posix Shell Glob Patterns (NOTE: Package Seperators `.` are replaced with path seperators `/`)"""
    exclude: NotRequired[list[str]]
    """List of packages to exclude as Posix Shell Glob Patterns (NOTE: Package Seperators `.` are replaced with path seperators `/`)"""

    @staticmethod
    def validate(spec: Config.BuildSpec):
      logger.debug("Validating config.spec.build")
      if not isinstance(spec, dict): raise CLIError("Build Spec must be a dictionary")
      if 'include' not in spec: raise CLIError("Build Spec must contain an 'include' key")
      if not isinstance(spec['include'], list): raise CLIError("Build Spec 'include' must be a list")
      if 'exclude' in spec and not isinstance(spec['exclude'], list): raise CLIError("Build Spec 'exclude' must be a list")

def _load_map(file: str | None, spec: Config | pkg.PkgSpec) -> Config | pkg.PkgSpec:
  if file:
    _file = pathlib.Path(file)
    if not (_file.exists() or _file.is_file()): raise CLIError(f"File '{_file}' does not exist or is not a file")
    if _file.suffix == '.json': _spec = orjson.loads(_file.read_text())
    elif _file.suffix in ('.yaml', '.yml'): _spec = yaml.safe_load(_file.read_text())
    else: raise CLIError(f"Unsupported configuration format: {_file.suffix}")
  else:
    try: _spec = orjson.loads(sys.stdin.read())
    except orjson.JSONDecodeError:
      try: _spec = yaml.safe_load(sys.stdin.read())
      except yaml.YAMLError as e: raise CLIError(f"Failed to parse stdin: {e}")

  spec.validate(_spec)
  return _spec

def main(args: tuple[str, ...], kwargs: CLI_KWARGS) -> int:
  logger.trace(f"Starting main function with arguments: {args}\nKeywords: {kwargs}")
  if len(args) < 1: raise CLIError("No subcommand provided")
  subcmd = args[0]
  if subcmd == 'gen':
    cfg: Config = _load_map(kwargs['config'], Config)
    return _generate_utils_pkg_spec(cfg, pathlib.Path(kwargs['build']))
  elif subcmd == 'build':
    cfg: Config = _load_map(kwargs['config'], Config)
    pkg_spec: pkg.PkgSpec = _load_map(kwargs.get('spec'), pkg.PkgSpec)
    return _build_utils_pkg(pkg_spec, cfg, pathlib.Path(kwargs['build']))
  else:
    raise CLIError(f"Unknown subcommand '{subcmd}'")

class CLIError(RuntimeError): pass

def setup_logging(log_level: str = os.environ.get('LOG_LEVEL', 'INFO')):
  logger.remove()
  logger.add(sys.stderr, level=log_level, enqueue=True, colorize=True)
  logger.trace(f'Log level set to {log_level}')
  import logging
  _log_level = {
    'TRACE': 'DEBUG',
    'DEBUG': 'DEBUG',
    'INFO': 'INFO',
    'WARNING': 'WARNING',
    'SUCCESS': 'ERROR',
    'ERROR': 'ERROR',
    'CRITICAL': 'CRITICAL'
  }[log_level]
  for _handle in (
    ''
  ):
    logger.trace(f'Setting log level for {_handle} to {_log_level}')
    _logger = logging.getLogger(_handle)
    _logger.setLevel(_log_level)
    _logger.addHandler(logging.StreamHandler(sys.stderr))

def finalize_logging():
  logger.complete()

class CLI_KWARGS(TypedDict):
  log: str
  """The Log Level"""
  build: str
  """The Build Directory"""
  config: str
  """The Build Configuration File"""
  spec: NotRequired[str]
  """The Package Specification File; if omitted, it is read from stdin"""

def parse_argv(argv: list[str], env: dict[str, str]) -> tuple[tuple[str, ...], CLI_KWARGS]:
  def _default_build_dir() -> str: return (pathlib.Path(env['CI_PROJECT_DIR']) / '.cache/build').as_posix() if 'CI_PROJECT_DIR' in env else os.getcwd()
  args = []
  kwargs = {
    "log": env.get('LOG_LEVEL', 'INFO'),
    "build": env.get('BUILD_DIR', _default_build_dir()),
    "config": env.get('BUILD_CONFIG', './pkg.yaml'),
  }
  for idx, arg in enumerate(argv):
    if arg == '--':
      logger.trace(f"Found end of arguments at index {idx}")
      args.extend(argv[idx+1:])
      break
    elif arg.startswith('--'):
      logger.trace(f"Found keyword argument: {arg}")
      if '=' in arg: key, value = arg[2:].split('=', 1)
      else: key, value = arg[2:], True
      kwargs[key] = value
    else:
      logger.trace(f"Found positional argument: {arg}")
      args.append(arg)
  return tuple(args), kwargs

if __name__ == '__main__':
  setup_logging()
  _rc = 255
  try:
    logger.trace(f"Arguments: {sys.argv[1:]}\nEnvironment: {os.environ}")
    args, kwargs = parse_argv(sys.argv[1:], os.environ)
    logger.trace(f"Arguments: {args}\nKeywords: {kwargs}")
    setup_logging(kwargs['log']) # Reconfigure logging
    logger.trace(f"Log level set to {kwargs['log']}")
    _rc = main(args, kwargs)
  except CLIError as e:
    logger.error(str(e))
    _rc = 2
  except:
    logger.opt(exception=True).critical('Unhandled exception')
    _rc = 3
  finally:
    finalize_logging()
    sys.stdout.flush()
    sys.stderr.flush()
  exit(_rc)