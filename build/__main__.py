from __future__ import annotations
import os, sys, orjson, yaml, time, pathlib
from typing import TypedDict, NotRequired, Any, Literal
from collections.abc import Generator
from loguru import logger

### Local Imports
from . import pkg, artifact
###


def _build_util_pkg(
  cfg: Config,
  workdir: pathlib.Path
) -> int:
  srcdir = pathlib.Path(cfg['spec']['source'])
  if not (srcdir.exists() and srcdir.is_dir()):
    raise CLIError(f"Package Source directory '{srcdir}' does not exist")
  logger.info(f"Building package {cfg['spec']['build']['name']} from {srcdir.as_posix()}")
  assembly: dict[str, Any] = pkg.assemble(
    build_spec=cfg['spec']['build'],
    srcdir=srcdir,
    workdir=workdir,
  )
  logger.debug(f"{assembly=}")
  logger.success(f"Package {cfg['spec']['build']['name']} built under {workdir.as_posix()}")

  artifact_spec: artifact.TarSpec = cfg['spec']['artifact'] | {
    'include': [
      pathlib.Path(assembly[k]).relative_to(workdir).as_posix()
      for k in ('pkgdir', 'requirements') if k in assembly
    ]
  }
  logger.debug(f"{artifact_spec=}")
  logger.info(f"Creating Artifact at {artifact_spec['dst']}")
  assembly |= artifact.create(
    spec=artifact_spec,
    workdir=workdir,
  )
  logger.debug(f"{assembly=}")
  logger.success(f"Artifact created at {assembly['artifact']}")

  sys.stdout.buffer.write(orjson.dumps(assembly, option=orjson.OPT_APPEND_NEWLINE))
  return 0

class Config(TypedDict):
  metadata: dict[str, Any]
  spec: dict

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
    build: pkg.BuildSpec
    """The Build Spec"""
    
    @staticmethod
    def validate(spec: Config.Spec):
      logger.debug("Validating the Configuration Spec")
      if not isinstance(spec, dict): raise CLIError("Spec must be a dictionary")
      if 'build' not in spec: raise CLIError("Spec must contain a 'build' section")
      if not isinstance(spec['build'], dict): raise CLIError("Build section must be a dictionary")
      if 'source' not in spec: raise CLIError("Spec must contain a 'source' section")
      if not isinstance(spec['source'], str): raise CLIError("Source section must be a string")
      pkg.BuildSpec.validate(spec['build'])
      Config.PartialArtifactSpec.validate(spec['artifact'])
  
  class PartialArtifactSpec(TypedDict):
    kind: Literal['tar.gz', 'tar.xz', 'tar.bz2', 'wheel']
    dst: str

    @staticmethod
    def validate(spec: Config.PartialArtifactSpec):
      if not isinstance(spec, dict): raise CLIError("Artifact Spec must be a dictionary")
      if 'kind' not in spec: raise CLIError("Artifact Spec must contain a 'kind' key")
      if spec['kind'] not in ('tar.gz', 'tar.xz', 'tar.bz2', 'wheel'): raise CLIError("Artifact Spec 'kind' must be one of 'tar.gz', 'tar.xz', 'tar.bz2', 'wheel")
      if 'dst' not in spec: raise CLIError("Artifact Spec must contain a 'dst' key")

  @staticmethod
  def load(file: pathlib.Path) -> Config:
    if file.suffix == '.json': return orjson.loads(file.read_text())
    elif file.suffix in ('.yaml', '.yml'): return yaml.safe_load(file.read_text())
    else: raise CLIError(f"Unsupported configuration format: {file.suffix}")

def _load_config(file: str) -> Config:
  cfg_file = pathlib.Path(file)
  if not (cfg_file.exists() or cfg_file.is_file()): raise CLIError(f"Configuration file '{cfg_file}' does not exist or is not a file")
  cfg = Config.load(cfg_file)
  Config.validate(cfg)
  return cfg

def main(args: tuple[str, ...], kwargs: CLI_KWARGS) -> int:
  logger.trace(f"Starting main function with arguments: {args}\nKeywords: {kwargs}")
  if len(args) < 1: raise CLIError("No subcommand provided")
  subcmd = args[0]
  if subcmd == 'pkg':
    cfg = _load_config(kwargs['config'])
    return _build_util_pkg(cfg, pathlib.Path(kwargs['build']))
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