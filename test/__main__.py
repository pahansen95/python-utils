from __future__ import annotations
import os, sys, pathlib, asyncio
from typing import TypedDict
from loguru import logger

from utils.testing import test_registry, TestCode, TestError, TestResult

group_test_results_t = tuple[tuple[TestError, ...], dict[TestCode, dict[str, TestResult]]]
async def run_test_group(name: str) -> group_test_results_t:
  logger.info(f"Running Test Group {name}")
  group_tests = test_registry.get_group_tests(name)
  test_order: tuple[list] = tuple(sorted(group_tests.keys()))
  _test_results: list[TestResult | Exception] = await asyncio.gather(*[group_tests[name] for name in test_order], return_exceptions=True)
  test_errors: list[TestError] = []
  test_results: dict[TestCode, dict[str, TestResult]] = { TestCode.PASS: {}, TestCode.FAIL: {}, TestCode.SKIP: {} }

  for _name, result in zip(test_order, _test_results):
    if isinstance(result, Exception): test_errors.append(TestError(_name, result))
    elif not isinstance(result, TestResult): raise TypeError(f"Expected '{name}::{_name}' to return TestResult or Exception, got {type(result).__name__}")
    else: test_results[result.code][_name] = result

  return (test_errors, test_results)

async def cmd_run_tests(
  groups: list[str] = None,
) -> int:
  logger.info("Running Test Suite")

  # Run Tests by Group
  _error = False
  group_names: tuple[str, ...] = tuple(sorted(g for g in test_registry.groups if groups is None or g in groups))
  if len(group_names) < 1: raise RuntimeError("No Test Groups Registered")
  # Log the Tests registered with the Test Registry
  for group_name in group_names:
    test_names = list(sorted(test_registry.tests[group_name].keys()))
    logger.info(f"Test Group '{group_name}' has {len(test_names)} Tests Registered: {', '.join(test_names)}")
  # Run the Tests
  group_test_results: list[group_test_results_t] = await asyncio.gather(*[run_test_group(name) for name in group_names])
  
  for group_name, (test_errors, test_results) in zip(group_names, group_test_results):
    if len(test_errors) > 0: _error = True
    for error in test_errors: logger.opt(exception=error.error).critical(f"'{group_name}::{error.name}' encountered an Error: {error.error}")
    for test_name, test_result in test_results[TestCode.PASS].items(): logger.success(f"'{group_name}::{test_name}': {test_result}")
    for test_name, test_result in test_results[TestCode.FAIL].items(): logger.warning(f"'{group_name}::{test_name}': {test_result}")
    for test_name, test_result in test_results[TestCode.SKIP].items(): logger.info(f"'{group_name}::{test_name}': {test_result}")

  if _error: logger.critical("!!! TEST SUITE COMPLETED WITH RUNTIME ERRORS !!!")
  else: logger.success("Tests Completed Successfully") # TODO: Change Level based on test results
  return 1 if _error else 0

def main(args: tuple[str, ...], kwargs: CLI_KWARGS) -> int:
  logger.trace(f"Starting main function with arguments: {args}\nKeywords: {kwargs}")
  if len(args) < 1: raise CLIError("No subcommand provided")
  subcmd = args[0]
  if subcmd == 'run':
    groups = args[1:] if len(args) > 1 else None
    return asyncio.run(cmd_run_tests(groups=groups))
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
  utils: str
  """The Utils Package Directory"""

def parse_argv(argv: list[str], env: dict[str, str]) -> tuple[tuple[str, ...], CLI_KWARGS]:
  def _default_utils_dir() -> str: return (pathlib.Path(env['CI_PROJECT_DIR']) / 'src').as_posix() if 'CI_PROJECT_DIR' in env else (pathlib.Path(os.getcwd()) / 'src/utils').as_posix()
  args = []
  kwargs = {
    "log": env.get('LOG_LEVEL', 'INFO'),
    "utils": env.get('UTILS_DIR', _default_utils_dir()),
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