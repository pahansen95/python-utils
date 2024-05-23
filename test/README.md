# Tests

> Test the Utils Package by eating our own dogfood.

Test definitions are found under [tests/](./tests/).

Register all tests into the `TestRegistry` in [__init__.py](./__init__.py)

Run the tests using this standalone `test` module:

```shell

source "${CI_PROJECT_DIR}/.venv/bin/activate"
python3 -m test run

```

Optionally run a subset of tests by specifying names of test groups;

```shell

python3 -m test run group-name-1 group-name-2

```

## Writing tests

First you need to make your source code importable. Generally you can either:

- Install a built package you already prepared
- Setup a directory structure for Python to search for the source code

Below we demonstrate making the source code for the `utils` package importable by setting up a directory structure.

```shell

# This assumes the current project is the `python-utils` project
install -dm0755 "${CI_PROJECT_DIR}/.cache/.pythonpath"
ln -s "${CI_PROJECT_DIR}/src" "${CI_PROJECT_DIR}/.cache/.pythonpath/utils"
export PYTHONPATH="${CI_PROJECT_DIR}/.cache/.pythonpath:${PYTHONPATH}"

```

Then you can write tests using the following template:

> Assumes there exists a Python module/package `foo` under `src/`

```python

import asyncio
from loguru import logger
from typing import Literal, ContextManager, Generator
from contextlib import contextmanager
import tempfile
import os

# The Testing Framework
from utils.testing import TestResult, TestCode

async def test_foobar(*args, **kwargs) -> TestResult:
  # Local Imports from your Package Source
  import foo
  
  assert foo.bar.mirror('foobaz') == 'foobaz'
  assert (await foo.bar.async_mirror('foobaz')) == 'foobaz'

  return TestResult(TestCode.PASS)

# etc...

```
