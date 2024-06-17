"""

Test the Linux inotify(7) integrations

"""
from __future__ import annotations
from utils.testing import TestResult, TestCode
import tempfile, pathlib

__all__ = [
  "test_inotify_c_integrations",
]

async def test_inotify_c_integrations(*args, **kwargs) -> TestResult:
  from utils.linux.inotify import (
    inotify_init, inotify_add_watch, inotify_rm_watch,
    INotifyMask
  )
  from utils.errors import NO_ERROR
  import os
  try:
    ### Create the INotify Instance

    err, fd = inotify_init(INotifyMask.NONE)
    assert err is NO_ERROR, err
    os.set_blocking(fd, False)

    with tempfile.TemporaryFile() as tmpfile:
      ### Watch a File
      err, wd = inotify_add_watch(fd, pathlib.Path(tmpfile), INotifyMask.IN_MODIFY)
      assert err is NO_ERROR, err

      ### TODO Test Reading a Notification Event

      ### Cancel File watch
      err = inotify_rm_watch(fd, wd)
    
    ### Close the Inotify Instance
    os.close(fd)
  except AssertionError as e: return TestResult(TestCode.FAIL, str(e))

  return TestResult(TestCode.PASS)