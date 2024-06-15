"""Errors (not Exceptions)"""
from __future__ import annotations
from typing import TypedDict

NO_ERROR_T = type('NO_ERROR', (), {})
NO_ERROR = NO_ERROR_T()

class Error(TypedDict):
  """An Error"""

  kind: str
  """The Kind of Error"""
  message: str
  """A Human Readable description about the Error that is helpful"""

  @staticmethod
  def render(error: Error) -> str: return f"{error['kind']}: {error['message']}"
