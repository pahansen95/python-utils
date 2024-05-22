"""Errors (not Exceptions)"""

from typing import Protocol, runtime_checkable
import enum

NO_ERROR_T = type('NO_ERROR', (), {})
NO_ERROR = NO_ERROR_T()

@runtime_checkable
class Error(Protocol):
  """A Error"""

  kind: enum.Enum
  """The Kind of Error"""
  message: str
  """A Human Readable description about the Error that is helpful"""

  def __str__(self) -> str:
    """Pretty Print the Error for logging"""
    return f"{type(self).__name__}({self.kind.name}): {self.message}"
