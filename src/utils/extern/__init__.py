import enum, sys

class SystemBackend(enum.Enum):
  """The Current System Backend. Is used to switch between certain implementations."""
  PYTHON = "python"
  """A Backend using Python as the implementation."""
  LINUX = "linux"
  """A Backend using some Linux API or Library as the implementation."""
  WINDOWS = "windows"
  """A Backend using some Windows API or Library as the implementation."""
  MACOS = "macos"
  """A Backend using some MacOS API or Library as the implementation."""

  @staticmethod
  def get_current_backend() -> "SystemBackend":
    if sys.platform.lower().startswith("linux"): return SystemBackend.LINUX
    elif sys.platform.lower().startswith("win"): return SystemBackend.WINDOWS
    elif sys.platform.lower().startswith("darwin"): return SystemBackend.MACOS
    else: return SystemBackend.PYTHON

active_backend = SystemBackend.get_current_backend()
