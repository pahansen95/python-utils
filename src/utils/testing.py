import enum
from dataclasses import dataclass, field, KW_ONLY
from typing import Callable, Coroutine, Any

@dataclass
class TestError:
  name: str
  error: Exception

class TestCode(enum.Enum):
  PASS = enum.auto()
  FAIL = enum.auto()
  SKIP = enum.auto()

@dataclass(frozen=True)
class TestResult:
  code: TestCode
  """The Result of the Test."""
  msg: str | None = None
  """An Optional Descriptive Message associated w/ the state of the Test's result. ex. A Reason for failure."""

  def __str__(self) -> str:
    if self.msg is None: return f"Test {self.code.name}"
    return f"Test {self.code.name}: {self.msg}"

@dataclass
class TestRegistry:
  tests: dict[str, dict[str, Callable[..., Coroutine[Any, Any, TestResult]]]] = field(default_factory=dict)

  @property
  def groups(self) -> tuple[str]:
    """The Registered Test Groups."""
    return tuple(self.tests.keys())

  def register(self, group_name: str, name: str, fn: Callable[..., Coroutine[Any, Any, TestResult]]) -> None:
    """Register a Test in a Group."""
    if group_name not in self.tests: self.tests[group_name] = {}
    if name in self.tests[group_name]: raise ValueError(f"Test {name} is already registered")
    self.tests[group_name][name] = fn
  
  def deregister(self, group_name: str, name: str) -> None:
    """Deregister a Test from a Group."""
    if group_name not in self.tests: raise ValueError(f"Group {group_name} does not exist")
    if name not in self.tests[group_name]: raise ValueError(f"Test {name} is not registered")
    del self.tests[group_name][name]
    if len(self.tests[group_name]) == 0: del self.tests[group_name]
  
  def get_group_tests(self, group_name: str, *args, **kwargs) -> dict[str, Coroutine[Any, Any, TestResult]]:
    """Get the Tests in each Group."""
    if group_name not in self.tests: raise ValueError(f"Group {group_name} does not exist")
    return { name: fn(*args, **kwargs) for name, fn in self.tests[group_name].items() }

test_registry = TestRegistry()
"""The Shared Test Registry."""
