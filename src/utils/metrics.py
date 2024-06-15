import time
from typing import Any, Callable, Coroutine, TypeVar

T = TypeVar("T")

_time_unit_cutoff: dict[str, int] = { "ns": 1, "us": int(1e3), "ms": int(1e6), "s": int(1e9), "m": int(60e9), "h": int(3600e9) }
_time_unit_to_order: dict[str, int] = { u: order for order, u in enumerate(_time_unit_cutoff.keys(), 1) }
_time_units_by_order: dict[int, str] = { order: u for u, order in _time_unit_to_order.items() }
def render_duration(ns: int) -> str:
  """Pretty Print a time in Nanoseconds"""
  _dur = abs(ns)
  if _dur < _time_unit_cutoff["us"]: _order = _time_unit_to_order["ns"]
  elif _dur < _time_unit_cutoff["ms"]: _order = _time_unit_to_order["us"]
  elif _dur < _time_unit_cutoff["s"]: _order = _time_unit_to_order["ms"]
  elif _dur < _time_unit_cutoff["m"]: _order = _time_unit_to_order["s"]
  elif _dur < _time_unit_cutoff["h"]: _order = _time_unit_to_order["m"]
  else: _order = _time_unit_to_order["h"]
  _render = ""
  # Iterate through the orders of magnitude (from greatest to least)
  for lvl in range(_order, 0, -1):
    unit = _time_units_by_order[lvl]
    ns_per_unit = _time_unit_cutoff[unit]
    quot, _dur = divmod(_dur, ns_per_unit)
    if quot == 0: continue
    _render += f"{int(quot)}{unit} "
  return ("- " + _render if ns < 0 else _render).strip()

def timeit(func: Callable[[], T]) -> tuple[int, T]:
  """Time a function and return the time (in ns) and result.
  
  Pass the function ready to go; wrap it in a lamba or use functools.partial if you need to pass arguments.

  #### Example
  
  ```python
  def foo(bar: int) -> str:
    time.sleep(bar) # Do stuff
    return "baz"
  
  duration_ns, (result, ) = timeit(lambda: foo(5))
  ```
  """
  start = time.monotonic_ns()
  result = func()
  end = time.monotonic_ns()
  return (end - start, result)

async def atimeit(coro: Coroutine[None, None, T]) -> tuple[int, T]:
  """Time a coroutine and return the time (in ns) and result.
  
  Pass the Coroutine ready to go; call the coroutine function but don't await it.

  #### Example

  ```python
  async def foo(bar: int) -> str:
    await asyncio.sleep(bar) # Do stuff
    return "baz"
  
  duration_ns, (result, ) = await atimeit(foo(5))
  ```
  """
  start = time.monotonic_ns()
  result = await coro
  end = time.monotonic_ns()
  return (end - start, result)