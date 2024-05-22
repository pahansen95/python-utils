from typing import Any, Iterable, Iterator, Callable, TypeVar
from itertools import chain as _chain, groupby as _groupby
from loguru import logger

K = TypeVar('K')
V = TypeVar('V')

def chain(*iterables: Iterable[V]) -> Iterator[V]:
  return iter(_chain(*iterables))

def chain_from_iter(iterable: Iterable[Iterable[V]]) -> Iterator[V]:
  return iter(_chain.from_iterable(iterable))

def group_by(
  iterable: list[V],
  key: Callable[[V], K],
) -> list[tuple[K, Iterator[V]]]:
  """Group the Items in the Iterable by the Computed Key; unlike `itertools.groupby`, this function does not require the Iterable to already be sorted by the Key"""
  groups: dict[K, list[Iterable[Any]]] = {}
  for group, group_iter in _groupby(iterable, key=key):
    # logger.trace(f"{group=}")
    if group not in groups:
      groups[group] = []
    groups[group].append(list(group_iter))
    # logger.trace(f"{groups[group]=}")
  return list(
    (group, chain_from_iter(items))
    for group, items in groups.items()
  )