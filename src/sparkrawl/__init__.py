from typing import TypeVar, Callable, Dict, Any, Iterable, Tuple
from dataclasses import dataclass
import pyspark

Parent = TypeVar("Parent")
Child = TypeVar("TChild")
Attribs = Dict[str, Any]
BranchFn = Callable[[Parent], Iterable[Tuple[Child, Attribs]]]


@dataclass(frozen=True)
class with_attribs:
    branch_fn: BranchFn

    def __call__(self, item: Tuple[Parent, Attribs]):
        parent, record = item
        for child, record_ in self.branch_fn(parent):
            yield child, {**record, **record_}


@dataclass(frozen=True)
class no_attribs:
    flat_map_fn: Callable[[Parent], Iterable[Child]]

    def __call__(self, tup: Tuple[Parent, Attribs]):
        parent, attribs = tup
        for child in self.flat_map_fn(parent):
            yield child, {**attribs}


@dataclass(frozen=True)
class only_attribs:
    flat_map_fn: Callable[[Parent], Iterable[Attribs]]

    def __call__(self, tup: Tuple[Parent, Attribs]):
        path, attribs = tup
        for attribs_ in self.flat_map_fn(path):
            yield {**attribs_, **attribs}
