import dataclasses
from typing import TypeVar, Callable, Dict, Any, Iterable, Tuple, Optional
from dataclasses import dataclass
import pyspark
from abc import ABC, abstractmethod

from sparkrawl.common import singleton

Parent = TypeVar("Parent")
Child = TypeVar("TChild")
Attribs = Dict[str, Any]
BranchFn = Callable[[Parent], Iterable[Tuple[Child, Attribs]]]
ExplodeFn = Callable[
    [Tuple[Parent, Attribs]],
    Iterable[Tuple[Child, Attribs]]
]


class ExplodeType(ABC):

    @abstractmethod
    def create_input(self, item):
        ...

    @abstractmethod
    def create_output(self, out):
        ...


@singleton
class KEY_ONLY(ExplodeType):

    def create_input(self, item):
        key, _ = item
        return key

    def create_output(self, key):
        return key, {}  # no attributes


@singleton
class ATTRS_ONLY(ExplodeType):

    def create_input(self, item):
        _, attrs = item
        return attrs

    def create_output(self, attrs):
        return None, attrs  # no key


@singleton
class KEY_ATTRS(ExplodeType):

    def create_input(self, item):
        return item

    def create_output(self, item):
        return item


@dataclass(frozen=True)
class explodeWith:
    fn: Callable
    in_spec: ExplodeType = KEY_ONLY
    out_spec: ExplodeType = KEY_ATTRS
    drop_key: Optional[bool] = None

    @property
    def will_drop_key(self):  # Resolve drop_key intent.

        if self.drop_key is None:
            return self.out_spec == ATTRS_ONLY

        return self.drop_key

    def __call__(self, item):

        if self.will_drop_key:
            delegate = dataclasses.replace(
                self,
                drop_key=False
            )
            for child, attribs_ in delegate(item):
                yield attribs_

        else:
            _parent, attribs = item

            for out in self.fn(
                self.in_spec.create_input(item)
            ):
                child, attribs_ = self.out_spec.create_output(out)
                yield child, {**attribs, **attribs_}


def explode_df(
        df: pyspark.sql.DataFrame,
        parent_attrib: str,
        explode_fn: ExplodeFn,
        child_attrib: str,
):
    """

    TODO: Validate combination of explodeFn and child_attrib.
    TODO: Need to handle any existing schema.

    """
    return dict_rdd_to_df(
        df_to_dict_rdd(df)
        .map(extract_key(parent_attrib))
        .flatMap(explode_fn)
        .map(inject_key(child_attrib))
    )


def df_to_dict_rdd(
        df: pyspark.sql.DataFrame
) -> pyspark.RDD[Attribs]:
    return df.rdd.map(lambda row: row.asDict())


def dict_rdd_to_df(
        rdd: pyspark.RDD[Attribs]
) -> pyspark.sql.DataFrame:
    return rdd.map(lambda attribs: pyspark.Row(**attribs)).toDF()


@dataclass(frozen=True)
class extract_key:
    key_attrib: str

    def __call__(self, record):
        key = record.pop(self.key_attrib)
        return key, record


def inject_key(key_attrib: str, converter: type = None):

    def map_fn(tup):
        key, record = tup
        record[key_attrib] = converter(key) if converter else key
        return record

    return map_fn
