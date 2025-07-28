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
ExplodeDataFrameFn = Callable[
    [Tuple[Parent, Attribs]],
    Iterable[Attribs]
]


@dataclass(frozen=True)
class explode_with:

    fn: ExplodeFn
    """

    it _only_ makes sense for this to be (key, {attrs}) -> [(key, {attrs})]
    
    """

    def __call__(self, item):
        _parent, attribs = item

        for child, attribs_ in self.fn(item):
            yield child, {**attribs, **attribs_}


def explode_df(
        df: pyspark.sql.DataFrame,
        key_attrib: str,
        explode_fn: ExplodeDataFrameFn,
        *,
        new_cols_schema: pyspark.sql.types.StructType = None,
):

    rdd = (
        df_to_dict_rdd(df)
        .map(extract_key(key_attrib))
        .flatMap(explode_with(pipeline(
            explode_fn,
            for_each(key_by_none)  # TODO: For performance we'd just write a tailored variant of explode_with.
        )))
    ).values()

    schema_minus_key = remove_schema_field_by_name(df.schema, key_attrib)

    if new_cols_schema is None:
        infer_schema = None  # infer
    else:
        infer_schema = augment_schema(
            schema_minus_key,
            new_cols_schema
        )

    df_ = dict_rdd_to_df(rdd, infer_schema)

    if new_cols_schema is None:
        # Override inferences for old columns.
        df_ = df_.rdd.toDF(
            override_schema(df_.schema, schema_minus_key)
        )

    return df_


def df_to_dict_rdd(
        df: pyspark.sql.DataFrame
) -> pyspark.RDD[Attribs]:
    return df.rdd.map(lambda row: row.asDict())


def dict_rdd_to_df(
        rdd: pyspark.RDD[Attribs],
        schema: Optional[pyspark.sql.types.StructType] = None
) -> pyspark.sql.DataFrame:
    if schema is None:
        """
        
        TODO: Do we need better, cheaper, faster handling here?
        (For example: see unit test with attribute ordering issue.)

        """
        def row_factory(attribs):
            return pyspark.Row(**attribs)

    else:

        def row_factory(attribs):
            ordered = {name: attribs[name] for name in schema.fieldNames()}
            return pyspark.Row(**ordered)

    return (
        rdd
        .map(row_factory)
        .toDF(schema)
    )


def remove_schema_field_by_name(
        schema: pyspark.sql.types.StructType,
        name: str,
):
    return schema.__class__([
        f
        for f in schema.fields
        if f.name != name
    ])


def augment_schema(
        schema: pyspark.sql.types.StructType,
        new_cols_schema: pyspark.sql.types.StructType,
):
    fields, field_set = [], set()
    for k, f in enumerate(schema.fields):
        fields.append(f)
        field_set.add(f.name)

    for f in new_cols_schema.fields:
        if f in field_set:
            raise ValueError("A new column '{f.name}' has the same name as an old column.")
        fields.append(f)

    return schema.__class__(fields)


def override_schema(
        schema: pyspark.sql.types.StructType,
        overrides: pyspark.sql.types.StructType,
):
    fields, field_map = [], {}
    for k, f in enumerate(schema.fields):
        fields.append(f)
        field_map[f.name] = k

    for f in overrides.fields:
        fields[field_map[f.name]] = f

    return schema.__class__(fields)


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


def pipeline(*fn_seq):

    def fn(item):
        for fn_ in fn_seq:
            item = fn_(item)
        return item

    return fn


def for_each(fn):

    def fn_(it):
        yield from (
            fn(i) for i in it
        )

    return fn_


def map_key(fn):

    def fn_(item):
        key, value = item
        return fn(key), value

    return fn_


def key_only(item):
    key, value = item
    return key


def key_by_none(value):
    return None, value


def with_attribs(lambdict: Optional[Dict[str, Callable]] = None, **kwargs):

    if lambdict is None:
        lambdict = kwargs
    else:
        lambdict = {**lambdict, **kwargs}

    def fn(key):
        return key, {
            attrib: fn_(key)
            for attrib, fn_ in lambdict.items()
        }

    return fn


empty_attribs = with_attribs()


def dictwrap(key):

    def fn(value):
        return {key: value}

    return fn
