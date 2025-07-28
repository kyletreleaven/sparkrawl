import json

import pysparkling.sql.session

from ospath import *
import testcases
from testcases import *
from fileio import read_jsonlines
from sparkrawl import *

import pandas as pd
import pyspark
import pytest

import os
import sys

USE_SPARK = False


@pytest.fixture
def example_data(tmp_path):
    data_path = tmp_path / "data"
    populate_directory(data_path)
    return data_path


def test_nested_loop(example_data):


    def records():
        for d1 in example_data.iterdir():
            if not d1.is_dir():
                continue

            color = d1.name

            for d2 in d1.iterdir():
                if not d2.is_dir():
                    continue

                year = int(d2.name)

                for d3 in d2.iterdir():
                    if not d3.is_dir():
                        continue

                    attr3, value3 = d3.name.split("=", maxsplit=1)  # parquet partitioning

                    for f in d3.iterdir():
                        if not f.is_file():
                            continue

                        for record in read_jsonlines(f):
                            record.update(color=color, year=year, **{attr3: value3})
                            yield record

    df = pd.DataFrame.from_records(records())
    # assert False, df
    assert set(df.columns) == set(["color", "year", "size", "x"])


def test_pipeline(example_data):

    def parquet_attribs(path: Path):
        attr, value = path.name.split("=", maxsplit=1)
        return {attr: value}

    stage1 = pipeline(key_only, iterate_dirs, for_each(with_attribs(color=lambda path: path.name)))
    stage2 = pipeline(key_only, iterate_dirs, for_each(with_attribs(year=lambda path: int(path.name))))
    stage3 = pipeline(key_only, iterate_dirs, for_each(lambda path: (path, parquet_attribs(path))))
    stage4 = pipeline(key_only, iterate_files, for_each(with_attribs()))
    stage5 = pipeline(key_only, read_jsonlines, for_each(key_by_none))

    exploder = pipeline(
        with_attribs(),  # empty attribs
        fan_out(
            explode_with(stage1),
            explode_with(stage2),
            explode_with(stage3),
            explode_with(stage4),
            explode_with(stage5),
        ),
        for_each(drop_key)
    )

    df = pd.DataFrame.from_records(exploder(example_data))

    # assert False, df
    assert set(df.columns) == set(["color", "year", "size", "x"])


def test_explode_with(example_data):

    stage1 = pipeline(key_only, iterate_dirs, for_each(with_attribs(color=lambda path: path.name)))
    p1 = stage1
    p2 = explode_with(stage1)

    tagged = (example_data, dict(keep=42))

    for _, attribs in p1(tagged):
        assert attribs.keys() == {"color"}
        break

    for _, attribs in p2(tagged):
        assert attribs.keys() == {"color", "keep"}
        break


@pytest.fixture
def spark_context(pyspark_context, pysparkling_context):
    if USE_SPARK:
        return pyspark_context
    else:
        return pysparkling_context


@pytest.fixture
def pysparkling_context():
    import pysparkling
    return pysparkling.Context()


@pytest.fixture(scope="session")
def pyspark_context():
    if USE_SPARK:
        pypath = os.environ.get("PYTHONPATH", "")
        test_utils_path = Path(testcases.__file__).parent
        project_path = test_utils_path.parent
        src_path = project_path / "src"
        # assert False, test_utils_path
        paths = [
            str(src_path), str(test_utils_path), pypath
        ]
        worker_pypath = ":".join(paths)

        conf = (
            pyspark.SparkConf()
            .setAppName("MyRDDApp")
            .setMaster("local[*]")
            .set("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
            .set("spark.executorEnv.PYTHONPATH", worker_pypath)
        )
        return pyspark.SparkContext(conf=conf)


@pytest.fixture
def spark_session(spark_context):
    if USE_SPARK:
        return pyspark.sql.SparkSession(spark_context)
    else:
        return pysparkling.sql.session.SparkSession(spark_context)


@pytest.mark.skipif(not USE_SPARK, reason="Only needed when testing with Spark.")
def test_worker_env(spark_context):
    # assert False, os.path.abspath(os.path.curdir)

    if False:
        with (Path(os.path.curdir) / "temp.txt").open("w") as f:
            json.dump(dict(os.environ), f, indent=2)

    worker_pyenv, = (
        spark_context.parallelize([None])
        .map(lambda _: os.environ.get("PYTHONPATH"))
    ).collect()

    assert "sparkrawl/src" in worker_pyenv
    assert "sparkrawl/test_util" in worker_pyenv


def test_krawl(spark_context, tmp_path):

    data_path = tmp_path / "data"
    populate_directory(data_path)

    item = data_path, {"global_attr": 42}
    rdd = spark_context.parallelize([item])

    attribs = (
        rdd
        .flatMap(explode_with(pipeline(key_only, iterate_partitions)))
        .flatMap(explode_with(pipeline(key_only, iterate_files, for_each(empty_attribs))))
        .flatMap(explode_with(pipeline(key_only, read_jsonlines, for_each(key_by_none))))
    ).values().first()

    assert attribs["color"] in COLORS
    assert attribs["year"] in YEARS
    assert attribs["size"] in SIZES
    assert isinstance(attribs["x"], float)
    assert attribs["global_attr"] == 42

    # TODO: Break this test up.

    exploder1 = explode_with(pipeline(key_only, AttributePathBranch("color")))
    exploder2 = explode_with(pipeline(key_only, AttributePathBranch("year", int)))

    item = (data_path, {})
    for out in exploder1(item):
        subdir, attribs = out
        for out_ in exploder2(out):
            subdir_, attribs_ = out_
            assert "color" in attribs_
            assert "year" in attribs_

    by_fan = fan_out(
        exploder1,
        exploder2
    )
    for out_ in by_fan(item):
        subdir_, attribs_ = out_
        assert "color" in attribs_
        assert "year" in attribs_


if USE_SPARK:
    from pyspark.sql.types import *
else:
    from pysparkling.sql.types import *


@pytest.mark.parametrize("use_pandas", [False, True])
@pytest.mark.parametrize("crawl_schema", [
    None,
    StructType([
        StructField("child_path", StringType()),
        StructField("color", StringType()),
        StructField("year", IntegerType()),
        StructField("size", StringType()),
    ]),
])
def test_krawl_df(
        use_pandas, crawl_schema,
        spark_context, spark_session, tmp_path
):
    """

    ... maybe possible with explode...
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html

    but doesn't seem advisable

    """
    data_path = tmp_path / "data"
    populate_directory(data_path)

    item = dict(
        path=str(data_path),
        global_attr=42,
    )

    if use_pandas:
        df_ = pd.DataFrame.from_records([item])

        df = (
            spark_session
            .createDataFrame(df_)
        )

    else:
        data = [pyspark.Row(**item)]
        df = spark_context.parallelize(data).toDF()

    # convert signature
    iterate_partitions_ = pipeline(
        Path,
        iterate_partitions,
        for_each(map_key(str))
    ) # :: str -> [(str, {attrs})]

    # now we need something that takes key, {attrs}, and iterates just {attrs_}
    df1 = explode_df(
        df,
        "path",
        pipeline(
            key_only,
            iterate_partitions_,
            for_each(inject_key("child_path"))
        ),
        new_cols_schema=crawl_schema,
    )

    row = df1.rdd.first()
    assert row.global_attr == 42

    assert Path(row.child_path) == data_path / row.color / str(row.year) / f"size={row.size}"

    # TODO: Break this test up.

    iterate_files_ = pipeline(Path, iterate_files, for_each(str))

    df2 = explode_df(
        df1, "child_path",
        pipeline(
            key_only,
            iterate_files_,
            for_each(dictwrap("file_path"))
        ),
    )

    read_jsonlines_ = pipeline(Path, read_jsonlines)

    df3 = explode_df(
        df2, "file_path",
        pipeline(
            key_only,
            read_jsonlines_,
        )
    )

    assert isinstance(df3.rdd.first().x, float)


def test_branchers(tmp_path):

    data_path = tmp_path / "data"
    populate_directory(data_path)

    it = (
        (p3, {**d1, **d2, **d3})
        for p1, d1 in AttributePathBranch("color")(data_path)
        for p2, d2 in AttributePathBranch("year", int)(p1)
        for p3, d3 in ParquetPathBranch()(p2)
    )

    for leaf, attribs in it:
        assert attribs["color"] in COLORS
        assert attribs["year"] in YEARS
        assert attribs["size"] in SIZES

        break  # if we can pull one out, that's good enough


@pytest.mark.xfail(reason="Beware dict key order!")
def test_createDataFrame_unordered_schemaless(spark_session):

    data = [
        dict(a=1, b="1"),
        dict(b="2", a=2),
    ]

    df = spark_session.createDataFrame([
        pyspark.Row(**record) for record in data
    ])

    assert [row.a for row in df.rdd.collect()] == [1, 2]
