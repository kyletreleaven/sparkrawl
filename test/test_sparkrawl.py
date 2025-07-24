import json

import pysparkling.sql.session

from ospath import AttributePathBranch, ParquetPathBranch, iterate_files
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


@pytest.fixture(scope="session")
def spark_context():
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

    else:
        import pysparkling
        return pysparkling.Context()


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

    rdd = spark_context.parallelize([data_path])

    attribs = (
        rdd
        .map(lambda path: (path, {"global_attr": 42}))
        .flatMap(with_attribs(iterate_partitions))
        .flatMap(no_attribs(iterate_files))
        .flatMap(only_attribs(read_jsonlines))
    ).first()

    assert attribs["color"] in COLORS
    assert attribs["year"] in YEARS
    assert attribs["size"] in SIZES
    assert isinstance(attribs["x"], float)
    assert attribs["global_attr"] == 42


@pytest.mark.parametrize("use_pandas", [False, True])
def test_krawl_df(use_pandas, spark_context, spark_session, tmp_path):
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

    rdd = (
        df.rdd
        .map(lambda row: row.asDict())
        .map(extract_key("path"))
        .map(map_key(Path))
    )

    (path, attribs), = rdd.collect()

    assert isinstance(path, Path)
    assert attribs == dict(global_attr=42)


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
