import json

from ospath import AttributePathBranch, ParquetPathBranch, iterate_files
import testcases
from testcases import *
import pyspark
from fileio import read_jsonlines

import pytest

import os
import sys
# os.environ["PYSPARK_PYTHON"] = sys.executable

@pytest.fixture(scope="session")
def spark_context():
    pypath = os.environ.get("PYTHONPATH", "")
    test_utils_path = os.path.dirname(testcases.__file__)
    # assert False, test_utils_path
    worker_pypath = f"{pypath}:{test_utils_path}"

    conf = (
        pyspark.SparkConf()
        .setAppName("MyRDDApp")
        .setMaster("local[*]")
        .set("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
        .set("spark.executorEnv.PYTHONPATH", worker_pypath)
    )
    return pyspark.SparkContext(conf=conf)


def test_worker_env(spark_context):
    # assert False, os.path.abspath(os.path.curdir)

    if False:
        with (Path(os.path.curdir) / "temp.txt").open("w") as f:
            json.dump(dict(os.environ), f, indent=2)

    worker_pyenv, = (
        spark_context.parallelize([None])
        .map(lambda _: os.environ.get("PYTHONPATH"))
    ).collect()

    assert "sparkrawl/test_util" in worker_pyenv


@pytest.mark.skip
def test_krawl(spark_context, tmp_path):
    from sparkrawl import (
        key_by_attrib, no_attribs, only_attribs, brancher
    )

    data_path = tmp_path / "data"
    populate_directory(data_path)

    rdd = spark_context.parallelize([data_path])

    assert False, (
        rdd
        .map(lambda path: (path, {}))
        .flatMap(brancher(iterate_partitions))
        .flatMap(no_attribs(iterate_files))
        .flatMap(only_attribs(read_jsonlines))
    ).first()


# @pytest.mark.skip
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
