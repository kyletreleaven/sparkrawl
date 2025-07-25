import os
import sys

from sparkrawl import *

import testcases
from testcases import populate_directory
from ospath import *
from fileio import *

from pathlib import Path

data_path = Path("data")

if not data_path.exists():
    populate_directory(data_path)


import pandas as pd

item = dict(path=str(data_path), author="setiptah")
pdf = pd.DataFrame.from_records([item])


# Spark
USE_SPARK = True

def get_spark_context():

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


if USE_SPARK:
    from pyspark.sql import SparkSession
else:
    from pysparkling.sql.session import SparkSession


sc = get_spark_context()
sess = SparkSession(sc)

df = sess.createDataFrame(pdf)
df.show()


def on_path_str(fn):

    def fn_(path_str):
        for path, attribs in fn(Path(path_str)):
            yield str(path), attribs

    return fn_


df1 = explode_df(
    df,
    "path",
    explodeWith(on_path_str(AttributePathBranch("color"))),
    "path",
)
df1.show()

df2 = explode_df(
    df1, "path",
    explodeWith(on_path_str(AttributePathBranch("year", int))),
    "path",
)
df2.show()

df3 = explode_df(
    df2, "path",
    explodeWith(on_path_str(ParquetPathBranch())),
    "path",
)
df3.show()


def iterate_files_(path_str):
    for path_ in iterate_files(Path(path_str)):
        yield str(path_)

df4 = explode_df(
    df3, "path",
    explodeWith(iterate_files_, out_spec=KEY_ONLY),
    "path",
)
df4.show()


def read_jsonlines_(path_str):
    yield from read_jsonlines(Path(path_str))

df5 = explode_df(
    df4, "path",
    explodeWith(read_jsonlines_, out_spec=ATTRS_ONLY),
)
df5.show()
