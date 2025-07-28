import os
import sys
import tempfile

from sparkrawl import *

import testcases
from sparkrawl import with_attribs
from testcases import populate_directory
from ospath import *
from fileio import *

from pathlib import Path
from fakes3 import FakeS3, Uri


# Local examples!

data_path = Path("data")

if not data_path.exists():
    populate_directory(data_path)


## Raw

fn = pipeline(
    with_attribs(),
    fan_out(
        explode_with(pipeline(key_only, iterate_dirs, for_each(with_attribs(color=lambda path: path.name)))),
        explode_with(pipeline(key_only, AttributePathBranch("year", int))),
        explode_with(pipeline(key_only, ParquetPathBranch())),
        explode_with(
            pipeline(
                key_only,
                iterate_files,
                for_each(with_attribs(file_path=lambda p: p))
            )
        ),
        explode_with(
            pipeline(key_only, read_jsonlines, for_each(key_by_none))
        )
    ),
    for_each(drop_key)
)
list(fn(data_path))



## Pandas DataFrame
import pandas as pd

df = pd.DataFrame.from_records([dict(path=data_path, author="ktreleav")])

exploder = pipeline(
    fan_out(
        explode_with(pipeline(key_only, iterate_dirs, for_each(with_attribs(color=lambda path: path.name)))),
        explode_with(pipeline(key_only, AttributePathBranch("year", int))),
        explode_with(pipeline(key_only, ParquetPathBranch())),
        explode_with(
            pipeline(
                key_only,
                iterate_files,
                for_each(with_attribs(
                    # file_path=lambda p: p
                    partition=lambda path: int(path.name.split(".")[0])
                ))
            )
        ),
        explode_with(
            pipeline(key_only, read_jsonlines, for_each(key_by_none))
        )
    ),
    for_each(drop_key)
)

df_ = explode_pandas_df(df, "path", exploder)


## Enter S3

fake_s3_path = Path("fake-s3")
fake_s3 = FakeS3(fake_s3_path)

data_root = Uri.from_uri("s3://some-bucket/prefix")

data_path = fake_s3._resolve(str(data_root))

if not data_path.exists():
    populate_directory(data_path)


# Raw example. Don't even need S3 or Spark, right?

def uri_name(uri: str):
    return Uri.from_uri(uri).key_path.name

def read_s3_json(uri):
    with tempfile.NamedTemporaryFile() as f:
        fake_s3.get(uri, f.name)
        yield from read_jsonlines(Path(f.name))

def parquet_attribs(uri):
    attrib, value = uri_name(uri).split("=", maxsplit=1)
    return {attrib: value}

exploder_s3 = pipeline(
    fan_out(
        explode_with(pipeline(key_only, fake_s3.list_prefixes, for_each(with_attribs(color=uri_name)))),
        explode_with(pipeline(key_only, fake_s3.list_prefixes, for_each(with_attribs(year=pipeline(uri_name, int))))),
        explode_with(pipeline(key_only, fake_s3.list_prefixes, for_each(compute_value(parquet_attribs)))),
        explode_with(pipeline(key_only, fake_s3.list_objects, for_each(with_attribs(
            # uri=lambda uri: uri
        )))),
        explode_with(pipeline(key_only, read_s3_json, for_each(key_by_none)))
    ),
    for_each(drop_key)
)

pdf = pd.DataFrame.from_records([{"path": str(data_root)}])
pdf_ = explode_pandas_df(pdf, "path", exploder_s3)


# Enter Spark

USE_SPARK = False

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

## SQL

if USE_SPARK:
    from pyspark.sql import SparkSession
else:
    from pysparkling.sql.session import SparkSession

sc = get_spark_context()
sess = SparkSession(sc)

df = sess.createDataFrame(pdf)
df.show()

df_ = explode_df(df, "path", exploder_s3)
df_.show()


## RDD

rdd = sc.parallelize([str(data_root)]).map(with_attribs()).flatMap(exploder_s3)
rdd.sample(False, .1/6).collect()
