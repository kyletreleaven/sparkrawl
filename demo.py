from sparkrawl import *

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


import pysparkling.sql.session

sc = pysparkling.Context()
sess = pysparkling.sql.session.SparkSession(sc)

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
