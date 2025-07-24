import re
import glob
import random
from pathlib import Path

from fileio import write_jsonlines

COLORS = ["green", "red", "blue"]
YEARS = [2024, 2025]
SIZES = ["large", "small"]


def populate_directory(path: Path):

    for c in COLORS:
        for y in YEARS:
            for sz in SIZES:
                for k in range(5):
                    child = path / c / str(y) / f"size={sz}"
                    child.mkdir(parents=True, exist_ok=True)

                    file = child / f"{k}.jsonl"
                    data = [{"x": random.random()} for _ in range(10)]

                    write_jsonlines(data, file)


def iterate_partitions(path: Path):
    glob_patt = str(path / "*/*/*")
    re_patt = re.compile(str(path / "(?P<color>.*)/(?P<year_str>.*)/(?P<size_spec>.*)"))

    for child in glob.glob(glob_patt):
        m = re_patt.match(child)
        if m is None:
            continue

        gd = m.groupdict()

        try:
            key3, value3 = gd["size_spec"].split("=", maxsplit=1)
        except ValueError:
            continue

        result = dict(
            color=gd["color"],
            year=int(gd["year_str"]),
        )
        result[key3] = value3

        yield Path(child), result
