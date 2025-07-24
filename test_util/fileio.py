from pathlib import Path
import json


def write_jsonlines(data, path: Path):
    with path.open("w") as f:
        for record in data:
            f.write(f"{json.dumps(record)}\n")


def read_jsonlines(path: Path):
    with path.open() as f:
        for line in f.readlines():
            yield json.loads(line)
