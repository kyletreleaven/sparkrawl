from dataclasses import dataclass
from typing import Optional
from pathlib import Path


@dataclass(frozen=True)
class AttributePathBranch:
    attrib: str
    converter: Optional[type] = None

    def __call__(self, path: Path):
        for child in path.iterdir():
            if not child.is_dir(): continue

            value = child.name
            if self.converter:
                value = self.converter(value)
            yield child, {self.attrib: value}


@dataclass(frozen=True)
class ParquetPathBranch:
    converter: Optional[type] = None

    def __call__(self, path: Path):
        for child in path.iterdir():
            if not child.is_dir(): continue

            try:
                key, value = child.name.split("=", maxsplit=1)
                if self.converter:
                    value = self.converter(value)
                yield child, {key: value}

            except ValueError:
                pass


def iterate_files(path: Path):
    yield from (
        child
        for child in path.iterdir()
        if child.is_file()
    )


def iterate_dirs(path: Path):
    yield from (
        child
        for child in path.iterdir()
        if child.is_dir()
    )
