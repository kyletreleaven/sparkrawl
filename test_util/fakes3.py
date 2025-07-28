import dataclasses
import shutil
import tempfile
from functools import cached_property
from pathlib import Path
from pydoc import parentname
from urllib.parse import urlparse, quote, unquote
from dataclasses import dataclass
from sparkrawl.common import inner_class


@dataclass(frozen=True)
class FakeS3:
    root: Path

    def __post_init__(self):
        self.root.mkdir(parents=True, exist_ok=True)

    def _to_path(self, bucket: str, key: str) -> Path:
        return self.root / bucket / key

    def _resolve(self, uri: str):
        uri_ = Uri.from_uri(uri)
        path = self._to_path(uri_.bucket, uri_.key)
        path.parent.mkdir(parents=True, exist_ok=True)  # this needs to exist --- as a dir! --- now.
        return path

    def put(self, path: Path, uri: str):
        shutil.copyfile(path, self._resolve(uri))

    def get(self, uri: str, path: Path):
        shutil.copyfile(self._resolve(uri), path)

    def list_objects(self, uri: str):
        uri_ = Uri.from_uri(uri)
        path = self._to_path(uri_.bucket, uri_.key)
        for path_ in path.iterdir():
            if path_.is_file():
                yield f"s3://{uri_.bucket}/{uri_.key}/{path_.name}"

    def list_prefixes(self, uri: str):
        uri_ = Uri.from_uri(uri)
        path = self._to_path(uri_.bucket, uri_.key)
        for path_ in path.iterdir():
            if path_.is_dir():
                yield f"s3://{uri_.bucket}/{uri_.key}/{path_.name}"


@dataclass(frozen=True)
class Uri:
    bucket: str
    key_path: Path

    @cached_property
    def key(self):
        return str(self.key_path)

    @classmethod
    def from_uri(cls, uri: str):
        """Extract bucket and key from s3://bucket/key URI."""
        parsed = urlparse(uri)
        if parsed.scheme != "s3":
            raise ValueError(f"Not a valid s3 URI: {uri}")
        bucket = parsed.netloc
        key = unquote(parsed.path.lstrip("/"))
        return cls(bucket, Path(key))

    @property
    def parent(self):
        return dataclasses.replace(self, key_path=self.key_path.parent)

    def __truediv__(self, name: str):
        return dataclasses.replace(self, key_path=self.key_path / name)

    def __str__(self):
        return f"s3://{self.bucket}/{str(self.key)}"


fakes3 = FakeS3(Path("my-fake-s3"))

uri = Uri.from_uri("s3://my-bucket/my-prefix/some-object")

with tempfile.NamedTemporaryFile() as f:
    with open(f.name, "w") as f_:
        pass
    fakes3.put(f.name, str(uri))

# fakes3.get(str(uri), "downloaded")

list(fakes3.list_prefixes(str(uri.parent))), list(fakes3.list_objects(str(uri.parent)))
