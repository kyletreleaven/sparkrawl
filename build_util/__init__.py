import os
import subprocess as sp
import pathlib

DIR = pathlib.Path(__file__).parent
PROJECT_DIR = DIR.parent
BUILD_DIR = PROJECT_DIR / "build"
DOCS_DIR = PROJECT_DIR / "docs"

def singleton(factory):
    return factory()


def noxrun(session, args_, *args, **kwargs):
    return sp.run([
        "nox", "-r", "-s", session, "--", *args_
    ], *args, **kwargs)


@singleton
class docs:
    """Compiles documentation including a Jupyter notebook."""

    def create(self):
        demo_notebook.ensure()
        noxrun("docs", ["mkdocs", "build"])


@singleton
class demo_notebook:
    """A nice notebook.

    https://nbconvert.readthedocs.io/en/latest/usage.html#

    """
    DIR = PROJECT_DIR / "notebooks"
    notebook_file =  DIR / "sparkrawl-demo.ipynb"

    output_dir = DOCS_DIR / "notebooks"
    output_file = output_dir / "demo.html"

    def ensure(self):
        if not self.output_file.exists():
            self.create()

    def create(self):
        kernel.install()

        # Add test_util to downstream PYTHONPATH.
        env = {**os.environ}
        pypath = env.get("PYTHONPATH")
        parts = [os.path.abspath("test_util")]
        if pypath:
            parts.append(pypath)
        env["PYTHONPATH"] = ":".join(parts)

        noxrun("docs", [
            *"jupyter nbconvert --to html --execute".split(),
            # "--show-config-json",
            "--ExecutePreprocessor.kernel_name=nbconvert",  # same as in noxfile; not python3, so we can verify locally
            "--output", self.output_file,
            self.notebook_file,
        ], env=env)


@singleton
class kernel:

    def install(self):
        display_name = "Python (nbconvert env)"
        noxrun("docs", [
            *"python -m ipykernel install --user --name nbconvert".split(),
            "--display-name", display_name
        ])
