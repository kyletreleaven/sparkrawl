import nox

DEV = [
    "pandas", "pyspark", "pysparkling",
]

@nox.session
def dev(session):
    session.install(*DEV, "ipython")

    import os

    # pyenv = ":".join(["src", "test_util", os.environ.get("PYTHONPATH", "")])

    session.run(
        "ipython",
        env={"PYTHONPATH": "src:test_util"}
    )


@nox.session
def test(session):
    session.install(
        *DEV,
        "pytest", "pytest-coverage",
    )
    session.run("pytest")
