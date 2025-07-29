import nox


@nox.session
def format(session):
    """Runs the unit tests."""
    try:
        toml = nox.project.load_toml("pyproject.toml")
        deps = toml["project"]["dependencies"]
        build_deps = toml["project"]["optional-dependencies"]["build"]

    except:
        import json
        print(json.dumps(toml, indent=2))
        raise

    session.install(*deps, *build_deps)  # but not the package itself...
    session.run(
        "black",
        "src",
    )


@nox.session
def dev(session):
    try:
        toml = nox.project.load_toml("pyproject.toml")
        dev_deps = toml["project"]["optional-dependencies"]["dev"]

    except:
        import json
        print(json.dumps(toml, indent=2))
        raise

    session.install("-e", ".")
    session.install(*dev_deps)

    session.run(
        "ipython",
        env={"PYTHONPATH": "test_util"}
    )


@nox.session
def notebook(session):
    try:
        toml = nox.project.load_toml("pyproject.toml")
        dev_deps = toml["project"]["optional-dependencies"]["dev"]

    except:
        import json
        print(json.dumps(toml, indent=2))
        raise

    session.install("-e", ".")
    session.install(*dev_deps)

    import os

    session.run(
        "jupyter", "notebook",
        env={"PYTHONPATH": os.path.abspath("test_util")}
    )


@nox.session
def test(session):

    try:
        toml = nox.project.load_toml("pyproject.toml")
        deps = toml["project"]["dependencies"]
        test_deps = toml["project"]["optional-dependencies"]["test"]

    except:
        import json
        print(json.dumps(toml, indent=2))
        raise

    session.install(*deps, *test_deps)
    session.run("pytest", *(session.posargs or []))  # posargs for test filtering.
