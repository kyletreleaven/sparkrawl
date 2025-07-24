import nox


@nox.session
def dev(session):
    session.install("pyspark", "ipython")
    session.run("ipython")


@nox.session
def test(session):
    session.install("pyspark", "pytest", "pytest-coverage")
    session.run("pytest")
