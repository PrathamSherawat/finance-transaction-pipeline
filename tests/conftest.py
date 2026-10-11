import pytest

from finance_pipeline.ingest import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("finance-pipeline-tests")
    yield session
    session.stop()


def pytest_collection_modifyitems(items):
    """Mark every test that needs Spark as slow, so `pytest -m "not slow"` is a fast loop."""
    for item in items:
        if "spark" in item.fixturenames:
            item.add_marker(pytest.mark.slow)
