import os


def pytest_collection_modifyitems(config, items):
    import pytest

    spark_dir = os.path.join(os.path.dirname(__file__), "spark") + os.sep
    for item in items:
        if str(item.fspath).startswith(spark_dir):
            item.add_marker(pytest.mark.spark)
