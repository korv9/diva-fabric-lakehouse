import os
import shutil

import pytest


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    if not (shutil.which("java") or os.environ.get("JAVA_HOME")):
        pytest.skip("Java is required for local Spark tests")
    from diva_lakehouse.spark_utils import get_spark

    session = get_spark(local_warehouse=str(tmp_path_factory.mktemp("warehouse")), app_name="diva-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def lakehouse(tmp_path):
    from diva_lakehouse.storage import Lakehouse

    os.makedirs(tmp_path / "Files")
    return Lakehouse(str(tmp_path))
