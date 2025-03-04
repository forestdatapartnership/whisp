import logging
import pytest
import sys
import os

# # Add the src directory to the PYTHONPATH
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
def pytest_configure():
    os.environ[
        "JUPYTER_PLATFORM_DIRS"
    ] = "1"  # to stop deprecation warnigns related to jupyter path migration


from openforis_whisp.utils import init_ee, clear_ee_credentials
from openforis_whisp.logger import StdoutLogger

logger = StdoutLogger(__name__)


logging.getLogger("faker").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.INFO)


@pytest.fixture(scope="session", autouse=True)
def _setup_and_teardown_ee_session() -> None:
    init_ee()
    yield
    clear_ee_credentials()
