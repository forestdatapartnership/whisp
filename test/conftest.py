import logging
import pytest

from src.utils import init_ee, clear_ee_credentials
from src.logger import StdoutLogger


logger = StdoutLogger()


logging.getLogger("faker").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.INFO)


@pytest.fixture(scope="session", autouse=True)
def _setup_and_teardown_ee_session() -> None:
    init_ee()
    yield
    clear_ee_credentials()
