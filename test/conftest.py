import logging
import pytest

from whisp.utils import init_ee, clear_ee_credentials
from whisp.logger import StdoutLogger

logger = StdoutLogger(__name__)


logging.getLogger("faker").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.INFO)


@pytest.fixture(scope="session", autouse=True)
def _setup_and_teardown_ee_session() -> None:
    init_ee()
    yield
    clear_ee_credentials()
