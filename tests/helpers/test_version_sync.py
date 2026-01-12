import importlib.metadata
import sys
from pathlib import Path
import warnings

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

import requests
from packaging.version import parse as parse_version


def test_pyproject_version_matches_installed():
    installed = importlib.metadata.version("openforis-whisp")
    pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        pyproject = tomllib.load(f)
    source = pyproject["tool"]["poetry"]["version"]
    if installed != source:
        warnings.warn(
            f"Installed version ({installed}) does not match pyproject.toml ({source}).\n"
            "If you are in editable mode, run: pip install -e .[dev] and restart your environment.",
            UserWarning,
        )


def test_no_newer_version_on_pypi():
    installed = importlib.metadata.version("openforis-whisp")
    url = "https://pypi.org/pypi/openforis-whisp/json"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    latest = data["info"]["version"]
    if parse_version(latest) > parse_version(installed):
        warnings.warn(
            f"A newer version is available on PyPI ({latest}) than your installed version ({installed}).\n"
            "Consider upgrading: pip install --upgrade --pre openforis-whisp",
            UserWarning,
        )
