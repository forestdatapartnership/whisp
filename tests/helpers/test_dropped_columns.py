"""Warn when result columns are removed by schema validation."""

from unittest.mock import Mock

import pandas as pd
import pytest

from openforis_whisp import reformat


@pytest.mark.parametrize(
    "custom_bands, expected_columns, dropped",
    [
        (None, [], ["unknown_a", "unknown_b"]),
        (["unknown_a"], ["unknown_a"], ["unknown_b"]),
        (["unknown_a", "unknown_b"], ["unknown_a", "unknown_b"], []),
        ({"unknown_a": "float64"}, ["unknown_a", "unknown_b"], []),
    ],
)
def test_warn_only_for_extra_columns_actually_dropped(
    monkeypatch, custom_bands, expected_columns, dropped
):
    warning_logger = Mock()
    monkeypatch.setattr(reformat, "get_whisp_logger", lambda: warning_logger)
    data = pd.DataFrame({"Area": [1.0], "unknown_a": [0.3], "unknown_b": [0.4]})

    result = reformat.validate_dataframe_using_lookups_flexible(
        data, custom_bands=custom_bands
    )

    assert "Area" in result
    for column in ["unknown_a", "unknown_b"]:
        assert (column in result) == (column in expected_columns)
    if dropped:
        warning_logger.warning.assert_called_once()
        message = warning_logger.warning.call_args.args[0]
        assert all(column in message for column in dropped)
        assert all(column not in message for column in expected_columns)
    else:
        warning_logger.warning.assert_not_called()


def test_known_columns_do_not_warn(monkeypatch):
    warning_logger = Mock()
    monkeypatch.setattr(reformat, "get_whisp_logger", lambda: warning_logger)
    result = reformat.validate_dataframe_using_lookups_flexible(
        pd.DataFrame({"Area": [1.0], "EUFO_2020": [0.3]})
    )
    assert "EUFO_2020" in result
    warning_logger.warning.assert_not_called()


def test_direct_validation_warns_for_discarded_columns(monkeypatch):
    warning_logger = Mock()
    monkeypatch.setattr(reformat, "get_whisp_logger", lambda: warning_logger)
    result = reformat.validate_dataframe_using_lookups(
        pd.DataFrame({"Area": [1.0], "unknown_a": [0.3]})
    )
    assert "unknown_a" not in result
    assert "unknown_a" in warning_logger.warning.call_args.args[0]


def test_warning_is_visible_when_reformat_logger_is_suppressed(monkeypatch):
    import logging
    from openforis_whisp.logger import get_whisp_logger

    reformat_logger = logging.getLogger("openforis_whisp.reformat")
    monkeypatch.setattr(reformat_logger, "level", logging.ERROR)
    whisp_logger = get_whisp_logger()
    records = []

    class Capture(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = Capture()
    whisp_logger.addHandler(handler)
    try:
        reformat.validate_dataframe_using_lookups_flexible(
            pd.DataFrame({"Area": [1.0], "unknown_a": [0.3]})
        )
    finally:
        whisp_logger.removeHandler(handler)
    assert any(record.levelno == logging.WARNING and "unknown_a" in record.getMessage()
               for record in records)
