"""Test time conversion."""

from __future__ import annotations

from odsbox import asam_time

import pytest
import pandas as pd


def test_from_pd_timestamp():
    dates = [
        ("20241211", "2024-12-11T00:00:00"),
        ("20241211133310", "2024-12-11T13:33:10"),
        ("2024121113331056", "2024-12-11T13:33:10.560000"),
        ("20241211133310561234567", "2024-12-11T13:33:10.561234567"),
        ("2024121113", "2024-12-11T13:00:00"),
        ("202412111333", "2024-12-11T13:33:00"),
        ("20241211133356", "2024-12-11T13:33:56"),
        ("202412111333561", "2024-12-11T13:33:56.100000"),
        ("2024121113335612", "2024-12-11T13:33:56.120000"),
        ("20241211133356123", "2024-12-11T13:33:56.123000"),
        ("202412111333561234", "2024-12-11T13:33:56.123400"),
        ("2024121113335612345", "2024-12-11T13:33:56.123450"),
        ("20241211133356123456", "2024-12-11T13:33:56.123456"),
        ("202412111333561234567", "2024-12-11T13:33:56.123456700"),
        ("2024121113335612345678", "2024-12-11T13:33:56.123456780"),
        ("20241211133356123456789", "2024-12-11T13:33:56.123456789"),
        ("20241211133356123456789", "2024-12-11T13:33:56.123456789"),
        ("19700101", "1970-01-01T00:00:00"),
        ("1970010100", "1970-01-01T00:00:00"),
        ("197001010000", "1970-01-01T00:00:00"),
        ("19700101000000", "1970-01-01T00:00:00"),
    ]

    for date in dates:
        pd_timestamp_in = pd.Timestamp(date[1])
        result = asam_time.from_pd_timestamp(pd_timestamp_in, length=len(date[0]))
        assert result == date[0]
        # lets do the roundtrip
        pd_timestamp_out = asam_time.to_pd_timestamp(result)
        assert pd_timestamp_out == pd_timestamp_in


def test_from_pd_timestamp_special():
    assert "20241211000000000000000" == asam_time.from_pd_timestamp(pd.Timestamp("2024-12-11T00:00:00"), 23)
    assert "20241211000000000000" == asam_time.from_pd_timestamp(pd.Timestamp("2024-12-11T00:00:00"), 20)
    assert "20241211000000000" == asam_time.from_pd_timestamp(pd.Timestamp("2024-12-11T00:00:00"), 17)
    assert "20241211000000000" == asam_time.from_pd_timestamp(pd.Timestamp("2024-12-11T00:00:00"))


def test_from_pd_timestamp_z():
    assert "20241211133310000" == asam_time.from_pd_timestamp(pd.Timestamp("2024-12-11T13:33:10Z"))
    assert "20240711133310000" == asam_time.from_pd_timestamp(pd.Timestamp("2024-07-11T13:33:10Z"))
    assert "20241211133310000" == asam_time.from_pd_timestamp(pd.Timestamp("2024-12-11T13:33:10+02"))
    assert "20240711133310000" == asam_time.from_pd_timestamp(pd.Timestamp("2024-07-11T13:33:10+02"))


def test_from_pd_timestamp_special2():
    assert "20241211133356123456789" == asam_time.from_pd_timestamp(pd.Timestamp("2024-12-11T13:33:56.123456789"), 23)
    assert "20241211133356123" == asam_time.from_pd_timestamp(pd.Timestamp("2024-12-11T13:33:56.123456789"))
    assert "20241211133356123456789" == asam_time.from_pd_timestamp(pd.Timestamp("2024-12-11T13:33:56.123456789"), 26)
    assert "20241211133356123456" == asam_time.from_pd_timestamp(pd.Timestamp("2024-12-11T13:33:56.123456789"), 20)
    assert "20241211133356123" == asam_time.from_pd_timestamp(pd.Timestamp("2024-12-11T13:33:56.123456789"), 17)
    assert "20241211133356" == asam_time.from_pd_timestamp(pd.Timestamp("2024-12-11T13:33:56.123456789"), 14)


def test_from_pd_timestamp_none():
    assert "" == asam_time.from_pd_timestamp(pd.NaT)
    assert "" == asam_time.from_pd_timestamp(None)


def test_to_pd_timestamp():
    dates = [
        ("20241211", "2024-12-11T00:00:00"),
        ("20241211133310", "2024-12-11T13:33:10"),
        ("2024121113331056", "2024-12-11T13:33:10.560000"),
        ("20241211133310561234567", "2024-12-11T13:33:10.561234567"),
        ("2024121113", "2024-12-11T13:00:00"),
        ("202412111333", "2024-12-11T13:33:00"),
        ("20241211133356", "2024-12-11T13:33:56"),
        ("202412111333561", "2024-12-11T13:33:56.100000"),
        ("2024121113335612", "2024-12-11T13:33:56.120000"),
        ("20241211133356123", "2024-12-11T13:33:56.123000"),
        ("202412111333561234", "2024-12-11T13:33:56.123400"),
        ("2024121113335612345", "2024-12-11T13:33:56.123450"),
        ("20241211133356123456", "2024-12-11T13:33:56.123456"),
        ("202412111333561234567", "2024-12-11T13:33:56.123456700"),
        ("2024121113335612345678", "2024-12-11T13:33:56.123456780"),
        ("20241211133356123456789", "2024-12-11T13:33:56.123456789"),
        ("20241211133356123456789123", "2024-12-11T13:33:56.123456789"),
        ("19700101", "1970-01-01T00:00:00"),
        ("1970010100", "1970-01-01T00:00:00"),
        ("197001010000", "1970-01-01T00:00:00"),
        ("19700101000000", "1970-01-01T00:00:00"),
    ]

    for date in dates:
        result = asam_time.to_pd_timestamp(date[0])
        assert result.isoformat() == date[1]


def test_to_pd_timestamp_throw():
    with pytest.raises(SyntaxError):
        asam_time.to_pd_timestamp("2024")
    with pytest.raises(SyntaxError):
        asam_time.to_pd_timestamp("202401")
    with pytest.raises(SyntaxError):
        asam_time.to_pd_timestamp("2024011")
    with pytest.raises(ValueError, match="day is out of range for month"):
        asam_time.to_pd_timestamp("20240100")
    with pytest.raises(ValueError, match="year 0 is out of range"):
        asam_time.to_pd_timestamp("00000101")


def test_to_pd_timestamp_NaN():
    assert pd.isna(asam_time.to_pd_timestamp(""))
    assert pd.isna(asam_time.to_pd_timestamp(None))  # type: ignore
