"""datetime is represented as string in ASAM ODS formatted using YYYYMMDDHHMMSSFFF. Here you find some helpers."""

from __future__ import annotations

from typing import cast

import pandas as pd


def __normalize_datetime_string(asam_time: str) -> str:
    asam_time_len = len(asam_time)
    if asam_time_len < 8:
        raise SyntaxError("Time value mus at least contain year, month, day")
    if asam_time_len <= 14:
        return asam_time.ljust(14, "0")
    if asam_time_len <= 20:
        return asam_time.ljust(20, "0")
    if asam_time_len <= 23:
        return asam_time.ljust(23, "0")
    return asam_time


def to_pd_timestamp(asam_time: str | None) -> pd.Timestamp:
    """
    Convert ASAM ODS datetime string to pandas Timestamp.

    Args:
        asam_time: ASAM ODS datetime string to be converted. Formatted like `YYYYMMDDHHMMSSFFF`.
            It must at least contain `YYYYMMDD`.

    Returns:
        Corresponding pandas Timestamp value. For empty string `pd.NaT` is returned.

    Raises:
        SyntaxError: If content is invalid.
    """
    if asam_time is None or "" == asam_time:
        return cast(pd.Timestamp, pd.NaT)

    asam_time_normalized = __normalize_datetime_string(asam_time)
    asam_time_len = len(asam_time_normalized)

    return pd.Timestamp(
        year=int(asam_time_normalized[0:4]) if asam_time_len >= 4 else 0,
        month=int(asam_time_normalized[4:6]) if asam_time_len >= 6 else 1,
        day=int(asam_time_normalized[6:8]) if asam_time_len >= 8 else 1,
        hour=int(asam_time_normalized[8:10]) if asam_time_len >= 10 else 0,
        minute=int(asam_time_normalized[10:12]) if asam_time_len >= 12 else 0,
        second=int(asam_time_normalized[12:14]) if asam_time_len >= 14 else 0,
        microsecond=int(asam_time_normalized[14:20]) if asam_time_len >= 20 else 0,
        nanosecond=int(asam_time_normalized[20:23]) if asam_time_len >= 23 else 0,
    )


def from_pd_timestamp(timestamp: pd.Timestamp | None, length: int = 17) -> str:
    """
    Convert a pandas Timestamp to a string formatted as asamtime (`YYYYMMDDHHMMSSFFF`).

    Args:
        timestamp: The pandas Timestamp to convert. The timezone
            information given in timestamp is ignored.
        length: The desired length of the output string. The final string will
            be truncated to the specified length. The maximum is 23 including
            nanoseconds.

    Returns:
        The asam time representation of the timestamp. For `None` or `pd.NaT`
        an empty string is returned.
    """
    if timestamp is None or pd.isna(timestamp):
        return ""

    asam_time_str: str = timestamp.strftime("%Y%m%d%H%M%S%f")
    if length > 20:
        asam_time_str += f"{timestamp.nanosecond:03d}"
    return asam_time_str[: min(length, len(asam_time_str))]
