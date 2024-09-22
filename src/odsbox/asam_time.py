"""datetime is represented as string in ASAM ODS. Here you find some helpers."""

from __future__ import annotations

import pandas as pd


def __normalize_datetime_string(asam_time):
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


def to_pd_timestamp(asam_time: str) -> pd.Timestamp:
    """
    Convert ASAM ODS datetime string to numpy datetime.

    :param str asam_time: ASAM ODS datetime string to be converted
    :raises requests.SyntaxError: If content is invalid.
    :return np.datetime64: Corresponding numpy datetime value
    """
    if asam_time is None or "" == asam_time:
        return pd.NaT  # type: ignore

    asam_time_normalized = __normalize_datetime_string(asam_time)
    asam_time_len = len(asam_time_normalized)

    return pd.Timestamp(
        year=int(asam_time_normalized[0:4]) if asam_time_len >= 4 else 0,
        month=int(asam_time_normalized[4:6]) if asam_time_len >= 6 else 1,
        day=int(asam_time_normalized[6:8]) if asam_time_len >= 8 else 0,
        hour=int(asam_time_normalized[8:10]) if asam_time_len >= 10 else 0,
        minute=int(asam_time_normalized[10:12]) if asam_time_len >= 12 else 0,
        second=int(asam_time_normalized[12:14]) if asam_time_len >= 14 else 0,
        microsecond=int(asam_time_normalized[14:20]) if asam_time_len >= 20 else 0,
        nanosecond=int(asam_time_normalized[20:23]) if asam_time_len >= 23 else 0,
    )
