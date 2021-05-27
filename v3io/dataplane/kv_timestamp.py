import sys
import datetime

# used only n py2
BASE_DATETIME = datetime.datetime(1970, 1, 1)


def _get_timestamp_from_datetime_py3(dt):
    return dt.astimezone(datetime.timezone.utc).timestamp()


def _get_timestamp_from_datetime_py2(dt):
    return (dt - BASE_DATETIME).total_seconds()


# _get_timestamp_from_datetime is python version specific. resolve this once
if sys.version_info[0] >= 3:
    _get_timestamp_from_datetime = _get_timestamp_from_datetime_py3
else:
    _get_timestamp_from_datetime = _get_timestamp_from_datetime_py2


def encode(dt):
    timestamp = _get_timestamp_from_datetime(dt)

    # get integer and float part of nanoseconds
    seconds, nanoseconds_float = divmod(timestamp, 1)
    nanoseconds = int(nanoseconds_float * 1e9)

    return '{}:{}'.format(int(seconds), nanoseconds)


def decode(encoded_dt):
    seconds_str, nanoseconds_str = encoded_dt.split(':')

    timestamp = int(seconds_str) + (int(nanoseconds_str) / 1e9)

    return datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)
