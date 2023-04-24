import time
from datetime import datetime, timezone


def now_timestamp() -> int:
    """Now timestamp (Second)"""

    return int(time.time())


def iso_8601_to_timestamp(date_string: str) -> int:
    """Convert ISO 8601 datetime string to timestamp

    Args:
        date_string (str): ISO 8601 format.
            e.g. "2021-06-22T07:16:03Z" or "2021-06-22T07:16:03.032Z"
    """

    if len(date_string) == 20:
        return int(datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S%z").timestamp())
    else:
        return int(datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%z").timestamp())


def timestamp_to_iso_8601(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", ".000Z")
