import time
from datetime import datetime, timezone
from dateutil import parser


def now_timestamp() -> int:
    """Now timestamp (Second)"""

    return int(time.time())


def iso_8601_to_timestamp(date_string: str) -> int:
    """Convert ISO 8601 datetime string to the timestamp (integer)

    Args:
        date_string (str): ISO 8601 format.
            e.g. "2021-06-22T07:16:03Z" or "2021-06-22T07:16:03.032Z"
    """

    date_obj = parser.parse(date_string)
    return int(date_obj.timestamp())


def timestamp_to_iso_8601(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", ".000Z")
