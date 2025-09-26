import datetime
from zoneinfo import ZoneInfo

def get_datetime_from_epoch(epoch_seconds: float, tz: ZoneInfo) -> datetime.datetime:
    """Convert epoch seconds to a timezone-aware datetime object."""
    return datetime.datetime.fromtimestamp(epoch_seconds, tz)