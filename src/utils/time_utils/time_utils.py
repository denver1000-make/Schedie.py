
from datetime import  date, datetime, time
from zoneinfo import ZoneInfo



my_tz = ZoneInfo("Asia/Manila")
format_norm = "%I:%M%p"

def convert_firestore_time_to_standard_time(time_str: str, format: str) -> time:
    return datetime.strptime(time_str, format_norm).time()

def str_time_to_standardize_time(time_str: str) -> time:
    return convert_firestore_time_to_standard_time(
        format=format_norm,
        time_str=time_str
    )
    
def normalize_time_to_datetime(time: time, tz: ZoneInfo, date: date) -> datetime:
    normalized_time = datetime.combine(date, time, tz)
    return normalized_time

def time_to_datetime(time: time, year: int, month: int, day_of_month: int) -> datetime:
    return normalize_time_to_datetime(
        tz=my_tz, 
        date=date(
            day=day_of_month,
            month=month, 
            year=year
        ), time=time)

def diff_time_in_sec(t1: datetime, t2: datetime) -> float:
    return (t1 - t2).total_seconds()

def strip_date_of_start_time_v2(start_date: datetime) -> date:
    return start_date.date()

def parse_time(raw_time):
    """
    Parse time string in either 24-hour format (HH:MM) or 12-hour format (HH:MMAM/PM).
    
    Args:
        raw_time: Time string in format "06:32" or "6:32AM"
        
    Returns:
        datetime.time object
    """
    try:
        # First try 12-hour format (original format)
        return datetime.strptime(raw_time, "%I:%M%p").time()
    except ValueError:
        try:
            # Try 24-hour format (used by temporary schedules)
            return datetime.strptime(raw_time, "%H:%M").time()
        except ValueError:
            raise ValueError(f"Time '{raw_time}' does not match expected formats: 'HH:MM' or 'HH:MMAM/PM'")