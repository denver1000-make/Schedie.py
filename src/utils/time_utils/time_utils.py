
from datetime import  date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from src.modelsV2.model import ResolvedScheduleSlot


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

def strip_date_of_start_time(resolved_time_slot: ResolvedScheduleSlot) -> date:
    return resolved_time_slot.start_date_in_schedule.date()

def parse_time(raw_time):
    return datetime.strptime(raw_time, "%I:%M%p").time()