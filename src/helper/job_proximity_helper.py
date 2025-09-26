import datetime
from time import time

def is_time_within_timeslot(
    timeslot_day_name: str,
    current_day_name: str,
    current_time: datetime.time,
    parsed_start_time: datetime.time,
    parsed_end_time: datetime.time
) -> bool:
    start_sec = parsed_start_time.hour * 3600 + parsed_start_time.minute * 60 + parsed_start_time.second
    end_sec = parsed_end_time.hour * 3600 + parsed_end_time.minute * 60 + parsed_end_time.second
    current_time_seconds = current_time.hour * 3600 + current_time.minute * 60 + current_time.second
    
    return (timeslot_day_name == current_day_name) and (current_time_seconds >= start_sec) and (current_time_seconds <= end_sec)