# schedule_model.py
import datetime
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class TimeSlot:
    start_time: str
    end_time: str
    subject: Optional[str] = None
    teacher: Optional[str] = None
    teacher_email: Optional[str] = None
    time_start_in_seconds: int = 0
    start_time_date: Optional[datetime.datetime] = None
    end_time_date: Optional[datetime.datetime] = None
    # Additional fields needed for raw data processing
    timeslot_id: Optional[str] = None
    start_hour: Optional[int] = None
    start_minute: Optional[int] = None
    end_hour: Optional[int] = None
    end_minute: Optional[int] = None
    start_time_date_epoch: Optional[float] = None
    end_time_date_epoch: Optional[float] = None



@dataclass
class ScheduleOfDay:
    day_name: str
    day_order: int
    hours: List[TimeSlot] = field(default_factory=list)


@dataclass
class ScheduleV2:
    room_id: str
    schedule_days: List[ScheduleOfDay] = field(default_factory=list)

@dataclass
class ResolvedScheduleSlot:
    room_id: str
    day_name: str
    day_order: int
    start_time: str
    end_time: str
    subject: str
    teacher: str
    teacher_email: str
    time_start_in_seconds: int
    start_date_in_schedule: datetime.datetime
    end_date_in_schedule: datetime.datetime

@dataclass
class RunningTurnOnJob:
    timeslot_id: str
    is_temporary: bool

@dataclass
class ResolvedScheduleSlotV2:
    timeslot_id: str
    schedule_id: str  # Added missing schedule_id property
    room_id: str
    day_name: str
    day_order: int
    start_time: str
    end_time: str
    subject: str
    teacher: str
    teacher_email: str
    start_hour: int
    start_minute: int
    end_hour: int
    end_minute: int
    time_start_in_seconds: int | None
    start_date_in_seconds_epoch: float | None
    end_date_in_seconds_epoch: float | None
    is_temporary: bool = False




@dataclass
class ScheduleWrapper:
    schedule_id: str
    upload_date_epoch: float
    is_temporary: bool
    is_synced_to_remote: bool = True  # Default to True for remote sync status
    is_from_remote: bool = True       # Default to True for remote origin


@dataclass
class TemporarySchedule:
    temporary_schedule_id: str
    room_id: str
    day_name: str
    day_order: int
    start_hour: int
    start_minute: int
    end_hour: int
    end_minute: int
    start_date_time: float  # epoch timestamp
    end_date_time: float    # epoch timestamp
    upload_date: float      # epoch timestamp
    teacher: str
    teacher_email: str
    is_temporary: bool = True
    is_synced_to_remote: bool = True  # Default to True for remote sync status
    is_from_remote: bool = True       # Default to True for remote origin
    is_completed: bool = False        # Track if temporary schedule execution is completed
