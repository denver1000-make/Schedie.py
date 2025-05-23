# schedule_model.py

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
class JobLogEntry:
    id: int
    job_turn_on_id: str
    job_turn_off_id: str
    room_id: str
    run_time: str
    result: Optional[str]
    details: Optional[str]


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

@dataclass
class ScheduledJob:
    id: int
    job_id: str
    room_id: str
    job_type: str
    day_order: int
    start_seconds: int
    start_time: str
    end_time: str
    subject: Optional[str]
    teacher: Optional[str]
    teacher_email: Optional[str]
    status: str