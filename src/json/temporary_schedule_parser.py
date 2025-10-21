from pydantic import BaseModel, Field
from typing import Optional
from src.sql_orm.schedule.resolved_schedule_slots_orm import ResolvedScheduleSlotOrm
from src.sql_orm.schedule.schedule_wrapper_orm import ScheduleWrapperOrm
import uuid


class TemporaryScheduleJson(BaseModel):
    """Pydantic model for temporary schedule JSON structure"""
    timeslot_id: str = Field(alias="timeslotId")
    room_id: str = Field(alias="roomId")
    day_name: str = Field(alias="dayName")
    day_order: int = Field(alias="dayOrder")
    start_hour: int = Field(alias="startHour")
    start_minute: int = Field(alias="startMinute")
    end_hour: int = Field(alias="endHour")
    end_minute: int = Field(alias="endMinute")
    start_date_time: float = Field(alias="startDateTime")
    end_date_time: float = Field(alias="endDateTime")
    upload_date: float = Field(alias="uploadDate")
    teacher: str = Field(alias="teacher")
    teacher_email: str = Field(alias="teacherEmail")
    subject: Optional[str] = Field(default="Temporary Class", alias="subject")
    is_temporary: bool = Field(default=True, alias="isTemporary")
    
    class Config:
        populate_by_name = True


def parse_temporary_schedule_json(raw_data: dict) -> TemporaryScheduleJson:
    """
    Parse raw JSON data into TemporaryScheduleJson using Pydantic validation.
    
    Args:
        raw_data: Raw JSON dictionary from MQTT
        
    Returns:
        TemporaryScheduleJson: Validated temporary schedule object
    """
    return TemporaryScheduleJson.model_validate(raw_data)


def convert_temp_schedule_to_orm(temp_schedule: TemporaryScheduleJson) -> tuple[ScheduleWrapperOrm, ResolvedScheduleSlotOrm]:
    """
    Convert TemporaryScheduleJson to ORM objects for database insertion.
    
    Args:
        temp_schedule: Validated TemporaryScheduleJson object
        
    Returns:
        tuple: (ScheduleWrapperOrm, ResolvedScheduleSlotOrm) ready for database insertion
    """
    # Generate unique schedule_id for the wrapper
    schedule_id = str(uuid.uuid4())
    
    # Create schedule wrapper
    schedule_wrapper = ScheduleWrapperOrm(
        schedule_id=schedule_id,
        upload_date_epoch=temp_schedule.upload_date,
        is_temporary=True,
        is_synced_to_remote=True,
        is_from_remote=True,
        in_use=True  # Temporary schedules are immediately active
    )
    
    # Create resolved schedule slot
    schedule_slot = ResolvedScheduleSlotOrm(
        timeslot_id=temp_schedule.timeslot_id,
        schedule_id=schedule_id,
        room_id=temp_schedule.room_id,
        day_name=temp_schedule.day_name,
        day_order=temp_schedule.day_order,
        start_time=f"{temp_schedule.start_hour:02d}:{temp_schedule.start_minute:02d}",
        end_time=f"{temp_schedule.end_hour:02d}:{temp_schedule.end_minute:02d}",
        subject=temp_schedule.subject or "Temporary Class",
        teacher=temp_schedule.teacher,
        teacher_email=temp_schedule.teacher_email,
        start_hour=temp_schedule.start_hour,
        start_minute=temp_schedule.start_minute,
        end_hour=temp_schedule.end_hour,
        end_minute=temp_schedule.end_minute,
        time_start_in_seconds=(temp_schedule.start_hour * 3600) + (temp_schedule.start_minute * 60),
        start_date_in_seconds_epoch=temp_schedule.start_date_time,
        end_date_in_seconds_epoch=temp_schedule.end_date_time,
        is_temporary=True
    )
    
    return schedule_wrapper, schedule_slot


def get_temp_schedule_summary(temp_schedule: TemporaryScheduleJson) -> str:
    """
    Generate a human-readable summary of the temporary schedule.
    
    Args:
        temp_schedule: Validated TemporaryScheduleJson object
        
    Returns:
        str: Human-readable temporary schedule summary
    """
    return (
        f"Temporary {temp_schedule.subject} class "
        f"in {temp_schedule.room_id} on {temp_schedule.day_name} "
        f"from {temp_schedule.start_hour:02d}:{temp_schedule.start_minute:02d} "
        f"to {temp_schedule.end_hour:02d}:{temp_schedule.end_minute:02d}. "
        f"Teacher: {temp_schedule.teacher}"
    )