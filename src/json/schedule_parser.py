from pydantic import BaseModel, Field
from typing import List, Optional
from src.sql_orm.schedule.resolved_schedule_slots_orm import ResolvedScheduleSlotOrm
from src.modelsV2.model import ResolvedScheduleSlotV2


class TimeslotJson(BaseModel):
    """Pydantic model for timeslot JSON structure"""
    timeslot_id: Optional[str] = Field(default=None, alias="timeslotId")
    start_time: str = Field(alias="startTime")
    end_time: str = Field(alias="endTime")
    subject: Optional[str] = Field(default="", alias="subject")
    teacher: Optional[str] = Field(default="", alias="teacher")
    teacher_email: Optional[str] = Field(default="", alias="teacherEmail")
    start_hour: int = Field(alias="startHour")
    start_minute: int = Field(alias="startMinute")
    end_hour: int = Field(alias="endHour")
    end_minute: int = Field(alias="endMinute")
    time_start_in_seconds: int = Field(alias="timeStartInSeconds")
    
    class Config:
        allow_population_by_field_name = True


class ScheduleDayJson(BaseModel):
    """Pydantic model for schedule day JSON structure"""
    day_name: str = Field(alias="dayName")
    day_order: int = Field(alias="dayOrder")
    hours: List[TimeslotJson] = Field(default_factory=list)
    
    class Config:
        allow_population_by_field_name = True


class ScheduleJson(BaseModel):
    """Pydantic model for individual schedule JSON structure"""
    room_id: str = Field(alias="roomId")
    schedule_of_day_map: List[ScheduleDayJson] = Field(alias="scheduleOfDayMap")
    
    class Config:
        allow_population_by_field_name = True


class ScheduleWrapperJson(BaseModel):
    """Pydantic model for complete schedule wrapper JSON structure"""
    schedule_id: str = Field(alias="scheduleId")
    upload_date: float = Field(alias="uploadDate")
    is_temporary: bool = Field(default=False, alias="isTemporary")
    schedules: List[ScheduleJson] = Field(default_factory=list)
    
    class Config:
        allow_population_by_field_name = True


def parse_schedule_wrapper_json(raw_data: dict) -> ScheduleWrapperJson:
    """
    Parse raw JSON data into ScheduleWrapperJson using Pydantic validation.
    
    Args:
        raw_data: Raw JSON dictionary from MQTT
        
    Returns:
        ScheduleWrapperJson: Validated schedule wrapper object
    """
    return ScheduleWrapperJson.model_validate(raw_data)


def process_raw_schedule_data_pydantic(raw_data: dict) -> ScheduleWrapperJson:
    """
    Process raw schedule data from MQTT using Pydantic validation.
    This handles the new raw format with nested schedules -> scheduleOfDayMap -> hours.
    
    Args:
        raw_data: The raw JSON data from MQTT
        
    Returns:
        ScheduleWrapperJson: Validated schedule wrapper with parsed data
    """
    return ScheduleWrapperJson.model_validate(raw_data)


def extract_resolved_slots_from_json(schedule_wrapper: ScheduleWrapperJson) -> List[ResolvedScheduleSlotOrm]:
    """
    Extract ResolvedScheduleSlotV2 objects from ScheduleWrapperJson.
    
    Args:
        schedule_wrapper: Validated ScheduleWrapperJson object
        
    Returns:
        List[ResolvedScheduleSlotV2]: List of resolved schedule slots
    """
    resolved_slot_list: List[ResolvedScheduleSlotOrm] = []
    
    for sched in schedule_wrapper.schedules:
        for day in sched.schedule_of_day_map:
            for slot in day.hours:
                resolved = ResolvedScheduleSlotOrm(
                    timeslot_id=slot.timeslot_id or "",
                    schedule_id=schedule_wrapper.schedule_id,
                    room_id=sched.room_id,
                    day_name=day.day_name,
                    day_order=day.day_order,
                    start_time=slot.start_time,
                    end_time=slot.end_time,
                    subject=slot.subject or "",
                    teacher=slot.teacher or "",
                    teacher_email=slot.teacher_email or "",
                    start_hour=slot.start_hour,
                    start_minute=slot.start_minute,
                    end_hour=slot.end_hour,
                    end_minute=slot.end_minute,
                    time_start_in_seconds=slot.time_start_in_seconds,
                    start_date_in_seconds_epoch=None,  # Not available in raw format
                    end_date_in_seconds_epoch=None,    # Not available in raw format
                    is_temporary=schedule_wrapper.is_temporary
                )
                resolved_slot_list.append(resolved)
                
    return resolved_slot_list