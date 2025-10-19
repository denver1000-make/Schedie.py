from pydantic import BaseModel, Field
from typing import Optional


class TimeSlotJson(BaseModel):
    """Pydantic model for timeslot JSON structure in cancellation"""
    end_hour: int = Field(alias="endHour")
    end_minute: int = Field(alias="endMinute")
    end_time: str = Field(alias="endTime")
    is_temporary: bool = Field(alias="isTemporary")
    start_hour: int = Field(alias="startHour")
    start_minute: int = Field(alias="startMinute")
    start_time: str = Field(alias="startTime")
    subject: str = Field(alias="subject")
    teacher: str = Field(alias="teacher")
    teacher_email: str = Field(alias="teacherEmail")
    time_start_in_seconds: int = Field(alias="timeStartInSeconds")
    timeslot_id: str = Field(alias="timeslotId")
    
    class Config:
        populate_by_name = True


class CancellationRequestJson(BaseModel):
    """Pydantic model for cancellation request JSON structure"""
    day: str = Field(alias="day")
    day_of_month: int = Field(alias="day_of_month")
    id: str = Field(alias="id")
    month: int = Field(alias="month")
    reason: str = Field(alias="reason")
    room_id: str = Field(alias="room_id")
    teacher_email: str = Field(alias="teacher_email")
    teacher_id: str = Field(alias="teacher_id")
    teacher_name: str = Field(alias="teacher_name")
    time_slot: TimeSlotJson = Field(alias="time_slot")
    cancellation_id: str = Field(alias="")
    timeslot_id: str = Field(alias="timeslot_id")
    year: int = Field(alias="year")
    
    class Config:
        populate_by_name = True


def parse_cancellation_request_json(raw_data: dict) -> CancellationRequestJson:
    """
    Parse raw cancellation JSON data into CancellationRequestJson using Pydantic validation.
    
    Args:
        raw_data: Raw JSON dictionary from MQTT
        
    Returns:
        CancellationRequestJson: Validated cancellation request object
    """
    return CancellationRequestJson.model_validate(raw_data)


def extract_cancellation_info_for_orm(cancellation_request: CancellationRequestJson) -> dict:
    """
    Extract cancellation information in a format suitable for CancelledScheduleOrm.
    
    Args:
        cancellation_request: Validated CancellationRequestJson object
        
    Returns:
        dict: Dictionary with fields for CancelledScheduleOrm creation
    """
    import datetime
    
    # Format the cancelled date as YYYY-MM-DD
    cancelled_date = f"{cancellation_request.year:04d}-{cancellation_request.month:02d}-{cancellation_request.day_of_month:02d}"
    
    return {
        "timeslot_id": cancellation_request.timeslot_id,
        "cancellation_type": "permanent_instance",  # Based on the specific date
        "cancelled_at": datetime.datetime.utcnow(),
        "cancelled_date": cancelled_date,
        "reason": cancellation_request.reason,
        "cancelled_by": cancellation_request.teacher_email
    }


def get_cancellation_summary(cancellation_request: CancellationRequestJson) -> str:
    """
    Generate a human-readable summary of the cancellation request.
    
    Args:
        cancellation_request: Validated CancellationRequestJson object
        
    Returns:
        str: Human-readable cancellation summary
    """
    return (
        f"Cancellation for {cancellation_request.time_slot.subject} class "
        f"in {cancellation_request.room_id} on {cancellation_request.day} "
        f"{cancellation_request.month}/{cancellation_request.day_of_month}/{cancellation_request.year} "
        f"from {cancellation_request.time_slot.start_time} to {cancellation_request.time_slot.end_time}. "
        f"Teacher: {cancellation_request.teacher_name}. "
        f"Reason: {cancellation_request.reason}"
    )