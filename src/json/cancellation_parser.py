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
    """Pydantic model for flattened cancellation request JSON structure"""
    # Core cancellation fields
    received_by_hub: bool = Field(alias="received_by_hub", default=False)
    cancellation_id: Optional[str] = Field(alias="cancellation_id", default=None)
    teacher_id: str = Field(alias="teacher_id")
    timeslot_id: str = Field(alias="timeslot_id")
    teacher_name: str = Field(alias="teacher_name")
    teacher_email: str = Field(alias="teacher_email")
    day: str = Field(alias="day")
    room_id: str = Field(alias="room_id")
    reason: str = Field(alias="reason")
    accepted: Optional[bool] = Field(alias="accepted", default=None)
    year: int = Field(alias="year")
    month: int = Field(alias="month")
    day_of_month: int = Field(alias="day_of_month")
    is_temporary: bool = Field(alias="is_temporary", default=False)
    
    # Flattened timeslot fields (previously nested in time_slot object)
    start_time: Optional[str] = Field(alias="start_time", default=None)
    end_time: Optional[str] = Field(alias="end_time", default=None)
    subject: Optional[str] = Field(alias="subject", default=None)
    start_hour: Optional[int] = Field(alias="start_hour", default=None)
    start_minute: Optional[int] = Field(alias="start_minute", default=None)
    end_hour: Optional[int] = Field(alias="end_hour", default=None)
    end_minute: Optional[int] = Field(alias="end_minute", default=None)
    teacher: Optional[str] = Field(alias="teacher", default=None)
    time_start_in_seconds: Optional[int] = Field(alias="time_start_in_seconds", default=None)
    start_epoch_in_seconds: Optional[int] = Field(alias="start_epoch_in_seconds", default=None)
    end_epoch_in_seconds: Optional[int] = Field(alias="end_epoch_in_seconds", default=None)
    
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
        f"Cancellation for {cancellation_request.subject or 'Unknown'} class "
        f"in {cancellation_request.room_id} on {cancellation_request.day} "
        f"{cancellation_request.month}/{cancellation_request.day_of_month}/{cancellation_request.year} "
        f"from {cancellation_request.start_time or 'Unknown'} to {cancellation_request.end_time or 'Unknown'}. "
        f"Teacher: {cancellation_request.teacher_name}. "
        f"Reason: {cancellation_request.reason}"
    )