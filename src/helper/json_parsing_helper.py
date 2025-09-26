from typing import Any, List
from src.modelsV2.model import ResolvedScheduleSlotV2, TimeSlot, ScheduleOfDay, ScheduleV2
import paho.mqtt.client as mqtt
import psycopg2.pool as pool


def process_raw_schedule_data(raw_data: dict, schedule_id: str) -> List[ResolvedScheduleSlotV2]:
    """
    Process raw schedule data from MQTT and convert it to ResolvedScheduleSlotV2 format.
    This handles the new raw format with nested schedules -> scheduleOfDayMap -> hours.
    
    Args:
        raw_data: The raw JSON data from MQTT
        schedule_id: The schedule ID to assign to all slots
        
    Returns:
        List[ResolvedScheduleSlotV2]: List of resolved schedule slots
    """
    # Extract schedules array
    schedules_array = raw_data.get("schedules", [])
    all_schedules = []
    
    for schedule in schedules_array:
        room_id = schedule.get("roomId")
        schedule_map = schedule.get("scheduleOfDayMap", [])
        schedule_days = []
        
        for day in schedule_map:
            day_name = day.get("dayName")
            day_order = day.get("dayOrder", 0)
            raw_hours = day.get("hours", [])
            
            time_slots = [
                TimeSlot(
                    timeslot_id=slot.get("timeslotId"),
                    start_time=slot.get("startTime"),
                    end_time=slot.get("endTime"),
                    subject=slot.get("subject"),
                    teacher=slot.get("teacher"),
                    start_hour=slot.get("startHour"),
                    start_minute=slot.get("startMinute"),
                    end_hour=slot.get("endHour"),
                    end_minute=slot.get("endMinute"),
                    teacher_email=slot.get("teacherEmail"),
                    time_start_in_seconds=int(slot.get("timeStartInSeconds", 0))
                )
                for slot in raw_hours
            ]
            
            time_slots.sort(key=lambda t: t.time_start_in_seconds)
            
            schedule_days.append(ScheduleOfDay(
                day_name=day_name,
                day_order=day_order,
                hours=time_slots
            ))
        
        schedule_days.sort(key=lambda d: d.day_order)
        
        all_schedules.append(ScheduleV2(
            room_id=room_id,
            schedule_days=schedule_days
        ))
    
    # Extract resolved slots
    resolved_slots = extract_resolved_slots_v2(all_schedules, schedule_id)
    return resolved_slots


def extract_resolved_slots_v2(schedules: List[ScheduleV2], schedule_id: str) -> List[ResolvedScheduleSlotV2]:
    """
    Extract ResolvedScheduleSlotV2 objects from ScheduleV2 objects.
    
    Args:
        schedules: List of ScheduleV2 objects
        schedule_id: The schedule ID to assign to all slots
        
    Returns:
        List[ResolvedScheduleSlotV2]: List of resolved schedule slots
    """
    resolved_slot_list: List[ResolvedScheduleSlotV2] = []
    
    for sched in schedules:
        for day in sched.schedule_days:
            for slot in day.hours:
                resolved = ResolvedScheduleSlotV2(
                    timeslot_id=slot.timeslot_id or "",
                    schedule_id=schedule_id,
                    room_id=sched.room_id,
                    day_name=day.day_name,
                    day_order=day.day_order,
                    start_time=slot.start_time,
                    end_time=slot.end_time,
                    subject=slot.subject or "",
                    teacher=slot.teacher or "",
                    teacher_email=slot.teacher_email or "",
                    start_hour=slot.start_hour or 0,
                    start_minute=slot.start_minute or 0,
                    end_hour=slot.end_hour or 0,
                    end_minute=slot.end_minute or 0,
                    time_start_in_seconds=slot.time_start_in_seconds,
                    start_date_in_seconds_epoch=slot.start_time_date_epoch,
                    end_date_in_seconds_epoch=slot.end_time_date_epoch,
                    is_temporary=getattr(slot, 'is_temporary', False)
                )
                resolved_slot_list.append(resolved)
                
    return resolved_slot_list

def parse_schedule_json(
    jsonData: Any, 
    message: mqtt.MQTTMessage,
    pg_conn_pool: pool.SimpleConnectionPool, 
    schedule_id: str
) -> List[ResolvedScheduleSlotV2]:
    resolved_slots_data = jsonData.get("resolved_slots", [])
    slots: List[ResolvedScheduleSlotV2] = []
    for slot_data in resolved_slots_data:
            # Map the JSON fields to your model fields
            slot = ResolvedScheduleSlotV2(
                schedule_id=schedule_id,
                timeslot_id=slot_data["timeslot_id"],
                room_id=slot_data["room_id"],
                day_name=slot_data["day_name"],
                day_order=slot_data["day_order"],
                start_time=slot_data["start_time"],
                end_time=slot_data["end_time"],
                subject=slot_data.get("subject", ""),
                teacher=slot_data.get("teacher", ""),
                teacher_email=slot_data.get("teacher_email", ""),
                end_hour=slot_data.get("end_hour", -1),
                end_minute=slot_data.get("end_minute", -1),
                start_hour=slot_data.get("start_hour", -1),
                start_minute=slot_data.get("start_minute", -1),
                time_start_in_seconds=slot_data.get("start_time_seconds"),
                start_date_in_seconds_epoch=slot_data.get("start_date_in_seconds_epoch"),
                end_date_in_seconds_epoch=slot_data.get("end_date_in_seconds_epoch")
            )
            slots.append(slot)
    return slots