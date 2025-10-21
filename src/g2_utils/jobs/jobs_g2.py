


from src.sql_orm.cancellation.cancelled_schedule_orm import check_if_timeslot_cancelled, remove_cancelled_info_orm_by_timeslot_id
from src.sql_orm.schedule.resolved_schedule_slots_orm import get_nearby_schedules_for_room_and_day, ResolvedScheduleSlotOrm
from src.g2_utils.mqtt.mqtt_funcs_g2 import mqtt_turn_off, mqtt_turn_on, send_schedule_ending, send_shutdown_warning
from src.sql_orm.turn_on_jobs.turn_on_job_orm import RunningTurnOnJobOrm, get_running_job, insert_running_job
from src.sql_orm.connection.sqlalchemy_pg import get_session
from src.constants import DEVICE_TZ, MINUTE_MARK_TO_SKIP
from paho.mqtt.client import Client

def get_schedule_slot_by_timeslot_id(timeslot_id: str) -> ResolvedScheduleSlotOrm | None:
    """
    Fetch a ResolvedScheduleSlotOrm by its timeslot_id from the database.
    """
    session = get_session()
    try:
        return session.query(ResolvedScheduleSlotOrm).filter(
            ResolvedScheduleSlotOrm.timeslot_id == timeslot_id
        ).first()
    finally:
        session.close()

def turn_on_proc(
    timeslot_id: str,
    mqtt_client: Client
):
    # Fetch the schedule slot from database
    resolved_schedule_slot_orm = get_schedule_slot_by_timeslot_id(timeslot_id)
    if not resolved_schedule_slot_orm:
        print(f"Schedule slot not found for timeslot_id: {timeslot_id}")
        return
    
    # Check for date-specific cancellation
    from datetime import datetime
    
    today_date = datetime.now(tz=DEVICE_TZ).strftime("%Y-%m-%d")
    
    from src.sql_orm.cancellation.cancelled_schedule_orm import check_if_timeslot_cancelled_on_date
    
    cancellation_info = check_if_timeslot_cancelled_on_date(
        timeslot_id=timeslot_id,
        date_str=today_date
    )
    
    if cancellation_info:
        print(f"⚠️ Schedule {timeslot_id} cancelled for {today_date} - skipping turn on")
        return 
    
    running_job = get_running_job(
        timeslot_id=timeslot_id
    )
    
    if not running_job:
        
        mqtt_turn_on(
            resolved_schedule_slot_orm.room_id,
            mqtt_client
        )
        
        insert_running_job(
            running_job=RunningTurnOnJobOrm(
                timeslot_id = timeslot_id,
                is_temporary = False
            )
        )
    
def turn_off_proc(
    timeslot_id: str,
    mqtt_client: Client,
    minute_mark_to_skip: int = MINUTE_MARK_TO_SKIP
):
    # Fetch the schedule slot from database
    resolved_schedule_slot_orm = get_schedule_slot_by_timeslot_id(timeslot_id)
    if not resolved_schedule_slot_orm:
        print(f"Schedule slot not found for timeslot_id: {timeslot_id}")
        return
    
    # Check for date-specific cancellation
    from datetime import datetime
    
    today_date = datetime.now(tz=DEVICE_TZ).strftime("%Y-%m-%d")
    
    from src.sql_orm.cancellation.cancelled_schedule_orm import check_if_timeslot_cancelled_on_date
    
    cancelled_schedule_info = check_if_timeslot_cancelled_on_date(
        timeslot_id=timeslot_id,
        date_str=today_date
    )
    
    if cancelled_schedule_info:
        print(f"Cancellation Detected for Timeslot {timeslot_id} on {today_date} - skipping turn off")
        return
    
    # Check if a nearby schedule is present
    nearby_timeslot = get_nearby_schedules_for_room_and_day(
        current_end_hour=resolved_schedule_slot_orm.end_hour,
        current_end_minute=resolved_schedule_slot_orm.end_minute,
        day_name=resolved_schedule_slot_orm.day_name,
        minute_mark_to_skip=minute_mark_to_skip,
        room_id=resolved_schedule_slot_orm.room_id
    )
    
    should_turn_off = True  # Default to turning off
    
    if nearby_timeslot:
        print(f"[TURN_OFF_PROC] {timeslot_id} has a nearby sched {nearby_timeslot.timeslot_id}")
        # Check if nearby schedule is cancelled for today
        is_nearby_slot_cancelled = check_if_timeslot_cancelled_on_date(
            timeslot_id=nearby_timeslot.timeslot_id,
            date_str=today_date
        )
        
        # Only keep on if nearby schedule exists and is NOT cancelled
        if not is_nearby_slot_cancelled:
            should_turn_off = False
        
        print(f"[TURN_OFF_PROC] {nearby_timeslot.timeslot_id} is_cancelled: {bool(is_nearby_slot_cancelled)}")
    
    # Turn off if no nearby schedule or nearby schedule is cancelled
    if should_turn_off:
        print(f"[TURN_OFF_PROC] Turning off {timeslot_id} on Room {resolved_schedule_slot_orm.room_id}")
        mqtt_turn_off(
            mqtt_client=mqtt_client,
            room_id=resolved_schedule_slot_orm.room_id
        )
    else:
        print(f"[TURN_OFF_PROC] Skipping turn off procedure for {timeslot_id} on Room {resolved_schedule_slot_orm.room_id}")
    

def warning_proc(
    timeslot_id: str,
    minute_mark_to_skip: int,
    mqtt_client: Client,  
):
    # Fetch the schedule slot from database
    resolved_schedule_slot_orm = get_schedule_slot_by_timeslot_id(timeslot_id)
    if not resolved_schedule_slot_orm:
        print(f"Schedule slot not found for timeslot_id: {timeslot_id}")
        return
    
    nearby_timeslot = get_nearby_schedules_for_room_and_day(
        current_end_hour=resolved_schedule_slot_orm.end_hour,
        current_end_minute=resolved_schedule_slot_orm.end_minute,
        day_name=resolved_schedule_slot_orm.day_name,
        minute_mark_to_skip=minute_mark_to_skip,
        room_id=resolved_schedule_slot_orm.room_id
    )
    
    print(f"Nearby timeslot {nearby_timeslot}")
    
    if nearby_timeslot:
        send_schedule_ending(
            resolved_schedule_slot_orm.room_id,
            mqtt_client=mqtt_client
        )
    else:
        send_shutdown_warning(
            resolved_schedule_slot_orm.room_id,
            mqtt_client
        )