
import datetime
from typing import List
from src.helper.job_proximity_helper import is_time_within_timeslot
from src.helper.trigger_creation_helper import get_datetime_from_epoch
from src.constants import DEVICE_TZ, JOB_TURN_ON_SUFFIX, JOB_TURN_OFF_SUFFIX, JobStatus
from src.modelsV2.model import ResolvedScheduleSlotV2, RunningTurnOnJob
from src.mqtt.mqtt_functions import turn_off, turn_on, update_timeslot_status
from src.sql.running_turn_on_job.running_turn_on_job_sql import pg_insert_running_turn_on_job
from src.schedulerv2.scheduler_v2 import gen_job_name, parse_time
from src.utils.time_utils.time_utils import strip_date_of_start_time_v2
from psycopg2 import pool
import paho.mqtt.client as mqtt
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.base import BaseTrigger

def turn_on_with_tracking(
    room_id: str,
    job_turn_off_id: str,
    job_turn_on_id: str,
    is_temporary: bool,
    mqtt_client: mqtt.Client,
    pg_pool: pool.SimpleConnectionPool
):
    """Wrapper for turn_on that handles running job tracking with correct is_temporary value"""
    
    # Extract timeslot_id first
    if not job_turn_on_id.endswith(JOB_TURN_ON_SUFFIX):
        print(f"[ERROR] Invalid job ID format: {job_turn_on_id}")
        return
        
    timeslot_id = job_turn_on_id[:-len(JOB_TURN_ON_SUFFIX)]  # Remove suffix
    
    # Core control mechanism: Check if there's a valid running job before proceeding
    from src.sql.running_turn_on_job.running_turn_on_job_sql import pg_get_running_job_with_schedule_slot
    
    try:
        running_job, schedule_slot = pg_get_running_job_with_schedule_slot(pg_pool, timeslot_id)
        if not running_job:
            print(f"[NO_RUNNING_JOB] Turn on skipped for Room {room_id} - no running job found (likely cancelled)")
            return
        else:
            print(f"[RUNNING_JOB_FOUND] Turn on proceeding for Room {room_id} - running job exists")
    except Exception as e:
        print(f"[ERROR] Failed to check running job for {timeslot_id}: {e}")
        return
    
    # Call the original turn_on function
    turn_on(room_id, job_turn_off_id, job_turn_on_id, mqtt_client, pg_pool)
    
    # Log the running job
    if pg_pool is not None:
        running_job = RunningTurnOnJob(
            timeslot_id=timeslot_id,
            is_temporary=is_temporary
        )
        
        # Insert running job record
        pg_insert_running_turn_on_job(pg_pool, running_job)

def schedule_v3_with_immediate_check(
    list_resolved_slots: List[ResolvedScheduleSlotV2],
    scheduler_v2,  # Can be BackgroundScheduler or AsyncIOScheduler
    mqtt_client: mqtt.Client,
    pg_conn_pool: pool.SimpleConnectionPool,
    isTemp: bool,
    current_time: datetime.datetime
):
    """
    Enhanced version of schedule_v3 that checks for immediate execution needs
    
    For regular schedules (non-temp):
    - If current time overlaps with a schedule slot for today, execute immediately
    - Still schedule the regular cron jobs for future occurrences
    
    For temp schedules:
    - Uses DateTrigger with start_date_in_seconds_epoch and end_date_in_seconds_epoch
    """
    current_day_name = current_time.strftime('%A')  # Monday, Tuesday, etc.
    current_time_seconds = current_time.hour * 3600 + current_time.minute * 60 + current_time.second
    
    print(f"🕐 Current time: {current_time.strftime('%A %I:%M:%S %p')}")
    print(f"🕐 Current time in seconds: {current_time_seconds}")
    
    for timeslot in list_resolved_slots:
        # Use timeslot_id with suffixes for unique job names
        turn_on_name = f"{timeslot.timeslot_id}{JOB_TURN_ON_SUFFIX}"
        turn_off_name = f"{timeslot.timeslot_id}{JOB_TURN_OFF_SUFFIX}"

        turnOnBaseTrigger: BaseTrigger
        turnOffBaseTrigger: BaseTrigger
        
        if isTemp:
            # For temporary schedules, use DateTrigger with epoch timestamps
            print(f"[TEMP_SCHEDULE] Using DateTrigger for temporary schedule {timeslot.timeslot_id}")
            
            # Validate epoch timestamps exist for temporary schedules
            if not timeslot.start_date_in_seconds_epoch or not timeslot.end_date_in_seconds_epoch:
                print(f"[ERROR] Missing epoch timestamps for temporary schedule {timeslot.timeslot_id}")
                print(f"        start_epoch: {timeslot.start_date_in_seconds_epoch}")
                print(f"        end_epoch: {timeslot.end_date_in_seconds_epoch}")
                continue  # Skip this slot
            
            # Convert epoch timestamps to datetime objects
            start_datetime = datetime.datetime.fromtimestamp(
                timeslot.start_date_in_seconds_epoch, 
                tz=DEVICE_TZ
            )
            end_datetime = datetime.datetime.fromtimestamp(
                timeslot.end_date_in_seconds_epoch, 
                tz=DEVICE_TZ
            )
            
            print(f"[TEMP_SCHEDULE] Start: {start_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"[TEMP_SCHEDULE] End: {end_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            
            turnOnBaseTrigger = DateTrigger(run_date=start_datetime, timezone=DEVICE_TZ)
            turnOffBaseTrigger = DateTrigger(run_date=end_datetime, timezone=DEVICE_TZ)
            
            # For immediate execution check, use the datetime objects
            parsed_start_time = start_datetime.time()
            parsed_end_time = end_datetime.time()
            
        else:
            # For regular schedules, use CronTrigger with proper keyword arguments
            parsed_start_time = parse_time(timeslot.start_time)
            parsed_end_time = parse_time(timeslot.end_time)
            
            # Map day names to APScheduler day names
            day_name_map = {
                'Monday': 'mon', 'Tuesday': 'tue', 'Wednesday': 'wed',
                'Thursday': 'thu', 'Friday': 'fri', 'Saturday': 'sat', 'Sunday': 'sun'
            }
                
            scheduler_day = day_name_map.get(timeslot.day_name, timeslot.day_name.lower()[:3])
            
            # Create turn_on trigger with normal start time
            turnOnBaseTrigger = CronTrigger(
                    hour=parsed_start_time.hour,
                    minute=parsed_start_time.minute,
                    day_of_week=scheduler_day,
                    timezone=DEVICE_TZ
                )
            
            # Create turn_off trigger with 1-minute earlier time to prevent race conditions
            # This ONLY affects when the job runs, NOT the stored schedule data
            trigger_end_minutes = parsed_end_time.minute - 1
            trigger_end_hour = parsed_end_time.hour
            
            if trigger_end_minutes < 0:
                trigger_end_minutes = 59
                trigger_end_hour = trigger_end_hour - 1 if trigger_end_hour > 0 else 23
            
            print(f"[RACE_PREVENTION] Turn_off trigger scheduled 1 minute early: {parsed_end_time.strftime('%H:%M')} -> {trigger_end_hour:02d}:{trigger_end_minutes:02d}")
            print(f"                  (Database schedule data remains unchanged: {parsed_end_time.strftime('%H:%M')})")
                
            turnOffBaseTrigger = CronTrigger(
                    hour=trigger_end_hour,
                    minute=trigger_end_minutes,
                    day_of_week=scheduler_day,
                    timezone=DEVICE_TZ
                )
            
        # Debug immediate execution check
        print(f"[DEBUG] Checking immediate execution for {timeslot.room_id}:")
        print(f"        Slot: {timeslot.day_name} {timeslot.start_time}-{timeslot.end_time}")
        print(f"        Current: {current_day_name} {current_time.strftime('%H:%M:%S')}")
        print(f"        Start time seconds: {(parsed_start_time.hour * 3600) + (parsed_start_time.minute * 60)}")
        print(f"        Current time seconds: {current_time_seconds}")
        print(f"        End time seconds: {(parsed_end_time.hour * 3600) + (parsed_end_time.minute * 60)}")
        
        is_current_time_within_slot = is_time_within_timeslot(
                timeslot_day_name=timeslot.day_name.lower(),
                current_day_name=current_day_name.lower(),
                current_time=current_time.time(),
                parsed_start_time = parsed_start_time,
                parsed_end_time = parsed_end_time
            )
        
        print(f"        Within slot: {is_current_time_within_slot}")
            
        if is_current_time_within_slot:
                print(f"🚨 IMMEDIATE EXECUTION NEEDED for {timeslot.room_id}")
                print(f"📅 Schedule: {timeslot.day_name} {timeslot.start_time}-{timeslot.end_time}")
                print(f"🏢 Room: {timeslot.room_id} | Subject: {timeslot.subject}")
                # Execute turn_on immediately with proper tracking
                turn_on_with_tracking(
                    room_id=timeslot.room_id,
                    job_turn_off_id=turn_off_name,
                    job_turn_on_id=turn_on_name,
                    is_temporary=isTemp,
                    mqtt_client=mqtt_client,
                    pg_pool=pg_conn_pool
                )
                
                update_timeslot_status(
                    mqtt_client=mqtt_client,
                    status=JobStatus.JOB_ONGOING,
                    subject=timeslot.subject,
                    is_temp=isTemp,
                    timeslot_id=timeslot.timeslot_id
                )
                
        update_timeslot_status(
            mqtt_client=mqtt_client,
            status=JobStatus.JOB_STANDBY,
            subject=timeslot.subject,
            is_temp=isTemp,
            timeslot_id=timeslot.timeslot_id
        )
        
        scheduler_v2.add_job(turn_on_with_tracking, trigger=turnOnBaseTrigger, 
                            id=turn_on_name, args=[
            timeslot.room_id,
            turn_off_name,
            turn_on_name,
            isTemp,
            mqtt_client,
            pg_conn_pool,
            
        ])
        
        scheduler_v2.add_job(turn_off, trigger=turnOffBaseTrigger,
                            id=turn_off_name, args=[
            timeslot.room_id,
            turn_off_name,
            turn_on_name,
            isTemp,
            mqtt_client,
            pg_conn_pool,
            
        ])

        print(f"[SCHEDULED] Added turn_on and turn_off jobs for {timeslot.room_id} - {timeslot.subject}")

    # Schedule warning jobs for all slots after main scheduling is complete
    print(f"[WARNINGS] Scheduling warning jobs for {len(list_resolved_slots)} slots")
    from src.schedulerv2.warning_funcs import schedule_warning_jobs_for_slots
    
    schedule_warning_jobs_for_slots(
        slots=list_resolved_slots,
        scheduler_v2=scheduler_v2,
        mqtt_client=mqtt_client,
        pg_conn_pool=pg_conn_pool
    )
    
    print(f"[COMPLETED] Successfully scheduled {len(list_resolved_slots)} schedule slots with warnings")