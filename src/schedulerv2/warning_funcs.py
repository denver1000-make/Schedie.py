import datetime
import json
from typing import Any, List, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from psycopg2 import pool
import paho.mqtt.client as mqtt

from src.modelsV2.model import ResolvedScheduleSlotV2
from src.mqtt.mqtt_manager import publish_v2
from src.mqtt.mqtt_functions import send_room_shutdown_warning_mqtt, send_schedule_ending_mqtt, send_skipped_turn_off_mqtt

# Import global constants
from src.constants import DEVICE_TZ, MINUTE_MARK_TO_WARN

def send_schedule_warning_orm(room_id: str, 
    subject: str, 
    end_time: str, 
    mqtt_client: mqtt.Client,
    session: Any,
    current_end_hour: int,
    current_end_minute: int,
    day_name: str,
    timeslot_id: Optional[str] = None):
    try:
        # Check if there's an active running job for this timeslot
        if timeslot_id:
            from src.sql.running_turn_on_job.running_turn_on_job_sql import pg_get_running_turn_on_job
            
            running_job = pg_get_running_turn_on_job(pg_conn_pool, timeslot_id)
            if not running_job:
                print(f"[SKIP_WARNING] No running job found for timeslot {timeslot_id} - skipping warning")
                print(f"                Room {room_id} ({subject}) was never turned on, no need to warn")
                return
            else:
                print(f"[PROCEED_WARNING] Running job found for timeslot {timeslot_id} - sending warning")
        
        # Import here to avoid circular imports
        from src.mqtt.mqtt_functions import check_skip_and_send_warning
        
        # Use the integrated skip check and warning function
        will_skip = check_skip_and_send_warning(
            pg_conn_pool=pg_conn_pool,
            room_id=room_id,
            current_end_hour=current_end_hour,
            current_end_minute=current_end_minute,
            day_name=day_name,
            subject=subject,
            mqtt_client=mqtt_client,
            timeslot_id=timeslot_id  # Now pass the actual timeslot_id
        )
        
        action = "skip_turn_off" if will_skip else "close_room"
        print(f"[WARNING] Schedule warning sent for {room_id}: {action} (warning job only - turn_off handles actual skip)")
        
    except Exception as e:
        print(f"[ERROR] Error sending schedule warning: {e}")
        # Fallback: send basic shutdown warning message
        try:
            send_room_shutdown_warning_mqtt(
                room_id,
                subject,
                "schedule_ending",
                mqtt_client,
                end_time=end_time,
                message=f"Room {room_id} ({subject}) will close at {end_time}"
            )
        except:
            pass

def send_schedule_warning(
    room_id: str, 
    subject: str, 
    end_time: str, 
    mqtt_client: mqtt.Client,
    pg_conn_pool: pool.SimpleConnectionPool,
    current_end_hour: int,
    current_end_minute: int,
    day_name: str,
    timeslot_id: Optional[str] = None
):
    """
    Send schedule warning with integrated skip logic.
    Only sends warnings if there's an active running job for the timeslot.
    Determines whether to send "close room" or "skip turn off" message based on nearby schedules.
    
    Args:
        room_id: Room ID
        subject: Schedule subject
        end_time: End time string (HH:MM format)
        mqtt_client: MQTT client
        pg_conn_pool: Database connection pool for skip logic
        current_end_hour: Current schedule end hour
        current_end_minute: Current schedule end minute  
        day_name: Current day name
        timeslot_id: Timeslot ID to check for running job (optional for backward compatibility)
    """
    try:
        # Check if there's an active running job for this timeslot
        if timeslot_id:
            from src.sql.running_turn_on_job.running_turn_on_job_sql import pg_get_running_turn_on_job
            
            running_job = pg_get_running_turn_on_job(pg_conn_pool, timeslot_id)
            if not running_job:
                print(f"[SKIP_WARNING] No running job found for timeslot {timeslot_id} - skipping warning")
                print(f"                Room {room_id} ({subject}) was never turned on, no need to warn")
                return
            else:
                print(f"[PROCEED_WARNING] Running job found for timeslot {timeslot_id} - sending warning")
        
        # Import here to avoid circular imports
        from src.mqtt.mqtt_functions import check_skip_and_send_warning
        
        # Use the integrated skip check and warning function
        will_skip = check_skip_and_send_warning(
            pg_conn_pool=pg_conn_pool,
            room_id=room_id,
            current_end_hour=current_end_hour,
            current_end_minute=current_end_minute,
            day_name=day_name,
            subject=subject,
            mqtt_client=mqtt_client,
            timeslot_id=timeslot_id  # Now pass the actual timeslot_id
        )
        
        action = "skip_turn_off" if will_skip else "close_room"
        print(f"[WARNING] Schedule warning sent for {room_id}: {action} (warning job only - turn_off handles actual skip)")
        
    except Exception as e:
        print(f"[ERROR] Error sending schedule warning: {e}")
        # Fallback: send basic shutdown warning message
        try:
            send_room_shutdown_warning_mqtt(
                room_id,
                subject,
                "schedule_ending",
                mqtt_client,
                end_time=end_time,
                message=f"Room {room_id} ({subject}) will close at {end_time}"
            )
        except:
            pass

def schedule_warning_jobs_for_slots_orm(
    slots: List[ResolvedScheduleSlotV2],
    scheduler_v2: BackgroundScheduler,
    mqtt_client: mqtt.Client,
    session: Any
): 
    """
    Schedule warning jobs for all slots based on minute_mark_to_warn setting.
    Supports both regular (CronTrigger) and temporary (DateTrigger) schedules.
    
    Args:
        slots: List of schedule slots to create warning jobs for
        scheduler_v2: APScheduler instance
        mqtt_client: MQTT client for publishing warnings
        pg_conn_pool: Database connection pool to get settings
    """
    try:
        # Use global hardcoded setting instead of database lookup
        minute_mark_to_warn = MINUTE_MARK_TO_WARN
        
        print(f"[WARNINGS] Scheduling warning jobs with {minute_mark_to_warn}-minute threshold (hardcoded)")
        
        # APScheduler day name mapping
        day_name_map = {
            'Monday': 'mon', 'Tuesday': 'tue', 'Wednesday': 'wed',
            'Thursday': 'thu', 'Friday': 'fri', 'Saturday': 'sat', 'Sunday': 'sun'
        }
        
        warning_jobs_scheduled = 0
        
        for slot in slots:
            try:
                # Generate unique job ID for the warning
                warning_job_id = f"{slot.timeslot_id}_warning"
                
                if slot.is_temporary and slot.end_date_in_seconds_epoch:
                    # TEMPORARY SCHEDULES: Use DateTrigger with specific end time
                    print(f"[TEMPORARY] Scheduling warning for: {slot.room_id} - {slot.subject}")
                    
                    # Convert epoch end time to datetime and subtract warning minutes
                    end_datetime = datetime.datetime.fromtimestamp(
                        slot.end_date_in_seconds_epoch,
                        tz=datetime.timezone(datetime.timedelta(hours=8))  # Asia/Manila
                    )
                    
                    warning_datetime = end_datetime - datetime.timedelta(minutes=minute_mark_to_warn)
                    
                    # Skip if warning time is in the past
                    current_time = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=8)))
                    if warning_datetime <= current_time:
                        print(f"[SKIP] Warning time in past for temporary schedule: {slot.subject}")
                        continue
                    
                    print(f"   Warning at: {warning_datetime.strftime('%Y-%m-%d %I:%M:%S %p %Z')}")
                    
                    # Schedule the warning job with DateTrigger
                    scheduler_v2.add_job(
                        send_schedule_warning,
                        trigger=DateTrigger(
                            run_date=warning_datetime,
                            timezone=DEVICE_TZ  # Asia/Manila
                        ),
                        args=[
                            slot.room_id, 
                            slot.subject, 
                            slot.end_time, 
                            mqtt_client,
                            session,
                            end_datetime.hour,
                            end_datetime.minute,
                            end_datetime.strftime('%A'),  # Day name like "Monday"
                            slot.timeslot_id  # Pass timeslot_id to check for running jobs
                        ],
                        id=warning_job_id,
                        replace_existing=True
                    )
                    
                else:
                    # REGULAR SCHEDULES: Use CronTrigger for recurring warnings
                    print(f"[REGULAR] Scheduling warning for: {slot.room_id} - {slot.subject}")
                    
                    # Calculate warning time (end_time - minute_mark_to_warn)
                    warn_hour = slot.end_hour
                    warn_minute = slot.end_minute - minute_mark_to_warn
                    
                    # Handle minute underflow (e.g., 10:05 - 10 minutes = 9:55)
                    while warn_minute < 0:
                        warn_minute += 60
                        warn_hour -= 1
                    
                    # Skip warnings that would be scheduled before midnight or at invalid times
                    if warn_hour < 0 or warn_hour > 23:
                        print(f"[SKIP] Invalid warning time for {slot.subject}")
                        continue
                    
                    # Get scheduler day format
                    scheduler_day = day_name_map.get(slot.day_name, slot.day_name.lower()[:3])
                    
                    # Schedule the warning job with CronTrigger
                    scheduler_v2.add_job(
                        send_schedule_warning,
                        trigger=CronTrigger(
                            hour=warn_hour,
                            minute=warn_minute,
                            day_of_week=scheduler_day,
                            timezone=datetime.timezone(datetime.timedelta(hours=8))  # Asia/Manila
                        ),
                        args=[
                            slot.room_id, 
                            slot.subject, 
                            slot.end_time, 
                            mqtt_client,
                            session,
                            slot.end_hour,
                            slot.end_minute,
                            slot.day_name,
                            slot.timeslot_id  # Pass timeslot_id to check for running jobs
                        ],
                        id=warning_job_id,
                        replace_existing=True  # Allow updating existing warnings
                    )
                
                warning_jobs_scheduled += 1
                print(f"[SUCCESS] Warning scheduled: {slot.room_id} '{slot.subject}' ({'TEMP' if slot.is_temporary else 'REGULAR'})")
                
            except Exception as e:
                print(f"[ERROR] Error scheduling warning for slot {slot.timeslot_id}: {e}")
        
        print(f"[COMPLETED] Successfully scheduled {warning_jobs_scheduled} warning jobs")
        
    except Exception as e:
        print(f"[ERROR] Error in schedule_warning_jobs_for_slots: {e}")

def schedule_warning_jobs_for_slots(
    slots: List[ResolvedScheduleSlotV2],
    scheduler_v2: BackgroundScheduler,
    mqtt_client: mqtt.Client,
    pg_conn_pool: pool.SimpleConnectionPool
):
    """
    Schedule warning jobs for all slots based on minute_mark_to_warn setting.
    Supports both regular (CronTrigger) and temporary (DateTrigger) schedules.
    
    Args:
        slots: List of schedule slots to create warning jobs for
        scheduler_v2: APScheduler instance
        mqtt_client: MQTT client for publishing warnings
        pg_conn_pool: Database connection pool to get settings
    """
    try:
        # Use global hardcoded setting instead of database lookup
        minute_mark_to_warn = MINUTE_MARK_TO_WARN
        
        print(f"[WARNINGS] Scheduling warning jobs with {minute_mark_to_warn}-minute threshold (hardcoded)")
        
        # APScheduler day name mapping
        day_name_map = {
            'Monday': 'mon', 'Tuesday': 'tue', 'Wednesday': 'wed',
            'Thursday': 'thu', 'Friday': 'fri', 'Saturday': 'sat', 'Sunday': 'sun'
        }
        
        warning_jobs_scheduled = 0
        
        for slot in slots:
            try:
                # Generate unique job ID for the warning
                warning_job_id = f"{slot.timeslot_id}_warning"
                
                if slot.is_temporary and slot.end_date_in_seconds_epoch:
                    # TEMPORARY SCHEDULES: Use DateTrigger with specific end time
                    print(f"[TEMPORARY] Scheduling warning for: {slot.room_id} - {slot.subject}")
                    
                    # Convert epoch end time to datetime and subtract warning minutes
                    end_datetime = datetime.datetime.fromtimestamp(
                        slot.end_date_in_seconds_epoch,
                        tz=datetime.timezone(datetime.timedelta(hours=8))  # Asia/Manila
                    )
                    
                    warning_datetime = end_datetime - datetime.timedelta(minutes=minute_mark_to_warn)
                    
                    # Skip if warning time is in the past
                    current_time = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=8)))
                    if warning_datetime <= current_time:
                        print(f"[SKIP] Warning time in past for temporary schedule: {slot.subject}")
                        continue
                    
                    print(f"   Warning at: {warning_datetime.strftime('%Y-%m-%d %I:%M:%S %p %Z')}")
                    
                    # Schedule the warning job with DateTrigger
                    scheduler_v2.add_job(
                        send_schedule_warning,
                        trigger=DateTrigger(
                            run_date=warning_datetime,
                            timezone=DEVICE_TZ  # Asia/Manila
                            
                        ),
                        args=[
                            slot.room_id, 
                            slot.subject, 
                            slot.end_time, 
                            mqtt_client,
                            pg_conn_pool,
                            end_datetime.hour,
                            end_datetime.minute,
                            end_datetime.strftime('%A'),  # Day name like "Monday"
                            slot.timeslot_id  # Pass timeslot_id to check for running jobs
                        ],
                        id=warning_job_id,
                        replace_existing=True
                    )
                    
                else:
                    # REGULAR SCHEDULES: Use CronTrigger for recurring warnings
                    print(f"[REGULAR] Scheduling warning for: {slot.room_id} - {slot.subject}")
                    
                    # Calculate warning time (end_time - minute_mark_to_warn)
                    warn_hour = slot.end_hour
                    warn_minute = slot.end_minute - minute_mark_to_warn
                    
                    # Handle minute underflow (e.g., 10:05 - 10 minutes = 9:55)
                    while warn_minute < 0:
                        warn_minute += 60
                        warn_hour -= 1
                    
                    # Skip warnings that would be scheduled before midnight or at invalid times
                    if warn_hour < 0 or warn_hour > 23:
                        print(f"[SKIP] Invalid warning time for {slot.subject}")
                        continue
                    
                    # Get scheduler day format
                    scheduler_day = day_name_map.get(slot.day_name, slot.day_name.lower()[:3])
                    
                    # Schedule the warning job with CronTrigger
                    scheduler_v2.add_job(
                        send_schedule_warning,
                        trigger=CronTrigger(
                            hour=warn_hour,
                            minute=warn_minute,
                            day_of_week=scheduler_day,
                            timezone=datetime.timezone(datetime.timedelta(hours=8))  # Asia/Manila
                        ),
                        args=[
                            slot.room_id, 
                            slot.subject, 
                            slot.end_time, 
                            mqtt_client,
                            pg_conn_pool,
                            slot.end_hour,
                            slot.end_minute,
                            slot.day_name,
                            slot.timeslot_id  # Pass timeslot_id to check for running jobs
                        ],
                        id=warning_job_id,
                        replace_existing=True  # Allow updating existing warnings
                    )
                
                warning_jobs_scheduled += 1
                print(f"[SUCCESS] Warning scheduled: {slot.room_id} '{slot.subject}' ({'TEMP' if slot.is_temporary else 'REGULAR'})")
                
            except Exception as e:
                print(f"[ERROR] Error scheduling warning for slot {slot.timeslot_id}: {e}")
        
        print(f"[COMPLETED] Successfully scheduled {warning_jobs_scheduled} warning jobs")
        
    except Exception as e:
        print(f"[ERROR] Error in schedule_warning_jobs_for_slots: {e}")


def clear_warning_jobs(scheduler_v2: BackgroundScheduler, schedule_prefix: Optional[str] = None):
    """
    Clear warning jobs from the scheduler.
    
    Args:
        scheduler_v2: APScheduler instance
        schedule_prefix: Optional prefix to clear specific schedule warnings
    """
    try:
        all_jobs = scheduler_v2.get_jobs()
        cleared_count = 0
        
        for job in all_jobs:
            job_id = job.id
            
            # Check if this is a warning job
            if job_id.endswith('_warning'):
                # If schedule_prefix is specified, only clear matching warnings
                if schedule_prefix is None or job_id.startswith(schedule_prefix):
                    scheduler_v2.remove_job(job_id)
                    cleared_count += 1
        
        print(f"[CLEANUP] Cleared {cleared_count} warning jobs")
        
    except Exception as e:
        print(f"[ERROR] Error clearing warning jobs: {e}")


def rebuild_warning_jobs_for_settings_change(
    scheduler_v2: BackgroundScheduler,
    mqtt_client: mqtt.Client,
    pg_conn_pool: pool.SimpleConnectionPool
):
    """
    Rebuild all warning jobs when settings change.
    This clears existing warning jobs and recreates them with new settings.
    
    Args:
        scheduler_v2: APScheduler instance
        mqtt_client: MQTT client for publishing warnings
        pg_conn_pool: Database connection pool
    """
    try:
        print("[REBUILD] Rebuilding warning jobs due to settings change...")
        
        # Step 1: Clear all existing warning jobs
        clear_warning_jobs(scheduler_v2)
        
        # Step 2: Get the latest schedule and recreate warning jobs
        from src.sql.schedule.schedule_wrapper_sql import pg_get_latest_schedule_wrapper
        from src.sql.resolved_timeslot.resolved_timeslot import pg_get_resolved_slots_by_schedule_id
        
        latest_wrapper = pg_get_latest_schedule_wrapper(pg_conn_pool)
        if not latest_wrapper:
            print("[WARNING] No schedule found, no warning jobs to rebuild")
            return
        
        # Get all slots for the latest schedule
        slots = pg_get_resolved_slots_by_schedule_id(pg_conn_pool, latest_wrapper.schedule_id)
        if not slots:
            print("[WARNING] No schedule slots found, no warning jobs to rebuild")
            return
        
        # Step 3: Recreate warning jobs with new settings
        schedule_warning_jobs_for_slots(
            slots=slots,
            scheduler_v2=scheduler_v2,
            mqtt_client=mqtt_client,
            pg_conn_pool=pg_conn_pool
        )
        
        print(f"[SUCCESS] Successfully rebuilt warning jobs for {len(slots)} schedule slots")
        
    except Exception as e:
        print(f"[ERROR] Error rebuilding warning jobs: {e}")


def check_and_rebuild_warnings_if_needed(
    scheduler_v2: BackgroundScheduler,
    mqtt_client: mqtt.Client,
    pg_conn_pool: pool.SimpleConnectionPool,
    new_minute_mark_to_warn: int
):
    """
    Smart rebuild: Only rebuild warning jobs if the minute_mark_to_warn setting actually changed.
    
    Args:
        scheduler_v2: APScheduler instance
        mqtt_client: MQTT client for publishing warnings
        pg_conn_pool: Database connection pool
        new_minute_mark_to_warn: The new warning threshold value
    """
    try:
        # With hardcoded settings, compare against the global constant
        current_threshold = MINUTE_MARK_TO_WARN
        
        if current_threshold != new_minute_mark_to_warn:
            print(f"[SETTINGS] Warning threshold requested change: {current_threshold} → {new_minute_mark_to_warn} minutes")
            print("[WARNING] Note: Using hardcoded settings, ignoring requested change")
        else:
            print(f"[SUCCESS] Warning threshold unchanged ({current_threshold} min), no rebuild needed")
            
    except Exception as e:
        print(f"[ERROR] Error checking warning settings: {e}")
