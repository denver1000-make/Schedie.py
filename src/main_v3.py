import os
from typing import List
import uuid
from zoneinfo import ZoneInfo
from psycopg2 import pool as pg_pool
from src.modelsV2.models import SettingsConfiguration
from src.sql.resolved_timeslot.resolved_timeslot import pg_create_resolved_schedule_slots_table, pg_insert_resolved_time_slots
from src.sql.schedule.schedule_wrapper_sql import pg_create_schedule_wrappers_table, pg_insert_schedule_wrapper, pg_clear_all_schedule_data
from src.sql.running_turn_on_job.running_turn_on_job_sql import pg_create_running_turn_on_jobs_table
from src.sql.cancelled_schedules.cancelled_schedules_sql import pg_create_cancelled_schedules_table, pg_clear_all_cancelled_schedules
from src.helper.json_parsing_helper import parse_schedule_json
from src.mqtt.mqtt_functions import send_heartbeat
from src.mqtt.mqtt_manager import (
    SYSTEM_SETTINGS_UPDATE_BASE,
    SYSTEM_SETTINGS_UPDATE_ACK, 
    SCHEDULE_TEMP_UPDATE,
    SCHEDULE_TEMP_ACK,
    SCHEDULE_UPDATE,
    SCHEDULE_UPDATE_ACK,
    SCHEDULE_CANCEL_PATTERN,
    register_callback,
    publish_v2,
    init_mqtt
)
from src.schedulerv2.scheduler_v2 import init_scheduler
from src.sql.db_connection import get_pg_connection
from src.modelsV2.model import ScheduleWrapper, ResolvedScheduleSlotV2
from src.schedulerv2.job_funcs import schedule_v3_with_immediate_check
from src.schedulerv2.warning_funcs import clear_warning_jobs, check_and_rebuild_warnings_if_needed
import paho.mqtt.client as mqtt
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
import datetime

# Constants
SCHEDULE_COLLECTION_PATH = "schedules"
SYSTEM_SETTINGS_PATH = "settings"
CLASS_CANCELLATION_REQUESTS = "classCancellationsRequest"
TEMPORARY_SCHEDULE_COLLECTION = "temporary_schedules"
DEVICE_TZ = ZoneInfo("Asia/Manila")

# Import global constants
from src.constants import MINUTE_MARK_TO_WARN, MINUTE_MARK_TO_SKIP

def load_config():
    envFile = os.getenv("ENV_FILE")
    if envFile is None:
        raise ValueError("Env file path not found")
    load_dotenv(dotenv_path=envFile)


def load_pg() -> pg_pool.SimpleConnectionPool:
    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_db = os.getenv("POSTGRES_DB")
    postgres_host = os.getenv("POSTGRES_HOST")

    if postgres_user is None:
        raise ValueError("Postgres User not set")
    if postgres_password is None:
        raise ValueError("Postgres Password not set")
    if postgres_db is None:
        raise ValueError("Postgres DB not set")
    if postgres_host is None:
        raise ValueError("Postgres Host not set")

    return get_pg_connection(
        database=postgres_db,
        host=postgres_host,
        password=postgres_password,
        port="5432",
        user=postgres_password
    )


def load_mqtt() -> mqtt.Client:
    mqtt_url: str | None = os.getenv("MQTT_URL")
    mqtt_username: str | None = os.getenv("MQTT_USERNAME")
    mqtt_port: str | None = os.getenv("MQTT_PORT")
    mqtt_pass: str | None = os.getenv("MQTT_PASSWORD")

    # schedule_db_conn: Connection = get_db_connection(DB_FOR_SCHEDULE_NAME)
    if mqtt_url is None:
        raise ValueError("MQTT_URL not set")

    if mqtt_username is None:
        raise ValueError("MQTR_USERNAME not set")

    if mqtt_port is None:
        raise ValueError("MQTT_PORT not set")

    if mqtt_pass is None:
        raise ValueError("MQTT_PASS not set")

    return init_mqtt(
        mqtt_url=mqtt_url,
        mqtt_port=int(mqtt_port),
        mqtt_username=mqtt_username,
        mqtt_password=mqtt_pass
    )


def insert_schedule_with_slots(
    pg_conn_pool: pg_pool.SimpleConnectionPool,
    schedule_wrapper: ScheduleWrapper,
    slots: List[ResolvedScheduleSlotV2]
) -> bool:
    """
    Insert a ScheduleWrapper and its related ResolvedScheduleSlotV2 objects
    This ensures proper foreign key relationships are maintained
    
    Args:
        pg_conn_pool: PostgreSQL connection pool
        schedule_wrapper: ScheduleWrapper object to insert first
        slots: List of ResolvedScheduleSlotV2 objects that reference the wrapper
        
    Returns:
        bool: True if both operations succeed, False otherwise
    """
    try:
        # First, insert the ScheduleWrapper (parent record)
        wrapper_success = pg_insert_schedule_wrapper(pg_conn_pool, schedule_wrapper)
        if not wrapper_success:
            print(f"❌ Failed to insert schedule wrapper: {schedule_wrapper.schedule_id}")
            return False
        
        # Then, insert the ResolvedScheduleSlotV2 objects (child records)
        slots_success = pg_insert_resolved_time_slots(pg_conn_pool, slots, schedule_wrapper.schedule_id)
        if not slots_success:
            print(f"❌ Failed to insert resolved slots for schedule: {schedule_wrapper.schedule_id}")
            return False
            
        print(f"✅ Successfully inserted schedule wrapper and {len(slots)} slots for: {schedule_wrapper.schedule_id}")
        return True
        
    except Exception as e:
        print(f"❌ Error inserting schedule with slots: {e}")
        return False


def setup_tables(pg_conn_pool: pg_pool.SimpleConnectionPool):
    # Create schedule_wrappers table first (parent table)
    pg_create_schedule_wrappers_table(
        conn_pool=pg_conn_pool
    )
    
    # Create resolved_schedule_slots table with foreign key reference
    pg_create_resolved_schedule_slots_table(
        conn_pool=pg_conn_pool
    )
    
    # Create running_turn_on_jobs table
    pg_create_running_turn_on_jobs_table(
        conn_pool=pg_conn_pool
    )
    
    # Create cancelled_schedules table for tracking cancellations
    pg_create_cancelled_schedules_table(
        conn_pool=pg_conn_pool
    )
    
    # Legacy temporary schedule tables are no longer needed
    # All schedules now use unified schedule_wrapper + resolved_schedule_slots tables
    
    # Settings are now centralized in constants.py
    # MINUTE_MARK_TO_WARN = 2 and MINUTE_MARK_TO_SKIP = 1


def load_and_schedule_existing_data(
    pg_conn_pool: pg_pool.SimpleConnectionPool,
    scheduler_v2: BackgroundScheduler,
    mqtt_client: mqtt.Client
) -> bool:
    """
    Load ALL existing schedule data from database and schedule it normally.
    Handles both permanent and temporary schedules for full system recovery.
    Cleans up temporary schedules that are already past their start time.
    """
    print("📂 Loading ALL existing schedule data from database...")
    
    try:
        # Clean up orphaned running jobs from previous system runs first
        cleanup_orphaned_running_jobs(pg_conn_pool)
        
        # Clean up expired temporary schedules (past start time)
        cleanup_expired_temporary_schedules(pg_conn_pool)
        
        # Get ALL schedule wrappers (both permanent and temporary)
        from src.sql.schedule.schedule_wrapper_sql import pg_get_all_schedule_wrappers
        from src.sql.resolved_timeslot.resolved_timeslot import pg_get_resolved_slots_by_schedule_id
        
        all_wrappers = pg_get_all_schedule_wrappers(pg_conn_pool)
        
        if not all_wrappers:
            print("📋 No existing schedule data found - starting fresh")
            return True
        
        print(f"📋 Found {len(all_wrappers)} schedule wrappers to process")
        
        # Process each schedule wrapper
        total_scheduled = 0
        total_skipped = 0
        current_time = datetime.datetime.now(DEVICE_TZ)
        
        # Separate permanent and temporary schedules
        permanent_wrappers = [w for w in all_wrappers if not w.is_temporary]
        temporary_wrappers = [w for w in all_wrappers if w.is_temporary]
        
        print(f"   - Permanent schedules: {len(permanent_wrappers)}")
        print(f"   - Temporary schedules: {len(temporary_wrappers)}")
        
        # Process permanent schedules first
        for wrapper in permanent_wrappers:
            all_slots = []  # Initialize for error handling
            try:
                print(f"📋 Loading permanent schedule: {wrapper.schedule_id}")
                
                all_slots = pg_get_resolved_slots_by_schedule_id(pg_conn_pool, wrapper.schedule_id)
                if not all_slots:
                    print(f"   ⚠️ No slots found for {wrapper.schedule_id}")
                    continue
                
                print(f"   📋 Found {len(all_slots)} permanent slots to schedule")
                
                # Schedule permanent slots
                schedule_v3_with_immediate_check(
                    list_resolved_slots=all_slots,
                    scheduler_v2=scheduler_v2,
                    mqtt_client=mqtt_client,
                    pg_conn_pool=pg_conn_pool,
                    isTemp=False,  # Permanent schedule
                    current_time=current_time
                )
                
                total_scheduled += len(all_slots)
                print(f"   ✅ Scheduled {len(all_slots)} permanent slots")
                
            except Exception as e:
                print(f"   ❌ Error scheduling permanent wrapper {wrapper.schedule_id}: {e}")
                total_skipped += len(all_slots)
        
        # Process temporary schedules
        for wrapper in temporary_wrappers:
            all_slots = []  # Initialize for error handling
            try:
                print(f"📋 Loading temporary schedule: {wrapper.schedule_id}")
                
                all_slots = pg_get_resolved_slots_by_schedule_id(pg_conn_pool, wrapper.schedule_id)
                if not all_slots:
                    print(f"   ⚠️ No slots found for {wrapper.schedule_id}")
                    continue
                
                print(f"   📋 Found {len(all_slots)} temporary slots to schedule")
                
                # Schedule temporary slots
                schedule_v3_with_immediate_check(
                    list_resolved_slots=all_slots,
                    scheduler_v2=scheduler_v2,
                    mqtt_client=mqtt_client,
                    pg_conn_pool=pg_conn_pool,
                    isTemp=True,  # Temporary schedule
                    current_time=current_time
                )
                
                total_scheduled += len(all_slots)
                print(f"   ✅ Scheduled {len(all_slots)} temporary slots")
                
            except Exception as e:
                print(f"   ❌ Error scheduling temporary wrapper {wrapper.schedule_id}: {e}")
                total_skipped += len(all_slots)
        
        print(f"✅ Complete startup scheduling finished:")
        print(f"   - Total scheduled: {total_scheduled} slots")
        print(f"   - Total skipped: {total_skipped} slots")
        print(f"   - System ready for power outage recovery!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error loading existing schedule data: {e}")
        return False


def cleanup_expired_temporary_schedules(pg_conn_pool: pg_pool.SimpleConnectionPool):
    """
    Clean up temporary schedules based on their timing:
    1. Remove schedules that are past their start time (never got to run due to power outage)
    2. Remove schedules that have completely finished (end time passed)
    This handles full power outage recovery by cleaning up stale temporary schedules.
    """
    print("🧹 Cleaning up expired temporary schedules...")
    
    conn = None
    cursor = None
    try:
        conn = pg_conn_pool.getconn()
        cursor = conn.cursor()
        
        current_epoch = datetime.datetime.now().timestamp()
        total_cleaned = 0
        
        # 1. Clean up schedules past their start time (missed due to power outage)
        print("   🔍 Checking for schedules past start time...")
        count_query_start = """
        SELECT COUNT(DISTINCT sw.schedule_id), 
               STRING_AGG(DISTINCT rs.subject, ', ') as subjects
        FROM schedule_wrappers sw
        JOIN resolved_schedule_slots rs ON sw.schedule_id = rs.schedule_id
        WHERE sw.is_temporary = true 
        AND rs.start_date_in_seconds_epoch IS NOT NULL 
        AND rs.start_date_in_seconds_epoch < %s
        AND (rs.end_date_in_seconds_epoch IS NULL OR rs.end_date_in_seconds_epoch > %s)
        """
        cursor.execute(count_query_start, (current_epoch, current_epoch))
        result = cursor.fetchone()
        expired_start_count = result[0] if result else 0
        
        if expired_start_count > 0:
            expired_subjects = result[1] if result and result[1] else "Unknown"
            print(f"   📋 Found {expired_start_count} schedules past start time: {expired_subjects}")
            
            delete_query_start = """
            DELETE FROM schedule_wrappers 
            WHERE schedule_id IN (
                SELECT DISTINCT sw.schedule_id
                FROM schedule_wrappers sw
                JOIN resolved_schedule_slots rs ON sw.schedule_id = rs.schedule_id
                WHERE sw.is_temporary = true 
                AND rs.start_date_in_seconds_epoch IS NOT NULL 
                AND rs.start_date_in_seconds_epoch < %s
                AND (rs.end_date_in_seconds_epoch IS NULL OR rs.end_date_in_seconds_epoch > %s)
            )
            """
            cursor.execute(delete_query_start, (current_epoch, current_epoch))
            total_cleaned += expired_start_count
        
        # 2. Clean up schedules that have completely finished (end time passed)
        print("   🔍 Checking for completely finished schedules...")
        count_query_end = """
        SELECT COUNT(DISTINCT sw.schedule_id), 
               STRING_AGG(DISTINCT rs.subject, ', ') as subjects
        FROM schedule_wrappers sw
        JOIN resolved_schedule_slots rs ON sw.schedule_id = rs.schedule_id
        WHERE sw.is_temporary = true 
        AND rs.end_date_in_seconds_epoch IS NOT NULL 
        AND rs.end_date_in_seconds_epoch < %s
        """
        cursor.execute(count_query_end, (current_epoch,))
        result = cursor.fetchone()
        finished_count = result[0] if result else 0
        
        if finished_count > 0:
            finished_subjects = result[1] if result and result[1] else "Unknown"
            print(f"   📋 Found {finished_count} completely finished schedules: {finished_subjects}")
            
            delete_query_end = """
            DELETE FROM schedule_wrappers 
            WHERE schedule_id IN (
                SELECT DISTINCT sw.schedule_id
                FROM schedule_wrappers sw
                JOIN resolved_schedule_slots rs ON sw.schedule_id = rs.schedule_id
                WHERE sw.is_temporary = true 
                AND rs.end_date_in_seconds_epoch IS NOT NULL 
                AND rs.end_date_in_seconds_epoch < %s
            )
            """
            cursor.execute(delete_query_end, (current_epoch,))
            total_cleaned += finished_count
        
        conn.commit()
        
        if total_cleaned > 0:
            print(f"✅ Cleaned up {total_cleaned} total expired temporary schedules")
        else:
            print("✅ No expired temporary schedules found")
            
    except Exception as e:
        print(f"⚠️ Error cleaning up expired temporary schedules: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            pg_conn_pool.putconn(conn)


def cleanup_orphaned_running_jobs(pg_conn_pool: pg_pool.SimpleConnectionPool):
    """
    Clean up running jobs that may have been left behind from previous system runs.
    This handles cases where the system was shut down while schedules were active.
    """
    print("🧹 Cleaning up orphaned running jobs...")
    
    try:
        from src.sql.running_turn_on_job.running_turn_on_job_sql import pg_clear_all_running_turn_on_jobs
        
        # Simply clear all running jobs on startup since we're reloading schedules anyway
        conn = None
        cursor = None
        
        try:
            conn = pg_conn_pool.getconn()
            cursor = conn.cursor()
            
            # Get count before clearing for reporting
            cursor.execute("SELECT COUNT(*) FROM running_turn_on_jobs")
            job_count = cursor.fetchone()[0]
            
            if job_count > 0:
                cursor.execute("DELETE FROM running_turn_on_jobs")
                conn.commit()
                print(f"✅ Cleared {job_count} orphaned running jobs")
            else:
                print("✅ No orphaned running jobs found")
                
        except Exception as e:
            print(f"⚠️ Error clearing orphaned running jobs: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                pg_conn_pool.putconn(conn)
                
    except Exception as e:
        print(f"⚠️ Error in cleanup_orphaned_running_jobs: {e}")


def clear_tables(pg_conn_pool: pg_pool.SimpleConnectionPool):
    """
    Clear all schedule data from both ScheduleWrapper and ResolvedScheduleSlot tables
    Uses CASCADE DELETE to efficiently clear both tables
    Also clears cancelled schedules table
    """
    print("🧹 Clearing all schedule tables...")
    
    # Clear regular schedule data (ScheduleWrapper and ResolvedScheduleSlot with CASCADE)
    success = pg_clear_all_schedule_data(pg_conn_pool)
    if success:
        print("✅ Regular schedule tables cleared successfully")
    else:
        print("❌ Failed to clear regular schedule tables")
        return
    
    # Also explicitly clear resolved_schedule_slots table for safety (in case CASCADE didn't work)
    conn = None
    cursor = None
    try:
        conn = pg_conn_pool.getconn()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM resolved_schedule_slots")
        affected_rows = cursor.rowcount
        conn.commit()
        
        print(f"✅ Explicitly cleared {affected_rows} rows from resolved_schedule_slots table")
        
    except Exception as e:
        print(f"⚠️ Error explicitly clearing resolved schedule slots: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            pg_conn_pool.putconn(conn)
    
    # Clear cancelled schedules table
    cancelled_success = pg_clear_all_cancelled_schedules(pg_conn_pool)
    if cancelled_success:
        print("✅ Cancelled schedules table cleared successfully")
    else:
        print("❌ Failed to clear cancelled schedules table")
    
    # Legacy temporary schedule tables are no longer used
    # All schedules now use unified schedule_wrapper + resolved_schedule_slots tables
    print("✅ Legacy temporary schedule tables are not used in unified system")
    
    # Settings are now hardcoded globally for simplicity
    print(f"✅ Using centralized settings: MINUTE_MARK_TO_WARN={MINUTE_MARK_TO_WARN}, MINUTE_MARK_TO_SKIP={MINUTE_MARK_TO_SKIP}")
        
    if success and cancelled_success:
        print("✅ All tables cleared successfully")
    else:
        print("❌ Some tables failed to clear")


def clear_scheduler(scheduler, pg_conn_pool=None):
    """Clear all jobs from the scheduler - unified system handles both regular and temporary schedules"""
    print("🧹 Clearing all scheduler jobs...")
    
    # Clear warning jobs (now handles both regular and temporary schedules via unified system)
    try:
        clear_warning_jobs(scheduler)
    except Exception as e:
        print(f"⚠️ Error clearing warning jobs: {e}")
    
    # Legacy temporary job cleanup is no longer needed
    # Unified system handles all schedules through schedule_v3_with_immediate_check
    
    # Clear all remaining jobs
    scheduler.remove_all_jobs()
    print("✅ Scheduler cleared")


def load_latest_schedule_and_setup(
    pg_conn_pool: pg_pool.SimpleConnectionPool,
    scheduler_v2: BackgroundScheduler,
    mqtt_client: mqtt.Client
) -> bool:
    try:
        
        return True
        
    except Exception as e:
        print(f"❌ Error loading latest schedule: {e}")
        return False


def main():
    load_config()
    
    scheduler_v2 = init_scheduler()
    pg_conn_pool = load_pg()
    mqtt_client = load_mqtt()
    
    # Setup database tables with foreign key relationships first
    setup_tables(pg_conn_pool)
    clear_tables(
        pg_conn_pool
    )
    
    # Load and schedule existing data instead of clearing tables
    load_success = load_and_schedule_existing_data(
        pg_conn_pool=pg_conn_pool,
        scheduler_v2=scheduler_v2,
        mqtt_client=mqtt_client
    )
    
    if not load_success:
        print("⚠️ Failed to load existing data - continuing with fresh start")
        clear_scheduler(scheduler=scheduler_v2)
    
    def temp_schedule_received_mqtt_v2(client: mqtt.Client, message: mqtt.MQTTMessage):
        import json
        import time
        from datetime import datetime
        from src.modelsV2.model import TemporarySchedule
        # Legacy temporary schedule functions removed - using unified system
        
        try:
            jsonData = json.loads(message.payload)
            print(f"📨 Received temporary schedule data (v2 normalized): {jsonData}")
            
            # Create TemporarySchedule object from MQTT data first
            temp_schedule = TemporarySchedule(
                temporary_schedule_id=jsonData.get("timeslotId", ""),
                room_id=jsonData.get("roomId", ""),
                day_name=jsonData.get("dayName", ""),
                day_order=jsonData.get("dayOrder", 0),
                start_hour=jsonData.get("startHour", 0),
                start_minute=jsonData.get("startMinute", 0),
                end_hour=jsonData.get("endHour", 0),
                end_minute=jsonData.get("endMinute", 0),
                start_date_time=jsonData.get("startDateTime", 0.0),
                end_date_time=jsonData.get("endDateTime", 0.0),
                upload_date=jsonData.get("uploadDate", 0.0),
                teacher=jsonData.get("teacher", ""),
                teacher_email=jsonData.get("teacherEmail", ""),
                is_temporary=jsonData.get("isTemporary", True),
                is_completed=False  # New temporary schedules are not completed
            )
            
            # Normalize to ScheduleWrapper (unified schedule metadata format)
            schedule_wrapper = ScheduleWrapper(
                schedule_id=str(uuid.uuid4()),  # Use temporary_schedule_id as schedule_id
                upload_date_epoch=temp_schedule.upload_date,
                is_temporary=True,  # Mark as temporary
                is_synced_to_remote=True,  # MQTT data is from remote and synced
                is_from_remote=True        # MQTT data comes from remote
            )
            
            schedule_slot = ResolvedScheduleSlotV2(
                timeslot_id=temp_schedule.temporary_schedule_id,
                schedule_id=temp_schedule.temporary_schedule_id,  
                room_id=temp_schedule.room_id,
                day_name=temp_schedule.day_name,
                day_order=temp_schedule.day_order,
                start_time=f"{temp_schedule.start_hour:02d}:{temp_schedule.start_minute:02d}",
                end_time=f"{temp_schedule.end_hour:02d}:{temp_schedule.end_minute:02d}",
                subject=jsonData.get("subject", "Temporary Class"),  
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
            
            print(f"🔄 Normalized temporary schedule:")
            print(f"   Schedule Wrapper: {schedule_wrapper}")
            print(f"   Schedule Slot: timeslot_id={schedule_slot.timeslot_id}, room={schedule_slot.room_id}")
            
            # Insert into unified schedule tables using existing function
            slots = [schedule_slot]  # Create list with single slot
            
            success = insert_schedule_with_slots(
                pg_conn_pool=pg_conn_pool,
                schedule_wrapper=schedule_wrapper,
                slots=slots
            )
            
            if success:
                print(f"✅ Successfully inserted normalized temporary schedule {schedule_wrapper}")
                
                # For temporary schedules, don't clear scheduler - just add to existing jobs
                print("📅 Adding temporary schedule to existing scheduler (no clearing)...")
                
                # Schedule the jobs using existing unified function
                current_time = datetime.now(tz=DEVICE_TZ)
                schedule_v3_with_immediate_check(
                    list_resolved_slots=slots,
                    scheduler_v2=scheduler_v2,
                    mqtt_client=mqtt_client,
                    pg_conn_pool=pg_conn_pool,
                    isTemp=True,  # Mark as temporary
                    current_time=current_time
                )
                
                print(f"🚀 Successfully scheduled normalized temporary schedule {schedule_wrapper.schedule_id}")
                
                # Send acknowledgment back to MQTT
                ack_payload = {
                    "temporary_schedule_id": schedule_wrapper.schedule_id,
                    "schedule_id": schedule_wrapper.schedule_id,  # Same as temporary_schedule_id
                    "received": True,
                    "processed": True,
                    "normalized": True,
                    "scheduled": True,
                    "processed_at": time.time()
                }
                
                client.publish(
                    f"{SCHEDULE_TEMP_ACK}/{temp_schedule.temporary_schedule_id}", 
                    json.dumps(ack_payload), 
                    qos=2
                )
                
                print(f"📤 Sent acknowledgment for normalized temporary schedule: {temp_schedule.temporary_schedule_id}")
            else:
                print(f"❌ Failed to process normalized temporary schedule: {temp_schedule.temporary_schedule_id}")
                
        except Exception as e:
            print(f"❌ Error processing temporary schedule MQTT message (v2 normalized): {e}")
            print(f"Raw message: {message.payload}")
            
    
    def schedule_received_mqtt(client: mqtt.Client, message: mqtt.MQTTMessage):
        import json
        from datetime import datetime
        from src.helper.json_parsing_helper import process_raw_schedule_data
        
        try:
            jsonData = json.loads(message.payload)
            
            # Check if this is the new raw format or the old processed format
            # Raw format has 'schedules' array, processed format has 'resolved_slots'
            if "schedules" in jsonData and "resolved_slots" not in jsonData:
                print("📨 Detected raw schedule format - preprocessing...")
                
                # Extract metadata from raw format
                schedule_id = jsonData.get("scheduleId", "unknown")
                upload_date_epoch = jsonData.get("uploadDate", 0)
                is_temporary = jsonData.get("isTemporary", False)
                
                # Process raw data to get ResolvedScheduleSlotV2 objects
                slots = process_raw_schedule_data(jsonData, schedule_id)
                
                print(f"📋 Preprocessed {len(slots)} schedule slots from raw format")
            else:
                print("📨 Detected processed schedule format - using existing parser...")
                
                # Use existing logic for processed format
                schedule_id = jsonData.get("schedule_id", "unknown")
                upload_date_epoch = jsonData.get("upload_date_epoch", 0)
                is_temporary = jsonData.get("is_temporary", False)
                
                slots = parse_schedule_json(
                    jsonData=jsonData,
                    message=message,
                    pg_conn_pool=pg_conn_pool,
                    schedule_id=schedule_id
                )
            
            print(f"📨 Received schedule data for ID: {schedule_id}")
            
            schedule_wrapper = ScheduleWrapper(
                schedule_id=schedule_id,
                upload_date_epoch=upload_date_epoch,
                is_temporary=is_temporary,
                is_synced_to_remote=True,  # MQTT data is from remote and synced
                is_from_remote=True        # MQTT data comes from remote
            )

            print(f"📋 Parsed {len(slots)} schedule slots")
            print(f"📦 Schedule wrapper: {schedule_wrapper}")
            
            # Insert schedule wrapper and slots into database using existing function
            success = insert_schedule_with_slots(
                pg_conn_pool=pg_conn_pool,
                schedule_wrapper=schedule_wrapper,
                slots=slots
            )
            
            if success:
                print(f"✅ Successfully inserted schedule {schedule_id} into database")
                
                # Clear existing scheduler jobs before scheduling new permanent schedule
                print("🧹 Clearing scheduler before adding new permanent schedule...")
                clear_scheduler(scheduler=scheduler_v2, pg_conn_pool=pg_conn_pool)
                
                # Schedule the jobs using existing function
                current_time = datetime.now(tz=DEVICE_TZ)
                schedule_v3_with_immediate_check(
                    list_resolved_slots=slots,
                    scheduler_v2=scheduler_v2,
                    mqtt_client=mqtt_client,
                    pg_conn_pool=pg_conn_pool,
                    isTemp=schedule_wrapper.is_temporary,
                    current_time=current_time
                )
                
                print(f"🚀 Successfully scheduled {len(slots)} jobs for schedule {schedule_id}")
            else:
                print(f"❌ Failed to insert schedule {schedule_id} into database")

            # Send acknowledgment
            mqtt_client.publish(f"{SCHEDULE_UPDATE_ACK}/{schedule_id}", json.dumps({
                "schedule_id": schedule_id,
                "received": True,
                "processed_at": datetime.now(tz=DEVICE_TZ).timestamp(),
                "scheduled": success
            }), qos=2)
            
        except Exception as e:
            print(f"❌ Error processing schedule MQTT message: {e}")
            print(f"Raw message: {message.payload}")
        
    def handle_schedule_cancellation(client: mqtt.Client, message: mqtt.MQTTMessage):
        """
        Handle schedule cancellation requests via MQTT.
        Topic format: cancel_schedule/{timeslot_id}
        Payload: Optional JSON with reason and cancelled_by fields
        """
        import json
        
        try:
            from src.mqtt.mqtt_functions import cancel_schedule
            
            # Extract timeslot_id from topic
            topic_parts = message.topic.split('/')
            if len(topic_parts) != 2:
                print(f"❌ Invalid cancellation topic format: {message.topic}")
                return
                
            timeslot_id = topic_parts[1]
            print(f"🚫 Received cancellation request for timeslot: {timeslot_id}")
            
            # Parse optional payload for metadata
            reason = "User requested cancellation"
            cancelled_by = "mqtt_client"
            
            if message.payload:
                try:
                    payload_data = json.loads(message.payload.decode())
                    reason = payload_data.get("reason", reason)
                    cancelled_by = payload_data.get("cancelled_by", cancelled_by)
                except:
                    print(f"⚠️ Could not parse cancellation payload, using defaults")
            
            # Process the cancellation
            success = cancel_schedule(
                timeslot_id=timeslot_id,
                pg_conn_pool=pg_conn_pool,
                mqtt_client=client,
                scheduler=scheduler_v2,  # Pass the scheduler for delayed operations
                reason=reason,
                subject="",
                cancelled_by=cancelled_by
            )
            
            # Send acknowledgment
            ack_payload = {
                "timeslot_id": timeslot_id,
                "success": success,
                "reason": reason,
                "timestamp": datetime.datetime.now().isoformat()
            }
            
            client.publish(
                topic=f"cancel_schedule_ack/{timeslot_id}",
                payload=json.dumps(ack_payload),
                qos=1
            )
            
            if success:
                print(f"✅ Successfully processed cancellation for {timeslot_id}")
            else:
                print(f"❌ Failed to process cancellation for {timeslot_id}")
                
        except Exception as e:
            print(f"❌ Error handling schedule cancellation: {e}")
            print(f"Topic: {message.topic}, Payload: {message.payload}")
        
    def handle_system_settings_update(client: mqtt.Client, message: mqtt.MQTTMessage):
        import json
        json_data = json.loads(
            message.payload.decode()
        )
        
        update_id = str(json_data["requestId"])
        new_minute_mark_to_warn = int(json_data["minuteMarkToWarn"])
        
        print(f"Received settings update with Id {update_id}")
        print(f"New minute_mark_to_warn: {new_minute_mark_to_warn}")
        
        # Check if warning threshold changed and rebuild warning jobs if needed
        check_and_rebuild_warnings_if_needed(
            scheduler_v2=scheduler_v2,
            mqtt_client=mqtt_client,
            pg_conn_pool=pg_conn_pool,
            new_minute_mark_to_warn=new_minute_mark_to_warn
        )
        
        # Legacy temporary warning rebuilds are no longer needed
        # The unified warning system handles both regular and temporary schedules
        
        # Settings are now hardcoded globally for simplicity
        # No database update needed - using centralized constants from constants.py
        print(f"⚠️  Settings update requested but using centralized constants: MINUTE_MARK_TO_WARN={MINUTE_MARK_TO_WARN}, MINUTE_MARK_TO_SKIP={MINUTE_MARK_TO_SKIP}")
        
        mqtt_client.publish(
            payload=update_id,
            topic=f"{SYSTEM_SETTINGS_UPDATE_ACK}/{update_id}",
            qos=2
        )

    def handle_connection_remote_id(client: mqtt.Client, message: mqtt.MQTTMessage):
        """
        Callback handler for connection/remote_id topic messages.
        Prints the received connection information for monitoring purposes.
        """
        try:
            payload = message.payload.decode('utf-8')
            print(f"🔗 [CONNECTION] Remote ID received: {payload}")
            print(f"    Topic: {message.topic}")
            print(f"    QoS: {message.qos}")
            print(f"    Retain: {message.retain}")
            
            # Additional parsing if payload is JSON
            try:
                import json
                json_data = json.loads(payload)
                print(f"    Parsed JSON: {json_data}")
            except Exception:
                print(f"    Raw payload: {payload}")
                
        except Exception as e:
            print(f"❌ [CONNECTION] Error handling remote_id message: {e}")

    register_callback(
        callback=temp_schedule_received_mqtt_v2,
        topic=SCHEDULE_TEMP_UPDATE
    )
    
    register_callback(
        callback=schedule_received_mqtt,
        topic=SCHEDULE_UPDATE
    )
    
    register_callback(
        callback=handle_schedule_cancellation,
        topic=SCHEDULE_CANCEL_PATTERN
    )
    
    register_callback(
        callback=handle_system_settings_update,
        topic=SYSTEM_SETTINGS_UPDATE_BASE
    )
    
    register_callback(
        callback=handle_connection_remote_id,
        topic="connection/remote_id"
    )

    # scheduler_v2.add_job(
    #     func=send_heartbeat,
    #     trigger=IntervalTrigger(seconds=1),
    #     args=[mqtt_client]
    # )

if __name__ == "__main__":
    import time
    
    try:
        main()
        
        # Keep the program running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[SYSTEM] Exiting gracefully.")
