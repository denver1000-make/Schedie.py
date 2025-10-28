#!/usr/bin/env python3
"""
Bare minimum system setup - PostgreSQL and MQTT connections only
Starting point for system rewrite
"""
import datetime
import sqlalchemy.orm as sa_orm
import sqlalchemy as sa
import os
import json
import time
from typing import List
from psycopg2 import pool as pg_pool
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

from src.g2_utils.mqtt.mqtt_funcs_g2 import send_shutdown_warning
from src.sql_orm.turn_on_jobs.turn_on_job_orm import get_running_job
from src.json.cancellation_parser import CancellationRequestJson
from src.json.temporary_schedule_parser import TemporaryScheduleJson, parse_temporary_schedule_json, convert_temp_schedule_to_orm, get_temp_schedule_summary
from src.g2_utils.scheduler_util import schedule_resolved_slots
from src.sql_orm.schedule.resolved_schedule_slots_orm import ResolvedScheduleSlotOrm, get_resolved_slots_by_schedule_id, insert_resolved_schedule_slots
from src.sql_orm.schedule.schedule_wrapper_orm import ScheduleWrapperOrm, get_in_use_schedule, insert_schedule_wrapper, set_all_schedule_wrappers_not_in_use, schedule_exists, get_schedule_wrapper_by_timeslot_id, delete_schedule_wrapper_by_id
from src.json.schedule_parser import ScheduleWrapperJson, extract_resolved_slots_from_json, parse_schedule_wrapper_json
from src.sql_orm.connection.sqlalchemy_pg import SessionFactory, initialize_global_engine, get_session
from src.mqtt.mqtt_manager import (
    register_callback,
    publish_v2,
    init_mqtt
)
from src.sql.db_connection import get_pg_connection
from src.schedulerv2.scheduler_v2 import init_scheduler

# Import constants
from src.constants import DEVICE_TZ, MINUTE_MARK_TO_WARN, MINUTE_MARK_TO_SKIP

# Import MQTT topic constants from mqtt_manager (same as main_v3)
from src.mqtt.mqtt_manager import (
    SCHEDULE_UPDATE_PATTERN,
    SCHEDULE_TEMP_UPDATE,
    SCHEDULE_CANCEL_PATTERN,
    SYSTEM_SETTINGS_UPDATE_PATTERN,
    SCHEDULE_TEMP_ACK,
    SCHEDULE_UPDATE_ACK,
    USAGE_REPORT_PATTERN
)

# Additional ACK constants
SETTINGS_UPDATE_ACK = "settings_update_ack"
CANCEL_SCHEDULE_ACK = "cancel_schedule_ack"

def load_config():
    """Load environment configuration"""
    envFile = os.getenv("ENV_FILE")
    if envFile is None:
        raise ValueError("Env file path not found")
    load_dotenv(dotenv_path=envFile)


def load_pg() -> pg_pool.SimpleConnectionPool:
    """Setup PostgreSQL connection pool"""
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
        user=postgres_user  # Fixed: was using password instead of user
    )



def load_mqtt() -> mqtt.Client:
    """Setup MQTT client connection"""
    mqtt_url: str | None = os.getenv("MQTT_URL")
    mqtt_username: str | None = os.getenv("MQTT_USERNAME")
    mqtt_port: str | None = os.getenv("MQTT_PORT")
    mqtt_pass: str | None = os.getenv("MQTT_PASSWORD")

    if mqtt_url is None:
        raise ValueError("MQTT_URL not set")
    if mqtt_username is None:
        raise ValueError("MQTT_USERNAME not set")
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


def setup_database_tables(pg_conn_pool: pg_pool.SimpleConnectionPool):
    """Setup basic database tables - implement as needed"""
    print("🗄️ Setting up database tables...")
    
    print("✅ Database tables setup complete")

def main():
    # Load configuration
    load_config()
    print("✅ Configuration loaded")
    
    # Initialize scheduler (but don't start yet)
    scheduler = init_scheduler()
    print("✅ Scheduler initialized")
    
    # Initialize global engine with environment variables
    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_db = os.getenv("POSTGRES_DB")
    postgres_host = os.getenv("POSTGRES_HOST")
    
    # Debug: Print loaded PostgreSQL credentials
    print(f"🔍 PostgreSQL Connection Details:")
    print(f"    Host: {postgres_host}")
    print(f"    Database: {postgres_db}")
    print(f"    User: {postgres_user}")
    print(f"    Password: {'*' * len(postgres_password) if postgres_password else 'None'}")
    
    if postgres_user is None:
        raise ValueError("Postgres User not set")
    if postgres_password is None:
        raise ValueError("Postgres Password not set")
    if postgres_db is None:
        raise ValueError("Postgres DB not set")
    if postgres_host is None:
        raise ValueError("Postgres Host not set")
    
    # Initialize global engine and session factory
    initialize_global_engine(
        user=postgres_user,
        password=postgres_password,
        host=postgres_host,
        port=5432,
        database=postgres_db
    )
    print("✅ Global SQLAlchemy engine and session factory initialized")
    
    from src.sql_orm.connection.base import Base
    from src.sql_orm.schedule.resolved_schedule_slots_orm import ResolvedScheduleSlotOrm
    from src.sql_orm.schedule.schedule_wrapper_orm import ScheduleWrapperOrm
    from src.sql_orm.turn_on_jobs.turn_on_job_orm import RunningTurnOnJobOrm
    from src.sql_orm.cancellation.cancelled_schedule_orm import CancelledScheduleOrm
    from src.sql_orm.usage.power_usage_orm import PowerUsageOrm
    
    from src.sql_orm.connection.sqlalchemy_pg import engine
    
    Base.metadata.create_all(engine)
    print("✅ Database tables created")
    
    mqtt_client = load_mqtt()
    print("✅ MQTT client initialized")
    
    if not scheduler.running:
        scheduler.start()
        print("✅ Scheduler started")
    else:
        print("✅ Scheduler already running")
    
    def receive_schedule_update(client: mqtt.Client, msg: mqtt.MQTTMessage):
        print(f"🔍 SCHEDULE UPDATE CALLBACK CALLED - Topic: {msg.topic}")
        
        # Check for null payload (retained message clearing)
        if not msg.payload or len(msg.payload) == 0:
            print("ℹ️ POSSIBLY CLEARING RETENTION - Empty payload received")
            return
            
        payload = msg.payload.decode()
        json_map = json.loads(payload)
        
        schedule_wrapper_pydantic = parse_schedule_wrapper_json(
            json_map
        )
        
        print(f"📋 Processing schedule: {schedule_wrapper_pydantic.schedule_id}")
        
        if schedule_exists(schedule_wrapper_pydantic.schedule_id):
            print(f"⚠️ Schedule {schedule_wrapper_pydantic.schedule_id} already exists, skipping insert")
            return
        
        resolved_schedule_orm_slots = extract_resolved_slots_from_json(
            schedule_wrapper_pydantic
        )
        
        print(f"📊 Extracted {len(resolved_schedule_orm_slots)} schedule slots")
        for slot in resolved_schedule_orm_slots:
            print(f"   - {slot.timeslot_id}: {slot.room_id} {slot.day_name} {slot.start_time}-{slot.end_time}")
        
        scheduler.remove_all_jobs()
        set_all_schedule_wrappers_not_in_use()
        
        insert_schedule_wrapper(
            ScheduleWrapperOrm(
                schedule_id = schedule_wrapper_pydantic.schedule_id,
                upload_date_epoch = schedule_wrapper_pydantic.upload_date,
                is_temporary = False,
                is_synced_to_remote = True,
                is_from_remote = True,
                in_use = True
            )
        )
        print(f"✅ Inserted schedule wrapper: {schedule_wrapper_pydantic.schedule_id}")
        
        inserted_timeslot_ids = insert_resolved_schedule_slots(
            resolved_schedule_orm_slots
        )
        
        if inserted_timeslot_ids:
            session = get_session()
            try:
                fresh_slots = session.query(ResolvedScheduleSlotOrm).filter(
                    ResolvedScheduleSlotOrm.timeslot_id.in_(inserted_timeslot_ids)
                ).all()
            finally:
                session.close()
            
            schedule_resolved_slots(
                resolved_slots=fresh_slots,
                mqtt_client=client,
                scheduler=scheduler,
                minute_mark_to_skip=MINUTE_MARK_TO_SKIP,
                minute_mark_to_warn=MINUTE_MARK_TO_WARN
            )
            print(f"✅ Scheduled {len(fresh_slots)} jobs for timeslots")
        else:
            print("ℹ️ No new slots to schedule")
        
        # Send ACK after successful processing
        ack_payload = {
            "schedule_id": schedule_wrapper_pydantic.schedule_id,
            "received": True,
            "processed": True,
            "scheduled": len(inserted_timeslot_ids) if inserted_timeslot_ids else 0,
            "processed_at": time.time()
        }
        
        publish_v2(
            client=client,
            topic=f"{SCHEDULE_UPDATE_ACK}/{schedule_wrapper_pydantic.schedule_id}",
            msg=json.dumps(ack_payload),
            log=True
        )
        
        print(f"✅ Completed schedule update for: {schedule_wrapper_pydantic.schedule_id}")
        
        # Send confirmation for ESP32 buzzer
        publish_v2(
            client=client,
            topic="perm_schedule/received",
            msg="1",
            log=True
        )
        print("🔔 Sent permanent schedule confirmation buzzer signal")
    
    def receive_cancellation_notice(client: mqtt.Client, msg: mqtt.MQTTMessage):
        print(f"🔍 CANCELLATION CALLBACK CALLED - Topic: {msg.topic}")
    
        # Check for null payload (retained message clearing)
        if not msg.payload or len(msg.payload) == 0:
            print("ℹ️ POSSIBLY CLEARING RETENTION - Empty payload received")
            return
            
        try:
            payload_str = msg.payload.decode()
            json_data = json.loads(payload_str)
            canc_obj = CancellationRequestJson.model_validate(json_data)
            print(f"✅ Cancellation received: {canc_obj}")
            
            from src.sql_orm.cancellation.cancelled_schedule_orm import insert_cancellation_info, CancelledScheduleOrm, check_cancellation_id_exists
            
            # Check for duplicate cancellation_id
            if canc_obj.cancellation_id and check_cancellation_id_exists(canc_obj.cancellation_id):
                print(f"⚠️ Cancellation {canc_obj.cancellation_id} already processed - skipping duplicate")
                return
            from src.sql_orm.turn_on_jobs.turn_on_job_orm import remove_running_job
            from src.g2_utils.jobs.jobs_g2 import get_schedule_slot_by_timeslot_id
            from apscheduler.triggers.date import DateTrigger
            from src.g2_utils.mqtt.mqtt_funcs_g2 import mqtt_turn_off
            
            time_now = datetime.datetime.now(tz=DEVICE_TZ)
            
            schedule_slot = get_schedule_slot_by_timeslot_id(canc_obj.timeslot_id)
            if not schedule_slot:
                print(f"❌ Schedule slot not found for timeslot_id: {canc_obj.timeslot_id}")
                return
            
            is_today_cancellation = (
                time_now.day == canc_obj.day_of_month and
                time_now.month == canc_obj.month and 
                time_now.year == canc_obj.year
            )
            
            running_job = get_running_job(canc_obj.timeslot_id)
            
            # Check if schedule time has passed for today's cancellations
            schedule_has_passed = False
            is_currently_active = False
            
            if is_today_cancellation:
                schedule_start_time = datetime.time(schedule_slot.start_hour, schedule_slot.start_minute)
                schedule_end_time = datetime.time(schedule_slot.end_hour, schedule_slot.end_minute)
                current_time = time_now.time()
                
                schedule_has_passed = current_time > schedule_end_time
                
                # Check if we're currently within the active schedule window
                if running_job:
                    is_currently_active = schedule_start_time <= current_time <= schedule_end_time
                    print(f"🕐 Schedule time check: {schedule_start_time} <= {current_time} <= {schedule_end_time} = {is_currently_active}")
            
            # Always create and store cancellation record first
            cancelled_date = f"{canc_obj.year:04d}-{canc_obj.month:02d}-{canc_obj.day_of_month:02d}"
            
            cancelled_schedule = CancelledScheduleOrm(
                cancellation_id=canc_obj.cancellation_id,
                timeslot_id=canc_obj.timeslot_id,
                cancellation_type="temporary_instance" if schedule_slot.is_temporary else "permanent_instance",
                cancelled_at=time_now,
                cancelled_date=cancelled_date,
                reason=canc_obj.reason,
                cancelled_by=canc_obj.teacher_email,
                room_id=canc_obj.room_id,
                teacher_name=canc_obj.teacher_name,
                teacher_id=canc_obj.teacher_id,
                teacher_email=canc_obj.teacher_email,
                day_name=canc_obj.day,
                year=canc_obj.year,
                month=canc_obj.month,
                day_of_month=canc_obj.day_of_month,
                subject=schedule_slot.subject,
                start_time=schedule_slot.start_time,
                end_time=schedule_slot.end_time
            )
            
            insert_cancellation_info(cancelled_schedule)
            print(f"✅ Stored cancellation record for {cancelled_date}, timeslot {canc_obj.timeslot_id}")
            
            # Handle immediate action based on schedule type and state
            if schedule_slot.is_temporary and running_job:
                # For temporary schedules: if running job exists, it's currently active (no complex time checks needed)
                print(f"🚨 ACTIVE TEMPORARY SCHEDULE CANCELLED - Initiating delayed shutdown for {canc_obj.room_id}")
                
                send_shutdown_warning(
                    mqtt_client=mqtt_client,
                    room_id=canc_obj.room_id
                )
                
                shutdown_time = time_now + datetime.timedelta(minutes=1)
                
                def shutdown_cancelled_temp_room():
                    mqtt_turn_off(
                        room_id=canc_obj.room_id,
                        mqtt_client=mqtt_client
                    )
                    remove_running_job(canc_obj.timeslot_id)
                    print(f"✅ Executed delayed shutdown for cancelled temporary schedule {canc_obj.timeslot_id}")
                    
                    # Delete temporary schedule completely after shutdown
                    schedule_wrapper = get_schedule_wrapper_by_timeslot_id(canc_obj.timeslot_id)
                    if schedule_wrapper:
                        # Remove any scheduled jobs for this temporary schedule
                        try:
                            job_turn_on_id = f"{canc_obj.timeslot_id}_turn_on"
                            job_turn_off_id = f"{canc_obj.timeslot_id}_turn_off"
                            job_warning_id = f"{canc_obj.timeslot_id}_warning"
                            
                            for job_id in [job_turn_on_id, job_turn_off_id, job_warning_id]:
                                try:
                                    scheduler.remove_job(job_id)
                                    print(f"🗑️ Removed scheduled job: {job_id}")
                                except Exception:
                                    pass
                                    
                        except Exception as e:
                            print(f"⚠️ Error removing scheduled jobs for {canc_obj.timeslot_id}: {e}")
                        
                        delete_success = delete_schedule_wrapper_by_id(schedule_wrapper.schedule_id)
                        if delete_success:
                            print(f"🗑️ Deleted temporary schedule {schedule_wrapper.schedule_id} after cancellation")
                        else:
                            print(f"⚠️ Failed to delete temporary schedule {schedule_wrapper.schedule_id}")
                
                scheduler.add_job(
                    func=shutdown_cancelled_temp_room,
                    trigger=DateTrigger(run_date=shutdown_time),
                    id=f"cancel_shutdown_{canc_obj.timeslot_id}",
                    replace_existing=True
                )
                
                print(f"✅ Scheduled shutdown for running temporary schedule {canc_obj.room_id} in 1 minute")
            elif is_today_cancellation and running_job and is_currently_active:
                # For permanent schedules: Schedule is CURRENTLY ACTIVE - send warning and shutdown in 1 minute
                print(f"🚨 ACTIVE PERMANENT SCHEDULE CANCELLED - Initiating delayed shutdown for {canc_obj.room_id}")
                
                send_shutdown_warning(
                    mqtt_client=mqtt_client,
                    room_id=canc_obj.room_id
                )
                
                shutdown_time = time_now + datetime.timedelta(minutes=1)
                
                def shutdown_cancelled_room():
                    # Re-verify the schedule should still be active when shutdown executes
                    current_shutdown_time = datetime.datetime.now(tz=DEVICE_TZ).time()
                    schedule_start_time = datetime.time(schedule_slot.start_hour, schedule_slot.start_minute)
                    schedule_end_time = datetime.time(schedule_slot.end_hour, schedule_slot.end_minute)
                    
                    # Only proceed with shutdown if still within schedule window
                    if schedule_start_time <= current_shutdown_time <= schedule_end_time:
                        mqtt_turn_off(
                            room_id=canc_obj.room_id,
                            mqtt_client=mqtt_client
                        )
                        remove_running_job(canc_obj.timeslot_id)
                        print(f"✅ Executed delayed shutdown for cancelled permanent schedule {canc_obj.timeslot_id}")
                    else:
                        print(f"ℹ️ Skipped delayed shutdown - permanent schedule {canc_obj.timeslot_id} no longer active")
                
                scheduler.add_job(
                    func=shutdown_cancelled_room,
                    trigger=DateTrigger(run_date=shutdown_time),
                    id=f"cancel_shutdown_{canc_obj.timeslot_id}",
                    replace_existing=True
                )
                
                print(f"✅ Scheduled shutdown for running permanent schedule {canc_obj.room_id} in 1 minute")
            elif is_today_cancellation and running_job and not is_currently_active:
                # Permanent schedule has running job but is not currently active (scheduled for later today)
                print(f"🔄 FUTURE PERMANENT SCHEDULE CANCELLED - Removing running job for {canc_obj.room_id} (scheduled for later today)")
                remove_running_job(canc_obj.timeslot_id)
            elif schedule_slot.is_temporary and not running_job:
                # Temporary schedule that is not currently running - delete immediately
                print(f"🔄 FUTURE TEMPORARY SCHEDULE CANCELLED - Deleting for {canc_obj.room_id}")
                schedule_wrapper = get_schedule_wrapper_by_timeslot_id(canc_obj.timeslot_id)
                if schedule_wrapper:
                    # Remove any scheduled jobs for this temporary schedule
                    try:
                        job_turn_on_id = f"{canc_obj.timeslot_id}_turn_on"
                        job_turn_off_id = f"{canc_obj.timeslot_id}_turn_off"
                        job_warning_id = f"{canc_obj.timeslot_id}_warning"
                        
                        for job_id in [job_turn_on_id, job_turn_off_id, job_warning_id]:
                            try:
                                scheduler.remove_job(job_id)
                                print(f"🗑️ Removed scheduled job: {job_id}")
                            except Exception:
                                pass
                                
                    except Exception as e:
                        print(f"⚠️ Error removing scheduled jobs for {canc_obj.timeslot_id}: {e}")
                    
                    delete_success = delete_schedule_wrapper_by_id(schedule_wrapper.schedule_id)
                    if delete_success:
                        print(f"🗑️ Deleted future temporary schedule {schedule_wrapper.schedule_id}")
                    else:
                        print(f"⚠️ Failed to delete temporary schedule {schedule_wrapper.schedule_id}")
                else:
                    print(f"⚠️ Could not find schedule wrapper for temporary schedule {canc_obj.timeslot_id}")
            elif is_today_cancellation and schedule_has_passed:
                print(f"ℹ️ Cancellation for {canc_obj.room_id} received but schedule already ended")
                
                # If it's a temporary schedule that has already ended, delete it
                if schedule_slot.is_temporary:
                    schedule_wrapper = get_schedule_wrapper_by_timeslot_id(canc_obj.timeslot_id)
                    if schedule_wrapper:
                        # Remove any scheduled jobs for this temporary schedule
                        try:
                            job_turn_on_id = f"{canc_obj.timeslot_id}_turn_on"
                            job_turn_off_id = f"{canc_obj.timeslot_id}_turn_off"
                            job_warning_id = f"{canc_obj.timeslot_id}_warning"
                            
                            for job_id in [job_turn_on_id, job_turn_off_id, job_warning_id]:
                                try:
                                    scheduler.remove_job(job_id)
                                    print(f"🗑️ Removed scheduled job: {job_id}")
                                except Exception:
                                    pass
                                    
                        except Exception as e:
                            print(f"⚠️ Error removing scheduled jobs for {canc_obj.timeslot_id}: {e}")
                        
                        delete_success = delete_schedule_wrapper_by_id(schedule_wrapper.schedule_id)
                        if delete_success:
                            print(f"🗑️ Deleted ended temporary schedule {schedule_wrapper.schedule_id}")
                        else:
                            print(f"⚠️ Failed to delete temporary schedule {schedule_wrapper.schedule_id}")
            else:
                # Handle other cases (future dates, etc.)
                if schedule_slot.is_temporary:
                    print(f"ℹ️ Cancellation for temporary schedule {canc_obj.room_id} - future date or other scenario")
                else:
                    print(f"ℹ️ Cancellation for permanent schedule {canc_obj.room_id} - future date or not running")        
            
    
            ack_payload = {
                "timeslot_id": canc_obj.timeslot_id,
                "cancellation_id": canc_obj.cancellation_id,
                "received": True,
                "processed": True,
                "processed_at": time.time()
            }
            
            publish_v2(
                client=client,
                topic=f"{CANCEL_SCHEDULE_ACK}/{ack_payload['cancellation_id']}",
                msg=json.dumps(ack_payload),
                log=True
            )
            
    
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in cancellation payload: {e}")
            return
        except Exception as e:
            print(f"❌ Failed to parse cancellation JSON: {e}")
            return
    
    def receive_temporary_schedule(client: mqtt.Client, msg: mqtt.MQTTMessage):
        print(f"🔍 TEMPORARY SCHEDULE CALLBACK CALLED - Topic: {msg.topic}")
        
        if len(msg.payload) == 0 or not msg.payload:
            return
        try:
            payload_str = msg.payload.decode()
            json_data = json.loads(payload_str)
            temp_schedule = parse_temporary_schedule_json(json_data)
            
            print(f"✅ Temporary schedule received: {get_temp_schedule_summary(temp_schedule)}")
            
            from src.sql_orm.schedule.resolved_schedule_slots_orm import check_timeslot_id_exists
            
            # Check for duplicate timeslot_id
            if check_timeslot_id_exists(temp_schedule.timeslot_id):
                print(f"⚠️ Temporary schedule {temp_schedule.timeslot_id} already exists - skipping duplicate")
                return
            
            # Convert to ORM objects
            schedule_wrapper, schedule_slot = convert_temp_schedule_to_orm(temp_schedule)
            
            # Store the schedule_id before inserting (to avoid detached instance issues)
            schedule_id = schedule_wrapper.schedule_id
            
            # Insert into database first
            insert_schedule_wrapper(schedule_wrapper)
            inserted_timeslot_ids = insert_resolved_schedule_slots([schedule_slot])
            
            print(f"✅ Temporary schedule saved to database with ID: {schedule_id}")
            
            # Schedule jobs only if insertion was successful
            if inserted_timeslot_ids:
                # Get fresh data from database for scheduling
                session = get_session()
                try:
                    fresh_slots = session.query(ResolvedScheduleSlotOrm).filter(
                        ResolvedScheduleSlotOrm.timeslot_id.in_(inserted_timeslot_ids)
                    ).all()
                finally:
                    session.close()
                
                schedule_resolved_slots(
                    resolved_slots=fresh_slots,
                    mqtt_client=client,
                    scheduler=scheduler,
                    minute_mark_to_skip=MINUTE_MARK_TO_SKIP,
                    minute_mark_to_warn=MINUTE_MARK_TO_WARN
                )
                print(f"✅ Temporary schedule jobs scheduled for room {temp_schedule.room_id}")
            else:
                print("⚠️ No temporary schedule jobs to schedule (possibly duplicate)")
            
            # Send acknowledgment
            import time
            
            ack_payload = {
                "temporary_schedule_id": temp_schedule.timeslot_id,
                "schedule_id": schedule_id,
                "received": True,
                "processed": True,
                "scheduled": True,
                "processed_at": time.time()
            }
            
            from src.mqtt.mqtt_manager import SCHEDULE_TEMP_ACK
            
            publish_v2(
                client=client,
                topic=f"{SCHEDULE_TEMP_ACK}/{temp_schedule.timeslot_id}",
                msg=json.dumps(ack_payload),
                log=True
            )
            
            # Send confirmation for ESP32 buzzer (room-specific)
            publish_v2(
                client=client,
                topic=f"temp_schedule_received/{temp_schedule.room_id}",
                msg="1",
                log=True
            )
            print(f"🔔 Sent temporary schedule confirmation buzzer signal to {temp_schedule.room_id}")
            
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in temporary schedule payload: {e}")
            return
        except Exception as e:
            print(f"❌ Failed to process temporary schedule: {e}")
            import traceback
            traceback.print_exc()
            return
    
    def receive_usage_report(client: mqtt.Client, msg: mqtt.MQTTMessage):
        """
        Receive power usage reports from ESP32 devices.
        Expected topic: usage_report/{room_id}
        Expected payload: power in watts (integer)
        """
        # Check for null payload
        if not msg.payload or len(msg.payload) == 0:
            return
            
        try:
            # Extract room_id from topic: usage_report/RM301 -> RM301
            topic_parts = msg.topic.split('/')
            if len(topic_parts) != 2 or topic_parts[0] != 'usage_report':
                return
                
            room_id = topic_parts[1]
            
            # Parse power data (ESP32 sends integer watts)
            power_watts = int(msg.payload.decode().strip())
            
            # Validate power reading
            if power_watts < 0 or power_watts > 10000:  # Reasonable bounds
                return
            
            from src.sql_orm.usage.power_usage_orm import insert_power_usage, PowerUsageOrm
            
            # Create power usage record
            usage_record = PowerUsageOrm(
                room_id=room_id,
                power_watts=power_watts,
                timestamp=datetime.datetime.now(tz=DEVICE_TZ).replace(tzinfo=None)  # Store as UTC
            )
            
            # Insert into database (no logging for success)
            insert_power_usage(usage_record)
                
        except (ValueError, Exception):
            # Silently handle errors to avoid log spam
            pass
            import traceback
            traceback.print_exc()
            return
    
    # Register MQTT callback OUTSIDE the callback function
    register_callback(
        SCHEDULE_UPDATE_PATTERN, 
        receive_schedule_update
    )
    
    register_callback(
        SCHEDULE_CANCEL_PATTERN,
        receive_cancellation_notice
    )
    
    register_callback(
        SCHEDULE_TEMP_UPDATE,
        receive_temporary_schedule
    )
    
    register_callback(
        USAGE_REPORT_PATTERN,
        receive_usage_report
    )
    
    # System restart recovery procedure
    def recover_system_state():
        """
        Recovery procedure to handle system restarts by checking current time
        against active schedules and restoring appropriate room states.
        Handles any scenario where the system was turned off and restarted.
        """
        print("🔄 Starting system state recovery procedure...")
        
        from src.sql_orm.turn_on_jobs.turn_on_job_orm import remove_running_job, insert_running_job
        from src.g2_utils.mqtt.mqtt_funcs_g2 import mqtt_turn_on, mqtt_turn_off
        from datetime import datetime, time
        
        current_time = datetime.now(tz=DEVICE_TZ)
        current_day_name = current_time.strftime("%A").lower()
        current_time_obj = current_time.time()
        
        print(f"🕐 Current time: {current_time.strftime('%A %H:%M:%S')}")
        
        # Get the active schedule
        schedule_in_use = get_in_use_schedule()
        if not schedule_in_use:
            print("ℹ️ No active schedule found - recovery complete")
            return
        
        # Get all resolved slots for the active schedule
        resolved_slots = get_resolved_slots_by_schedule_id(schedule_in_use.schedule_id)
        
        # Check each slot to see if it should be active right now
        active_rooms = set()
        for slot in resolved_slots:
            # Skip if not today's schedule
            if slot.day_name.lower() != current_day_name:
                continue
                
            # Create time objects for comparison
            slot_start_time = time(slot.start_hour, slot.start_minute)
            slot_end_time = time(slot.end_hour, slot.end_minute)
            
            # Check if current time falls within this schedule slot
            if slot_start_time <= current_time_obj <= slot_end_time:
                print(f"📍 Found active schedule: {slot.room_id} ({slot.subject}) {slot.start_time}-{slot.end_time}")
                active_rooms.add(slot.room_id)
                
                # Check if running job exists for this timeslot
                existing_job = get_running_job(slot.timeslot_id)
                if not existing_job:
                    # Create missing running job
                    from src.sql_orm.turn_on_jobs.turn_on_job_orm import RunningTurnOnJobOrm
                    new_running_job = RunningTurnOnJobOrm(
                        timeslot_id=slot.timeslot_id,
                        is_temporary=slot.is_temporary
                    )
                    insert_running_job(new_running_job)
                    print(f"✅ Restored running job for {slot.timeslot_id}")
                
                # Turn on the room (ESP32 will handle if already on)
                mqtt_turn_on(
                    room_id=slot.room_id,
                    mqtt_client=mqtt_client
                )
                print(f"🔛 Sent turn ON command to room {slot.room_id}")
        
        # Also check for rooms that might have running jobs but shouldn't be active
        # This handles cases where schedules ended during system downtime
        session = get_session()
        try:
            from src.sql_orm.turn_on_jobs.turn_on_job_orm import RunningTurnOnJobOrm
            
            # Join query to get all data in one go and avoid detached instance issues
            running_jobs_data = session.query(
                RunningTurnOnJobOrm.timeslot_id,
                RunningTurnOnJobOrm.is_temporary,
                ResolvedScheduleSlotOrm.day_name,
                ResolvedScheduleSlotOrm.start_hour,
                ResolvedScheduleSlotOrm.start_minute,
                ResolvedScheduleSlotOrm.end_hour,
                ResolvedScheduleSlotOrm.end_minute,
                ResolvedScheduleSlotOrm.room_id,
                ResolvedScheduleSlotOrm.subject
            ).join(
                ResolvedScheduleSlotOrm,
                RunningTurnOnJobOrm.timeslot_id == ResolvedScheduleSlotOrm.timeslot_id
            ).all()
            
        finally:
            session.close()
        
        # Process the data after session is closed (no ORM instances to worry about)
        for job_data in running_jobs_data:
            (job_timeslot_id, job_is_temporary, slot_day_name, slot_start_hour, 
             slot_start_minute, slot_end_hour, slot_end_minute, slot_room_id, slot_subject) = job_data
            
            # Skip if not today's schedule
            if slot_day_name.lower() != current_day_name:
                continue

            slot_start_time = time(slot_start_hour, slot_start_minute)
            slot_end_time = time(slot_end_hour, slot_end_minute)

            # If current time is outside schedule window, remove running job and turn off room
            if not (slot_start_time <= current_time_obj <= slot_end_time):
                print(f"📍 Found expired running job: {slot_room_id} ({slot_subject})")

                # Remove running job (uses separate session internally)
                remove_running_job(job_timeslot_id)
                print(f"🗑️ Removed expired running job for {job_timeslot_id}")

                # Turn off room if it's not needed by any other active schedule
                if slot_room_id not in active_rooms:
                    mqtt_turn_off(
                        room_id=slot_room_id,
                        mqtt_client=mqtt_client
                    )
                    print(f"🔴 Sent turn OFF command to room {slot_room_id}")
        
        if active_rooms:
            print(f"🏠 Recovery complete - {len(active_rooms)} rooms should be active: {', '.join(active_rooms)}")
        else:
            print("🏠 Recovery complete - no rooms should be active at this time")
    
    # Run system state recovery procedure
    recover_system_state()
    
    schedule_in_use = get_in_use_schedule()
    
    if schedule_in_use:
        
        scheduler.remove_all_jobs()
        
        resolved_slots = get_resolved_slots_by_schedule_id(
            schedule_id=schedule_in_use.schedule_id
        )
        
        schedule_resolved_slots(
            mqtt_client=mqtt_client,
            resolved_slots=resolved_slots,
            scheduler=scheduler,
            minute_mark_to_skip=MINUTE_MARK_TO_SKIP,
            minute_mark_to_warn=MINUTE_MARK_TO_WARN
        )
        
        print("[Schedule] Registered in Use Schedule")
    else:
        print("[Schedule] No Schedule in Use")
        
    print("✅ MQTT callback registered")


if __name__ == "__main__":
    try:
        main()
        
        # Keep the program running (like main_v3.py)
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n👋 Shutting down gracefully...")
    except Exception as e:
        print(f"💥 System startup failed: {e}")
        import traceback
        traceback.print_exc()
