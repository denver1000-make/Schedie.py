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
from zoneinfo import ZoneInfo
from psycopg2 import pool as pg_pool
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

from src.g2_utils.mqtt.mqtt_funcs_g2 import send_shutdown_warning
from src.sql_orm.turn_on_jobs.turn_on_job_orm import get_running_job
from src.json.cancellation_parser import CancellationRequestJson
from src.json.temporary_schedule_parser import TemporaryScheduleJson, parse_temporary_schedule_json, convert_temp_schedule_to_orm, get_temp_schedule_summary
from src.g2_utils.scheduler_util import schedule_resolved_slots
from src.sql_orm.schedule.resolved_schedule_slots_orm import ResolvedScheduleSlotOrm, get_resolved_slots_by_schedule_id, insert_resolved_schedule_slots
from src.sql_orm.schedule.schedule_wrapper_orm import ScheduleWrapperOrm, get_in_use_schedule, insert_schedule_wrapper, set_all_schedule_wrappers_not_in_use, schedule_exists
from src.json.schedule_parser import ScheduleWrapperJson, extract_resolved_slots_from_json, parse_schedule_wrapper_json
from src.sql_orm.connection.sqlalchemy_pg import SessionFactory, initialize_global_engine, get_session
from src.mqtt.mqtt_manager import (
    register_callback,
    publish_v2,
    init_mqtt
)
from src.sql.db_connection import get_pg_connection
from src.schedulerv2.scheduler_v2 import init_scheduler

# Constants
DEVICE_TZ = ZoneInfo("Asia/Manila")
MINUTE_MARK_TO_WARN = 2  # Minutes before schedule end to send warning
MINUTE_MARK_TO_SKIP = 2  # Minutes to check for nearby schedules

# Import MQTT topic constants from mqtt_manager (same as main_v3)
from src.mqtt.mqtt_manager import (
    SCHEDULE_UPDATE_PATTERN,
    SCHEDULE_TEMP_UPDATE,
    SCHEDULE_CANCEL_PATTERN,
    SYSTEM_SETTINGS_UPDATE_PATTERN,
    SCHEDULE_TEMP_ACK,
    SCHEDULE_UPDATE_ACK
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
            # print(f"✅ Cancellation received: {canc_obj}")
            
            # from src.sql_orm.cancellation.cancelled_schedule_orm import insert_cancellation_info, CancelledScheduleOrm
            # from src.sql_orm.turn_on_jobs.turn_on_job_orm import remove_running_job
            # from src.g2_utils.jobs.jobs_g2 import get_schedule_slot_by_timeslot_id
            # from apscheduler.triggers.date import DateTrigger
            # from src.g2_utils.mqtt.mqtt_funcs_g2 import mqtt_turn_off
            
            # time_now = datetime.datetime.now(tz=DEVICE_TZ)
            
            # # Get the schedule slot info to determine if it's temporary
            # schedule_slot = get_schedule_slot_by_timeslot_id(canc_obj.timeslot_id)
            # if not schedule_slot:
            #     print(f"❌ Schedule slot not found for timeslot_id: {canc_obj.timeslot_id}")
            #     return
            
            # # Check if cancellation is for today's instance
            # is_today_cancellation = (
            #     time_now.day == canc_obj.day_of_month and
            #     time_now.month == canc_obj.month and 
            #     time_now.year == canc_obj.year
            # )
            
            # running_job = get_running_job(canc_obj.timeslot_id)
            
            
            # if schedule_slot.is_temporary or (not schedule_slot.is_temporary and is_today_cancellation and running_job):
            #     if running_job:
            #         send_shutdown_warning(
            #             mqtt_client=mqtt_client,
            #             room_id=canc_obj.room_id
            #         )
                    
            #         shutdown_time = time_now + datetime.timedelta(minutes=5)
                    
            #         def shutdown_cancelled_room():
            #             mqtt_turn_off(
            #                 room_id=canc_obj.room_id,
            #                 mqtt_client=mqtt_client
            #             )
            #             remove_running_job(canc_obj.timeslot_id)
                    
            #         scheduler.add_job(
            #             func=shutdown_cancelled_room,
            #             trigger=DateTrigger(run_date=shutdown_time),
            #             id=f"cancel_shutdown_{canc_obj.timeslot_id}",
            #             replace_existing=True
            #         )
                    
            #         print(f"✅ Scheduled shutdown for running room {canc_obj.room_id} in 5 minutes (Today's cancellation)")
            #     else:
            #         print(f"✅ Cancellation processed for today but schedule not currently running")
            # else:
            #     cancelled_date = f"{canc_obj.year:04d}-{canc_obj.month:02d}-{canc_obj.day_of_month:02d}"
                
            #     cancelled_schedule = CancelledScheduleOrm(
            #         timeslot_id=canc_obj.timeslot_id,
            #         cancellation_type="permanent_instance" if not schedule_slot.is_temporary else "temporary_instance",
            #         cancelled_at=time_now,
            #         cancelled_date=cancelled_date,
            #         reason=canc_obj.reason,
            #         cancelled_by=canc_obj.teacher_email,
            #         cancellation_id=canc_obj.id,
            #         room_id=canc_obj.room_id,
            #         teacher_name=canc_obj.teacher_name,
            #         teacher_id=canc_obj.teacher_id,
            #         teacher_email=canc_obj.teacher_email,
            #         day_name=canc_obj.day,
            #         year=canc_obj.year,
            #         month=canc_obj.month,
            #         day_of_month=canc_obj.day_of_month,
            #         subject=schedule_slot.subject,
            #         start_time=schedule_slot.start_time,
            #         end_time=schedule_slot.end_time
            #     )
                
            #     insert_cancellation_info(cancelled_schedule)
            #     print(f"✅ Added cancellation record for future date {cancelled_date}, timeslot {canc_obj.timeslot_id}")    
            
            # Send cancellation ACK after successful processing
            ack_payload = {
                "timeslot_id": canc_obj.timeslot_id,
                "cancellation_id": canc_obj.id,
                "received": True,
                "processed": True,
                "processed_at": time.time()
            }
            
            publish_v2(
                client=client,
                topic=f"{CANCEL_SCHEDULE_ACK}/{ack_payload['cancellation_id']}",
                msg=json.dumps(ack_payload),
                retain=True,
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
        
        # Check for null payload (retained message clearing)
        if not msg.payload or len(msg.payload) == 0:
            print("ℹ️ POSSIBLY CLEARING RETENTION - Empty payload received")
            return
            
        try:
            payload_str = msg.payload.decode()
            json_data = json.loads(payload_str)
            temp_schedule = parse_temporary_schedule_json(json_data)
            
            print(f"✅ Temporary schedule received: {get_temp_schedule_summary(temp_schedule)}")
            
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
            
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in temporary schedule payload: {e}")
            return
        except Exception as e:
            print(f"❌ Failed to process temporary schedule: {e}")
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
