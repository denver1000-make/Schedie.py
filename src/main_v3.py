from datetime import datetime
import os
from typing import Callable, Dict, List, Optional
from zoneinfo import ZoneInfo
from psycopg2 import pool as pg_pool
from src.firestore.firestore_settings import init_firestore
from src.firestore.schedule_firestore import extract_resolved_slots_by_day, process_changes_from_firestore
from src.modelsV2.model import ResolvedScheduleSlot
from src.mqtt.mqtt_manager import init_mqtt, publish, publish_v2
from src.schedulerv2.scheduler_v2 import gen_job_name, generate_cron_trig, init_scheduler, parse_time
from src.sql.db_connection import get_pg_connection
import paho.mqtt.client as mqtt
from apscheduler.schedulers.background import BackgroundScheduler
from src.sql.jobs_sql import pg_clear_all_jobs, pg_fetch_job_by_id, pg_init_job_db, pg_insert_job, pg_query_room_for_nearby_star_sched
from src.sql.schedule_sql import log_job_pair_run, pg_clear_all_schedule_data, pg_create_schedule_table, pg_fetch_all_job_logs, pg_log_job_pair_run
from src.sql.settings_sql import pg_create_settings_table
from src.utils.time_utils.time_utils import str_time_to_standardize_time, strip_date_of_start_time, time_to_datetime
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.base import BaseTrigger

from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.firestore_v1.types import DocumentChange
from google.protobuf.internal.well_known_types import Timestamp
from dotenv import load_dotenv
# Constants
SCHEDULE_COLLECTION_PATH = "schedules"
SYSTEM_SETTINGS_PATH = "settings"
CLASS_CANCELLATION_REQUESTS = "classCancellationsRequest"
TEMPORARY_SCHEDULE_COLLECTION = "temporary_schedules"

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

def get_service_acc_path() -> str:
    service_acc: str | None = os.getenv("SERVICE_ACCOUNT_PATH")

    if service_acc is None:
        raise ValueError("Service Account Path Not set")
    return service_acc

def setup_tables(pg_conn_pool: pg_pool.SimpleConnectionPool):
    pg_create_settings_table(conn=pg_conn_pool)
    pg_create_schedule_table(conn_pool=pg_conn_pool)

def clear_tables(pg_conn_pool: pg_pool.SimpleConnectionPool):
    pg_clear_all_schedule_data(conn_pool=pg_conn_pool)
    pg_clear_all_jobs(conn_pool=pg_conn_pool)

def clear_scheduler(scheduler: BackgroundScheduler):
    scheduler.remove_all_jobs()

TurnOn = Callable[[Dict[str, str]], None]
TurnOff = Callable[[Dict[str, str]], None]
RecordInPG = Callable[[Dict[str, str]], None]

def turn_on(
        room_id: str,
        job_turn_off_id: str, 
        job_turn_on_id: str,
        mqtt_client: mqtt.Client,
        pg_pool: pg_pool.SimpleConnectionPool
    ):
        publish_v2(
            client=mqtt_client, 
            topic="turn_on", 
            msg=room_id, 
            log=True
        )
        
        if pg_conn_pool is not None:
            pg_log_job_pair_run(
                conn_pool=pg_conn_pool, 
                job_turn_off_id=job_turn_off_id, 
                job_turn_on_id=job_turn_on_id,
                room_id=room_id
            )
        log_job_pair_run(job_turn_off_id=job_turn_off_id, job_turn_on_id=job_turn_on_id, room_id=room_id)


def turn_off(
        room_id: str, 
        day_name: str, 
        year: int,
        month: int,
        day_of_month: int,
        time_in_sec: int,
        start_time: str, 
        end_time: str, 
        job_turn_off_id: str,
        job_turn_on_id: str,
        mqtt_client: mqtt.Client
    ):
    
    # if not list_of_turn_offs_to_skip.__contains__(job_turn_off_id):
    #     publish("turn_off", msg=room_id, log=True)
    # else:
    #    print("Skipped turn off.")
    
    
    
    publish_v2(client=mqtt_client, topic="turn_off", msg=room_id, log=True)
    
def schedule_v2(
        list_of_day: List[str],
        day_and_timeslot_dict: Dict[str, List[ResolvedScheduleSlot]],
        isTemp: bool
    ):
        for day in list_of_day:
            resolved_time_slots = day_and_timeslot_dict[day]
            for timeslot in resolved_time_slots:
                
                turn_on_name = gen_job_name(
                    job_type="turn_on",
                    room_id=timeslot.room_id,
                    start_time=timeslot.start_time,
                    end_time=timeslot.end_time,
                    day_name=timeslot.day_name)
                
                turn_off_name = gen_job_name(
                    job_type="turn_off",
                    room_id=timeslot.room_id,
                    start_time=timeslot.start_time,
                    end_time=timeslot.end_time,
                    day_name=timeslot.day_name)
                
                turnOnBaseTrigger: BaseTrigger
                turnOffBaseTrigger: BaseTrigger
                
                if isTemp:
                    turnOnBaseTrigger = DateTrigger(run_date=timeslot.start_date_in_schedule)
                    turnOffBaseTrigger = DateTrigger(run_date=timeslot.end_date_in_schedule)
                else:
                    parsed_start_time = parse_time(timeslot.start_time)
                    parsed_end_time = parse_time(timeslot.end_time)
                    cron_turn_on_job = generate_cron_trig(time_arg=parsed_start_time, full_day_name=timeslot.day_name)
                    cron_turn_off_job = generate_cron_trig(time_arg=parsed_end_time, full_day_name=timeslot.day_name)
                    turnOnBaseTrigger = CronTrigger(cron_turn_on_job)
                    turnOffBaseTrigger = CronTrigger(cron_turn_off_job)

                scheduler_v2.add_job(turn_on, trigger=turnOnBaseTrigger, args=[
                    timeslot.room_id,
                    timeslot.day_name,
                    timeslot.start_time,
                    timeslot.end_time,
                    turn_off_name,
                    turn_on_name,
                    mqtt_client,
                    pg_conn_pool
                ])
                time_in_sec = timeslot.end_date_in_schedule
                scheduler_v2.add_job(turn_off, trigger=turnOffBaseTrigger, args=[
                    timeslot.room_id,
                    timeslot.day_name,
                    timeslot.start_time,
                    timeslot.end_time,
                    turn_off_name,
                    turn_on_name,
                    mqtt_client
                ])
                
                schedule_date = strip_date_of_start_time(
                    resolved_time_slot=timeslot
                )

                pg_insert_job(
                    conn_pool=pg_conn_pool,
                    room_id=timeslot.room_id,
                    job_type="turn_on",
                    job_id=turn_on_name,
                    end_time=timeslot.end_time,
                    start_time=timeslot.start_time,
                    teacher=timeslot.teacher,
                    day_order=timeslot.day_order,
                    start_seconds=timeslot.time_start_in_seconds,
                    subject=timeslot.subject,
                    teacher_email=timeslot.teacher_email,
                    day_of_month=schedule_date.day,
                    month=schedule_date.month,
                    timestamp=timeslot.start_date_in_schedule,
                    year=schedule_date.year
                )

                pg_insert_job(
                    conn_pool=pg_conn_pool,
                    room_id=timeslot.room_id,
                    job_type="turn_off",
                    job_id=turn_off_name,
                    end_time=timeslot.end_time,
                    start_time=timeslot.start_time,
                    teacher=timeslot.teacher,
                    day_order=timeslot.day_order,
                    start_seconds=timeslot.time_start_in_seconds,
                    subject=timeslot.subject,
                    teacher_email=timeslot.teacher_email,
                    day_of_month=schedule_date.day,
                    timestamp=timeslot.end_date_in_schedule,
                    month=schedule_date.month,
                    year=schedule_date.year
                )

def register_temporary_schedule_handler(db, callback: Callable[
    [List[DocumentSnapshot], List[DocumentChange], Timestamp], None]):
    db.collection(TEMPORARY_SCHEDULE_COLLECTION).on_snapshot(callback)

def on_temporary_schedule_handler(
        doc_snapshot: List[DocumentSnapshot], 
        changes: List[DocumentChange],
        read_time: Timestamp
    ):
        list_of_sched = process_changes_from_firestore(changes)
        day_and_timeslot_dict = extract_resolved_slots_by_day(list_of_sched)
        list_of_day = list(day_and_timeslot_dict.keys())
        schedule_v2(list_of_day, day_and_timeslot_dict, True)

def one_time_job(mqtt_client: mqtt.Client, pg_conn_pool: pg_pool.SimpleConnectionPool):
    job_logs = pg_fetch_all_job_logs(conn_pool=pg_conn_pool)
    for job in job_logs:
        
        # job_definition = pg_fetch_job_by_id(
        #     conn_pool=pg_conn_pool,
        #     job_id=job.job_turn_on_id
        # )
        
        # if job_definition is None:
        #     raise ValueError(f"Job Definition for Job {job.job_turn_on_id}")
        
        end_time = "7:07PM"
        standard_time = str_time_to_standardize_time(
            time_str=end_time
        )
        
        now = datetime.now(ZoneInfo("Asia/Manila"))
        
        time_with_date = time_to_datetime(
            standard_time,
            now.year,
            now.month,
            now.day
        )
        
        print(f"None is {((time_with_date - now).seconds / 60 )} minutes away from turning off")
        

def insert_test_job_logs(
        pg_conn_pool: pg_pool.SimpleConnectionPool,
    ):
    """Insert test job turn on logs for testing detect_rooms_to_warn functionality"""
    from datetime import datetime
    
    # Test job 1 - Room A01
    pg_log_job_pair_run(
        conn_pool=pg_conn_pool,
        job_turn_on_id="test_turn_on_A01_08:00AM_10:00AM_monday",
        job_turn_off_id="test_turn_off_A01_08:00AM_10:00AM_monday",
        room_id="A01",
        result="success",
        details="Test job for room A01"
    )
    
    # Test job 2 - Room B02
    pg_log_job_pair_run(
        conn_pool=pg_conn_pool,
        job_turn_on_id="test_turn_on_B02_02:00PM_04:00PM_tuesday",
        job_turn_off_id="test_turn_off_B02_02:00PM_04:00PM_tuesday",
        room_id="B02",
        result="success",
        details="Test job for room B02"
    )
    
    print("[TEST] Inserted test job logs")

def insert_test_scheduled_jobs(
    pg_conn_pool: pg_pool.SimpleConnectionPool,
    gap_of_job1_end_to_job2_start_min: int,
    duration_of_jobs_min: int
):
    """
    Insert test scheduled jobs with customizable timing
    
    Args:
        minutes_from_now_job1: Minutes from current time for first job's end time
        minutes_from_now_job2: Minutes from current time for second job's end time
    """
    from datetime import datetime, timedelta
    
    current_time = datetime.now(tz=ZoneInfo("Asia/Manila"))
    
    # Calculate end times based on parameters
    
    job1_start_time = current_time
    job1_end_time = current_time + timedelta(minutes=duration_of_jobs_min)
    
    job2_start_time = job1_end_time + timedelta(minutes=gap_of_job1_end_to_job2_start_min)
    job2_end_time = job2_start_time + timedelta(minutes=duration_of_jobs_min)

    
    # Insert test job 1 - Turn Off job
    pg_insert_job(
        conn_pool=pg_conn_pool,
        job_id="test_turn_off_A01_08:00AM_10:00AM_monday",
        room_id="A01",
        job_type="turn_off",
        day_order=0,  # Monday
        start_seconds=job1_end_time.hour * 3600 + job1_end_time.minute * 60,
        start_time=job1_end_time.strftime("%I:%M%p"),
        end_time=job1_end_time.strftime("%I:%M%p"),
        subject="Test Subject 1",
        teacher="Test Teacher 1",
        teacher_email="teacher1@test.com",
        timestamp=job1_end_time,
        day_of_month=job1_end_time.day,
        month=job1_end_time.month,
        year=job1_end_time.year,
        status="scheduled"
    )
    
    # Insert test job 1 - Turn On job
    pg_insert_job(
        conn_pool=pg_conn_pool,
        job_id="test_turn_on_A01_08:00AM_10:00AM_monday",
        room_id="A01",
        job_type="turn_on",
        day_order=0,  # Monday
        start_seconds=job1_start_time.hour * 3600 + job1_start_time.minute * 60,
        start_time=job1_start_time.strftime("%I:%M%p"),
        end_time=job1_end_time.strftime("%I:%M%p"),
        subject="Test Subject 1",
        teacher="Test Teacher 1", 
        teacher_email="teacher1@test.com",
        timestamp=job1_start_time,
        day_of_month=job1_start_time.day,
        month=job1_start_time.month,
        year=job1_start_time.year,
        status="scheduled"
    )
    
    # Insert test job 2 - Turn Off job
    pg_insert_job(
        conn_pool=pg_conn_pool,
        job_id="test_turn_off_B02_02:00PM_04:00PM_tuesday",
        room_id="A01",
        job_type="turn_off",
        day_order=1,  # Tuesday
        start_seconds=job2_end_time.hour * 3600 + job2_end_time.minute * 60,
        start_time=job2_end_time.strftime("%I:%M%p"),
        end_time=job2_end_time.strftime("%I:%M%p"),
        subject="Test Subject 2",
        teacher="Test Teacher 2",
        teacher_email="teacher2@test.com",
        timestamp=job2_end_time,
        day_of_month=job2_end_time.day,
        month=job2_end_time.month,
        year=job2_end_time.year,
        status="scheduled"
    )
    
    # Insert test job 2 - Turn On job
    pg_insert_job(
        conn_pool=pg_conn_pool,
        job_id="test_turn_on_B02_02:00PM_04:00PM_tuesday",
        room_id="A01",
        job_type="turn_on",
        day_order=1,  # Tuesday
        start_seconds=job2_start_time.hour * 3600 + job2_start_time.minute * 60,
        start_time=job2_start_time.strftime("%I:%M%p"),
        end_time=job2_end_time.strftime("%I:%M%p"),
        subject="Test Subject 2",
        teacher="Test Teacher 2",
        teacher_email="teacher2@test.com",
        day_of_month=job2_start_time.day,
        month=job2_start_time.month,
        year=job2_start_time.year,
        timestamp=job2_start_time,
        status="scheduled"
    )
    
  
def setup_test_data(
    pg_conn_pool: pg_pool.SimpleConnectionPool,
    job1_minutes_from_now: int = 2,
    job2_minutes_from_now: int = 5
):
    """
    Setup complete test data for testing detect_rooms_to_warn
    
    Args:
        job1_minutes_from_now: Minutes from now when first job should end
        job2_minutes_from_now: Minutes from now when second job should end
    """
    print("[TEST] Setting up test data...")
    insert_test_job_logs(pg_conn_pool)
    insert_test_scheduled_jobs(pg_conn_pool, gap_of_job1_end_to_job2_start_min=10 , duration_of_jobs_min=30)
    print("[TEST] Test data setup complete!")

if __name__ == "__main__":
    load_config()
    pg_conn_pool = load_pg()
    mqtt_client = load_mqtt()
    service_acc = get_service_acc_path()
    
    firestore_db = init_firestore(service_account_path=service_acc)
    scheduler_v2 = init_scheduler()
    pg_init_job_db(conn_pool=pg_conn_pool)
    
    setup_tables(pg_conn_pool=pg_conn_pool)
    clear_tables(pg_conn_pool=pg_conn_pool)
    clear_scheduler(scheduler=scheduler_v2)
    
    # scheduler_v2.add_job(
    #     func=one_time_job,
    #     trigger=IntervalTrigger(seconds=1),  # Runs immediately once
    #     args=[mqtt_client, pg_conn_pool]
    # )
    
    # Setup test data - customize timing here
    # First job ends in 2 minutes, second job ends in 5 minutes
    setup_test_data(
        pg_conn_pool=pg_conn_pool,
        job1_minutes_from_now=2,
        job2_minutes_from_now=5
    )
    job_count = pg_query_room_for_nearby_star_sched(
        conn_pool=pg_conn_pool,
        job_id='test_turn_off_A01_08:00AM_10:00AM_monday',
        threshhold_minutes=40
    )
    print(f"{job_count} nearby jobs")
    
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[SYSTEM] Exiting gracefully.")














