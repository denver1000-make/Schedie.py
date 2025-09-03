from datetime import datetime
import os
from typing import Callable, Dict, List
from zoneinfo import ZoneInfo
from psycopg2 import pool as pg_pool
from src.firestore.firestore_settings import init_firestore
from src.firestore.schedule_firestore import extract_resolved_slots_by_day, process_changes_from_firestore
from src.modelsV2.model import ResolvedScheduleSlot
from src.mqtt.mqtt_manager import init_mqtt, publish_v2
from src.schedulerv2.scheduler_v2 import gen_job_name, generate_cron_trig, init_scheduler, parse_time
from src.sql.db_connection import get_pg_connection
from psycopg2 import pool, extensions
import paho.mqtt.client as mqtt
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from src.sql.jobs_sql import pg_clear_all_jobs, pg_init_job_db, pg_insert_job, pg_remove_job_by_job_id 
from src.sql.schedule_sql import log_job_pair_run, pg_clear_all_schedule_data, pg_create_schedule_table, pg_log_job_pair_run, pg_remove_job_pair_run
from src.sql.settings_sql import pg_create_settings_table
from src.utils.time_utils.time_utils import strip_date_of_start_time
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.base import BaseTrigger
from google.cloud.firestore_v1 import Client as FirestoreClient
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


def clear_scheduler(scheduler):
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

    if pg_pool is not None:
        pg_log_job_pair_run(
            conn_pool=pg_pool,
            job_turn_off_id=job_turn_off_id,
            job_turn_on_id=job_turn_on_id,
            room_id=room_id
        )


def turn_off(
        room_id: str,
        job_turn_off_id: str,
        job_turn_on_id: str,
        isTemp: bool,
        mqtt_client: mqtt.Client,
        pg_conn_pool: pool.SimpleConnectionPool
):
    
    if isTemp: 
        pg_remove_job_by_job_id(conn_pool=pg_conn_pool, job_id=job_turn_off_id)
        pg_remove_job_by_job_id(conn_pool=pg_conn_pool, job_id=job_turn_on_id)
    
    pg_remove_job_pair_run(
        conn_pool=pg_conn_pool,
        room_id=room_id,
        job_turn_off_id=job_turn_off_id,
        job_turn_on_id=job_turn_on_id
    )
    
    publish_v2(client=mqtt_client, topic="turn_off", msg=room_id, log=True)



def schedule_v2(
        list_of_day: List[str],
        day_and_timeslot_dict: Dict[str, List[ResolvedScheduleSlot]],
        scheduler_v2: AsyncIOScheduler,
        mqtt_client: mqtt.Client,
        pg_conn_pool: pool.SimpleConnectionPool,
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
                turn_off_name,
                turn_on_name,
                mqtt_client,
                pg_conn_pool
            ])
            scheduler_v2.add_job(turn_off, trigger=turnOffBaseTrigger, args=[
                timeslot.room_id,
                turn_off_name,
                turn_on_name,
                isTemp,
                mqtt_client,
                pg_conn_pool
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

def send_heartbeat(mqtt_client: mqtt.Client):
    mqtt_client.publish("hub_heartbeat", qos=2, payload=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")



async def main():
    
    load_config()
    pg_conn_pool = load_pg()
    mqtt_client = load_mqtt()
    service_acc = get_service_acc_path()

    firestore_db = init_firestore(service_account_path=service_acc)
    scheduler_v2 = await init_scheduler()
    pg_init_job_db(conn_pool=pg_conn_pool)

    setup_tables(pg_conn_pool=pg_conn_pool)
    clear_tables(pg_conn_pool=pg_conn_pool)
    clear_scheduler(scheduler=scheduler_v2)

    scheduler_v2.add_job(
        func=send_heartbeat,
        trigger=IntervalTrigger(seconds=1),
        args=[mqtt_client]
    )
    
    def on_temp_schedule_updated(col_snapshot, changes, read_time):
        added_changes = [change for change in changes if change.type.name == 'ADDED']
        if added_changes:
            slots = process_changes_from_firestore(docs=changes)
            normalized_dat_and_timeslot_dict = extract_resolved_slots_by_day(schedules=slots)
            schedule_v2(
                list(normalized_dat_and_timeslot_dict.keys()),
                day_and_timeslot_dict=normalized_dat_and_timeslot_dict,
                isTemp=True,
                mqtt_client=mqtt_client,
                scheduler_v2=scheduler_v2,
                pg_conn_pool=pg_conn_pool,
            )
            
    
    firestore_db.collection("temporary_schedules").on_snapshot(
        callback=on_temp_schedule_updated
    )


if __name__ == "__main__":
    import asyncio
    
    async def run_main():
        await main()
        
        # Keep the program running
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            print("[SYSTEM] Exiting gracefully.")
    
    # Run the async main function
    asyncio.run(run_main())
