
import datetime
import sqlite3
from typing import List, Optional
from psycopg2 import pool, extensions

from src.modelsV2.model import ScheduledJob

DAY_NAME_TO_ORDER = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6
}


def pg_fetch_job_log_run(conn_pool: pool.SimpleConnectionPool, job_turn_on_id: str, job_turn_off_id: str, room_id: str) -> Optional[tuple]:
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            cursor.execute('''
                SELECT * FROM job_turn_on_logs
                WHERE job_turn_on_id = %s
                    AND job_turn_off_id = %s
                    AND room_id = %s
                LIMIT 1
            ''', (job_turn_on_id, job_turn_off_id, room_id))
            row = cursor.fetchone()
            return row
    finally:
        conn_pool.putconn(single_conn)


def pg_init_job_db(conn_pool: pool.SimpleConnectionPool):
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scheduled_jobs (
                    id SERIAL PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    room_id TEXT NOT NULL,
                    job_type TEXT NOT NULL,
                    day_order INTEGER NOT NULL,
                    start_seconds INTEGER NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    subject TEXT,
                    teacher TEXT,
                    teacher_email TEXT,
                    status TEXT DEFAULT 'scheduled',
                    day_of_month INTEGER,
                    month INTEGER,
                    year INTEGER,
                    timestamp TIMESTAMPTZ
                );
            ''')
            single_conn.commit()
            print("[DB] Postgres scheduled_jobs table initialized.")
    finally:
        conn_pool.putconn(single_conn)


def pg_insert_job(
        conn_pool: pool.SimpleConnectionPool,
        job_id: str,
        room_id: str,
        job_type: str,
        day_order: int,
        start_seconds: int,
        start_time: str,
        end_time: str,
        subject: Optional[str],
        teacher: Optional[str],
        teacher_email: Optional[str],
        timestamp: Optional[datetime.datetime],
        day_of_month: int,
        month: int,
        year: int,
        status: str = "scheduled"
):
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            cursor.execute('''
                INSERT INTO scheduled_jobs (
                    job_id, room_id, job_type, day_order, start_seconds, start_time,
                    end_time, subject, teacher, teacher_email, status, day_of_month, 
                    month, year, timestamp
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (job_id, room_id, job_type, day_order, start_seconds, start_time,
                  end_time, subject, teacher, teacher_email, status, day_of_month, month, year, timestamp))
            single_conn.commit()
            print(f"[DB] Inserted {job_type} job with ID {job_id} (Postgres)")
    finally:
        conn_pool.putconn(single_conn)


def pg_fetch_job_by_id(conn_pool: pool.SimpleConnectionPool, job_id: str) -> Optional[ScheduledJob]:
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            cursor.execute('''
                SELECT id, job_id, room_id, job_type, day_order, start_seconds,
                       start_time, end_time, subject, teacher, teacher_email, status,
                       day_of_month, month, year
                FROM scheduled_jobs
                WHERE job_id = %s
                LIMIT 1
            ''', (job_id,))
            row = cursor.fetchone()
            if row:
                return ScheduledJob(*row)
            return None
    finally:
        conn_pool.putconn(single_conn)



def pg_remove_job_by_job_id(conn_pool: pool.SimpleConnectionPool, job_id: str):
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            cursor.execute('''
                DELETE FROM scheduled_jobs
                WHERE job_id = %s
            ''', (job_id,))
            single_conn.commit()
            print(f"[DB] Job with ID '{job_id}' removed. (Postgres)")
    finally:
        conn_pool.putconn(single_conn)


def pg_clear_all_jobs(conn_pool: pool.SimpleConnectionPool):
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            cursor.execute("DELETE FROM scheduled_jobs")
            single_conn.commit()
            print("[DB] All jobs cleared. (Postgres)")
    finally:
        conn_pool.putconn(single_conn)

def pg_query_room_for_nearby_start_sched(
        conn_pool: pool.SimpleConnectionPool,
        threshhold_minutes: int,
        job_id: str
    ):
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            cursor.execute('''
                SELECT COUNT(*) 
                FROM scheduled_jobs t_off 
                JOIN scheduled_jobs t_on ON (
                    t_off.room_id = t_on.room_id
                    AND t_on.job_type = 'turn_on'
                    AND t_on.timestamp > t_off.timestamp
                    AND t_on.timestamp <= t_off.timestamp + INTERVAL '%s minutes'
                )
                WHERE t_off.job_id = %s
                AND t_off.job_type = 'turn_off'
            ''', (threshhold_minutes, job_id))
            results = cursor.fetchone()
            if results is None:
                return 0
            count = results[0]
            return count
    finally:
        conn_pool.putconn(single_conn)

def pg_fetch_jobs_after_start_seconds_for_room_and_day(
        conn_pool: pool.SimpleConnectionPool,
        min_start_seconds: int,
        room_id: str,
        day_order: int,
        job_type: str
) -> List[ScheduledJob]:
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            cursor.execute('''
                SELECT id, job_id, room_id, job_type, day_order, start_seconds,
                       start_time, end_time, subject, teacher, teacher_email, status,
                       day_of_month, month, year
                FROM scheduled_jobs
                WHERE start_seconds > %s
                  AND room_id = %s
                  AND day_order = %s
                  AND job_type = %s
                ORDER BY start_seconds ASC
            ''', (min_start_seconds, room_id, day_order, job_type))
            rows = cursor.fetchall()
            return [ScheduledJob(*row) for row in rows]
    finally:
        conn_pool.putconn(single_conn)
