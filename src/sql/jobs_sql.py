import sqlite3
from typing import List, Optional
from psycopg2 import pool, extensions

from src.modelsV2.model import ScheduledJob

DB_PATH = "scheduled_jobs.db"
conn = sqlite3.connect(DB_PATH, check_same_thread=False)

DAY_NAME_TO_ORDER = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6
}


def init_job_db():
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scheduled_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,                    -- Unique job identifier
            room_id TEXT NOT NULL,
            job_type TEXT NOT NULL,
            day_order INTEGER NOT NULL,
            start_seconds INTEGER NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            subject TEXT,
            teacher TEXT,
            teacher_email TEXT,
            status TEXT DEFAULT 'scheduled'
        )
    ''')

    conn.commit()
    print("[DB] Table initialized.")


def insert_job(
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
        status: str = "scheduled"
):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO scheduled_jobs (
            job_id, room_id, job_type, day_order, start_seconds, start_time,
            end_time, subject, teacher, teacher_email, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (job_id, room_id, job_type, day_order, start_seconds, start_time,
          end_time, subject, teacher, teacher_email, status))
    conn.commit()
    print(f"[DB] Inserted {job_type} job with ID {job_id}")


def fetch_job_by_id(job_id: str) -> Optional[ScheduledJob]:
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, job_id, room_id, job_type, day_order, start_seconds,
               start_time, end_time, subject, teacher, teacher_email, status
        FROM scheduled_jobs
        WHERE job_id = ?
        LIMIT 1
    ''', (job_id,))
    row = cursor.fetchone()
    if row:
        return ScheduledJob(*row)
    return None


def remove_job_by_job_id(job_id: str):
    cursor = conn.cursor()
    cursor.execute('''
        DELETE FROM scheduled_jobs
        WHERE job_id = ?
    ''', (job_id,))
    conn.commit()
    print(f"[DB] Job with ID '{job_id}' removed.")


def clear_all_jobs():
    cursor = conn.cursor()
    cursor.execute("DELETE FROM scheduled_jobs")
    conn.commit()
    print("[DB] All jobs cleared.")


def fetch_jobs_after_start_seconds_for_room_and_day(
        min_start_seconds: int,
        room_id: str,
        day_order: int,
        job_type: str
) -> List[ScheduledJob]:
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, job_id, room_id, job_type, day_order, start_seconds,
               start_time, end_time, subject, teacher, teacher_email, status
        FROM scheduled_jobs
        WHERE start_seconds > ?
          AND room_id = ?
          AND day_order = ?
          AND job_type = ?
        ORDER BY start_seconds ASC
    ''', (min_start_seconds, room_id, day_order, job_type))

    rows = cursor.fetchall()
    return [ScheduledJob(*row) for row in rows]


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
                    status TEXT DEFAULT 'scheduled'
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
        status: str = "scheduled"
):
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            cursor.execute('''
                INSERT INTO scheduled_jobs (
                    job_id, room_id, job_type, day_order, start_seconds, start_time,
                    end_time, subject, teacher, teacher_email, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (job_id, room_id, job_type, day_order, start_seconds, start_time,
                  end_time, subject, teacher, teacher_email, status))
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
                       start_time, end_time, subject, teacher, teacher_email, status
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
                       start_time, end_time, subject, teacher, teacher_email, status
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
