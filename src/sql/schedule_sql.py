import sqlite3
from datetime import datetime
from typing import List
from psycopg2 import pool, extensions
from src.modelsV2.model import JobLogEntry

DB_FOR_SCHEDULE_NAME = "full_schedule.db"
conn = sqlite3.connect(DB_FOR_SCHEDULE_NAME, check_same_thread=False)


def pg_create_schedule_table(conn_pool: pool.SimpleConnectionPool):
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            cursor: extensions.cursor
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS schedules (
                    id SERIAL PRIMARY KEY,
                    room_id TEXT NOT NULL
                );
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS schedule_days (
                    id SERIAL PRIMARY KEY,
                    schedule_id INTEGER NOT NULL,
                    day_name TEXT,
                    day_order INTEGER,
                    FOREIGN KEY(schedule_id) REFERENCES schedules(id)
                );
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS time_slots (
                    id SERIAL PRIMARY KEY,
                    day_id INTEGER NOT NULL,
                    start_time TEXT,
                    end_time TEXT,
                    subject TEXT,
                    teacher TEXT,
                    teacher_email TEXT,
                    time_start_seconds INTEGER,
                    FOREIGN KEY(day_id) REFERENCES schedule_days(id)
                );
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS job_turn_on_logs (
                    id SERIAL PRIMARY KEY,
                    job_turn_on_id TEXT NOT NULL,
                    job_turn_off_id TEXT NOT NULL,
                    room_id TEXT NOT NULL,
                    run_time TEXT NOT NULL,
                    result TEXT,
                    details TEXT
                );
            ''')
            single_conn.commit()
            print("[INFO] Postgres tables created successfully.")
    finally:
        conn_pool.putconn(single_conn)


def create_schedule_tables():
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id TEXT NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS schedule_days (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            schedule_id INTEGER NOT NULL,
            day_name TEXT,
            day_order INTEGER,
            FOREIGN KEY(schedule_id) REFERENCES schedules(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS time_slots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day_id INTEGER NOT NULL,
            start_time TEXT,
            end_time TEXT,
            subject TEXT,
            teacher TEXT,
            teacher_email TEXT,
            time_start_seconds INTEGER,
            FOREIGN KEY(day_id) REFERENCES schedule_days(id)
        )
    ''')

    cursor.execute('''
            CREATE TABLE IF NOT EXISTS job_turn_on_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_turn_on_id TEXT NOT NULL,
                job_turn_off_id TEXT NOT NULL,
                room_id TEXT NOT NULL,
                run_time TEXT NOT NULL,
                result TEXT,
                details TEXT
            )
        ''')

    conn.commit()

    print("[INFO] Tables created successfully.")


def insert_schedule(room_id):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO schedules (room_id) VALUES (?)", (room_id,))
    schedule_id = cursor.lastrowid
    conn.commit()
    return schedule_id


def insert_schedule_day(schedule_id, day_name, day_order):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO schedule_days (schedule_id, day_name, day_order)
        VALUES (?, ?, ?)
    ''', (schedule_id, day_name, day_order))
    day_id = cursor.lastrowid
    conn.commit()
    return day_id


def insert_time_slot(day_id, start_time, end_time, subject, teacher, teacher_email, time_start_seconds):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO time_slots (day_id, start_time, end_time, subject, teacher, teacher_email, time_start_seconds)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (day_id, start_time, end_time, subject, teacher, teacher_email, time_start_seconds))
    conn.commit()


# Example parser to migrate from Firestore-like dicts
def process_schedule_data(schedule_docs):
    create_schedule_tables()

    for doc in schedule_docs:
        room_id = doc.get("roomId")
        schedule_map = doc.get("scheduleOfDayMap", {})

        print(f"[INFO] Inserting schedule for room: {room_id}")
        schedule_id = insert_schedule(room_id)

        for day in schedule_map.values():
            day_name = day.get("dayName")
            day_order = day.get("dayOrder")
            hours = day.get("hours", [])

            print(f"[INFO] Inserting day: {day_name} (order: {day_order})")
            day_id = insert_schedule_day(schedule_id, day_name, day_order)

            for slot in hours:
                insert_time_slot(
                    day_id,
                    slot.get("startTime"),
                    slot.get("endTime"),
                    slot.get("subject"),
                    slot.get("teacher"),
                    slot.get("teacherEmail"),
                    slot.get("timeStartInSeconds")
                )


def fetch_all_job_logs() -> List[JobLogEntry]:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, job_turn_on_id, job_turn_off_id, room_id, run_time, result, details FROM job_turn_on_logs")
    rows = cursor.fetchall()

    return [
        JobLogEntry(*row)
        for row in rows
    ]


def log_job_pair_run(
        job_turn_on_id: str,
        job_turn_off_id: str,
        room_id: str,
        result: str = "success",
        details: str = ""
):
    run_time = datetime.now().isoformat()

    with conn:
        conn.execute('''
            INSERT INTO job_turn_on_logs (
                job_turn_on_id,
                job_turn_off_id,
                room_id,
                run_time,
                result,
                details
            ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (job_turn_on_id, job_turn_off_id, room_id, run_time, result, details))


def clear_all_schedule_data():
    cursor = conn.cursor()
    tables = ["schedules", "schedule_days", "time_slots", "job_turn_on_logs"]

    for table in tables:
        cursor.execute(f"DELETE FROM {table}")
        print(f"[DB] Cleared all data from {table}")

    conn.commit()


def pg_insert_schedule(conn_pool: pool.SimpleConnectionPool, room_id: str) -> int:
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO schedules (room_id) VALUES (%s) RETURNING id;",
                (room_id,)
            )
            result = cursor.fetchone()
            if result is None:
                raise ValueError("Failed to insert schedule")
            schedule_id: int = result[0]
            single_conn.commit()
            return schedule_id
    finally:
        conn_pool.putconn(single_conn)

def pg_insert_schedule_day(conn_pool: pool.SimpleConnectionPool, schedule_id: int, day_name: str, day_order: int) -> int:
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            cursor.execute(
                '''
                INSERT INTO schedule_days (schedule_id, day_name, day_order)
                VALUES (%s, %s, %s) RETURNING id;
                ''',
                (schedule_id, day_name, day_order)
            )
            result = cursor.fetchone()
            if result is None:
                raise ValueError("Failed to insert schedule day")
            day_id = result[0]
            single_conn.commit()
            return day_id
    finally:
        conn_pool.putconn(single_conn)

def pg_insert_time_slot(conn_pool: pool.SimpleConnectionPool, day_id: int, start_time: str, end_time: str, subject: str, teacher: str, teacher_email: str, time_start_seconds: int):
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            cursor.execute(
                '''
                INSERT INTO time_slots (day_id, start_time, end_time, subject, teacher, teacher_email, time_start_seconds)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                ''',
                (day_id, start_time, end_time, subject, teacher, teacher_email, time_start_seconds)
            )
            single_conn.commit()
    finally:
        conn_pool.putconn(single_conn)

def pg_fetch_all_job_logs(conn_pool: pool.SimpleConnectionPool) -> List[JobLogEntry]:
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, job_turn_on_id, job_turn_off_id, room_id, run_time, result, details FROM job_turn_on_logs"
            )
            rows = cursor.fetchall()
            return [JobLogEntry(*row) for row in rows]
    finally:
        conn_pool.putconn(single_conn)

def pg_log_job_pair_run(
        conn_pool: pool.SimpleConnectionPool,
        job_turn_on_id: str,
        job_turn_off_id: str,
        room_id: str,
        result: str = "success",
        details: str = ""
):
    run_time = datetime.now().isoformat()
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            cursor.execute(
                '''
                INSERT INTO job_turn_on_logs (
                    job_turn_on_id,
                    job_turn_off_id,
                    room_id,
                    run_time,
                    result,
                    details
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ''',
                (job_turn_on_id, job_turn_off_id, room_id, run_time, result, details)
            )
            single_conn.commit()
    finally:
        conn_pool.putconn(single_conn)

def pg_clear_all_schedule_data(conn_pool: pool.SimpleConnectionPool):
    single_conn: extensions.connection = conn_pool.getconn()
    try:
        with single_conn.cursor() as cursor:
            tables = ["schedules", "schedule_days", "time_slots", "job_turn_on_logs"]
            for table in tables:
                cursor.execute(f"DELETE FROM {table};")
                print(f"[DB] Cleared all data from {table}")
            single_conn.commit()
    finally:
        conn_pool.putconn(single_conn)
