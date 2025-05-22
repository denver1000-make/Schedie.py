import sqlite3
from datetime import datetime

DB_NAME = "full_schedule.db"


def create_tables():
    conn = sqlite3.connect(DB_NAME)
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

    conn.commit()
    conn.close()
    print("[INFO] Tables created successfully.")


def insert_schedule(room_id):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO schedules (room_id) VALUES (?)", (room_id,))
    schedule_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return schedule_id


def insert_schedule_day(schedule_id, day_name, day_order):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO schedule_days (schedule_id, day_name, day_order)
        VALUES (?, ?, ?)
    ''', (schedule_id, day_name, day_order))
    day_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return day_id


def insert_time_slot(day_id, start_time, end_time, subject, teacher, teacher_email, time_start_seconds):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO time_slots (day_id, start_time, end_time, subject, teacher, teacher_email, time_start_seconds)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (day_id, start_time, end_time, subject, teacher, teacher_email, time_start_seconds))
    conn.commit()
    conn.close()


# Example parser to migrate from Firestore-like dicts
def process_schedule_data(schedule_docs):
    create_tables()

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
