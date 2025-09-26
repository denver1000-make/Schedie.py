from typing import List, Optional
from psycopg2 import pool
import psycopg2

from modelsV2.model import ResolvedScheduleSlotV2


def pg_create_resolved_schedule_slots_table(conn_pool: pool.SimpleConnectionPool) -> bool:
    """
    Create the resolved_schedule_slots table for storing ResolvedScheduleSlotV2 data
    
    Args:
        conn_pool: PostgreSQL connection pool
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS resolved_schedule_slots (
            id SERIAL PRIMARY KEY,
            timeslot_id VARCHAR(255) NOT NULL,
            schedule_id VARCHAR(255) NOT NULL,
            room_id VARCHAR(100) NOT NULL,
            day_name VARCHAR(20) NOT NULL,
            day_order INTEGER NOT NULL,
            start_time VARCHAR(20) NOT NULL,
            end_time VARCHAR(20) NOT NULL,
            subject VARCHAR(200),
            teacher VARCHAR(200),
            teacher_email VARCHAR(255),
            start_hour INTEGER NOT NULL DEFAULT 0,
            start_minute INTEGER NOT NULL DEFAULT 0,
            end_hour INTEGER NOT NULL DEFAULT 0,
            end_minute INTEGER NOT NULL DEFAULT 0,
            time_start_in_seconds INTEGER,
            start_date_in_seconds_epoch BIGINT,
            end_date_in_seconds_epoch BIGINT,
            is_temporary BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_resolved_slots_timeslot_id ON resolved_schedule_slots(timeslot_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_resolved_slots_schedule_id ON resolved_schedule_slots(schedule_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_resolved_slots_room_day ON resolved_schedule_slots(room_id, day_order)")
        
        conn.commit()
        print("✅ Created resolved_schedule_slots table with indexes")
        return True
        
    except Exception as e:
        print(f"❌ Error creating resolved_schedule_slots table: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_insert_resolved_time_slots(conn_pool: pool.SimpleConnectionPool, slots: List[ResolvedScheduleSlotV2], schedule_id: str) -> bool:
    """
    Insert ResolvedScheduleSlotV2 objects into the resolved_schedule_slots table
    
    Args:
        conn_pool: PostgreSQL connection pool
        slots: List of ResolvedScheduleSlotV2 objects to insert
        schedule_id: The schedule ID these slots belong to
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not slots:
        print("⚠️ No slots provided for insertion")
        return True
    
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO resolved_schedule_slots (
            timeslot_id, schedule_id, room_id, day_name, day_order, start_time, end_time,
            subject, teacher, teacher_email, start_hour, start_minute, end_hour, end_minute,
            time_start_in_seconds, start_date_in_seconds_epoch, end_date_in_seconds_epoch, is_temporary
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        slot_data = []
        for slot in slots:
            slot_data.append((
                slot.timeslot_id,
                schedule_id,
                slot.room_id,
                slot.day_name,
                slot.day_order,
                slot.start_time,
                slot.end_time,
                slot.subject or "",
                slot.teacher or "",
                slot.teacher_email or "",
                slot.start_hour,
                slot.start_minute,
                slot.end_hour,
                slot.end_minute,
                slot.time_start_in_seconds,
                slot.start_date_in_seconds_epoch,
                slot.end_date_in_seconds_epoch,
                slot.is_temporary
            ))
        
        cursor.executemany(insert_query, slot_data)
        conn.commit()
        print(f"✅ Inserted {len(slot_data)} resolved schedule slots for schedule_id: {schedule_id}")
        return True
        
    except Exception as e:
        print(f"❌ Error inserting resolved schedule slots: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_delete_resolved_slots_by_timeslot_id(conn_pool: pool.SimpleConnectionPool, timeslot_id: str) -> bool:
    """
    Delete resolved schedule slots by timeslot_id
    
    Args:
        conn_pool: PostgreSQL connection pool
        timeslot_id: The timeslot ID to delete
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        delete_query = "DELETE FROM resolved_schedule_slots WHERE timeslot_id = %s"
        cursor.execute(delete_query, (timeslot_id,))
        
        deleted_count = cursor.rowcount
        conn.commit()
        
        print(f"✅ Deleted {deleted_count} resolved schedule slot(s) with timeslot_id: {timeslot_id}")
        return True
        
    except Exception as e:
        print(f"❌ Error deleting resolved schedule slots by timeslot_id: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_delete_resolved_slots_by_schedule_id(conn_pool: pool.SimpleConnectionPool, schedule_id: str) -> bool:
    """
    Delete all resolved schedule slots by schedule_id
    
    Args:
        conn_pool: PostgreSQL connection pool
        schedule_id: The schedule ID to delete slots for
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        delete_query = "DELETE FROM resolved_schedule_slots WHERE schedule_id = %s"
        cursor.execute(delete_query, (schedule_id,))
        
        deleted_count = cursor.rowcount
        conn.commit()
        
        print(f"✅ Deleted {deleted_count} resolved schedule slot(s) for schedule_id: {schedule_id}")
        return True
        
    except Exception as e:
        print(f"❌ Error deleting resolved schedule slots by schedule_id: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_get_resolved_slots_by_schedule_id(conn_pool: pool.SimpleConnectionPool, schedule_id: str) -> List[ResolvedScheduleSlotV2]:
    """
    Get all resolved schedule slots for a specific schedule_id
    
    Args:
        conn_pool: PostgreSQL connection pool
        schedule_id: The schedule ID to get slots for
        
    Returns:
        List of ResolvedScheduleSlotV2 objects
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        query = """
        SELECT timeslot_id, schedule_id, room_id, day_name, day_order, start_time, end_time,
               subject, teacher, teacher_email, start_hour, start_minute, end_hour, end_minute,
               time_start_in_seconds, start_date_in_seconds_epoch, end_date_in_seconds_epoch, 
               is_temporary, created_at
        FROM resolved_schedule_slots 
        WHERE schedule_id = %s
        ORDER BY day_order, time_start_in_seconds
        """
        
        cursor.execute(query, (schedule_id,))
        results = cursor.fetchall()
        
        slots = []
        for result in results:
            slot = ResolvedScheduleSlotV2(
                timeslot_id=result[0],
                schedule_id=result[1],
                room_id=result[2],
                day_name=result[3],
                day_order=result[4],
                start_time=result[5],
                end_time=result[6],
                subject=result[7] or "",
                teacher=result[8] or "",
                teacher_email=result[9] or "",
                start_hour=result[10],
                start_minute=result[11],
                end_hour=result[12],
                end_minute=result[13],
                time_start_in_seconds=result[14],
                start_date_in_seconds_epoch=result[15],
                end_date_in_seconds_epoch=result[16],
                is_temporary=result[17] or False
            )
            slots.append(slot)
        
        print(f"✅ Retrieved {len(slots)} resolved schedule slots for schedule_id: {schedule_id}")
        return slots
        
    except Exception as e:
        print(f"❌ Error getting resolved schedule slots: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_get_resolved_slot_by_timeslot_id(conn_pool: pool.SimpleConnectionPool, timeslot_id: str) -> Optional[ResolvedScheduleSlotV2]:
    """
    Get a specific resolved schedule slot by timeslot_id
    
    Args:
        conn_pool: PostgreSQL connection pool
        timeslot_id: The timeslot ID to search for
        
    Returns:
        ResolvedScheduleSlotV2 object if found, None otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        query = """
        SELECT timeslot_id, schedule_id, room_id, day_name, day_order, start_time, end_time,
               subject, teacher, teacher_email, start_hour, start_minute, end_hour, end_minute,
               time_start_in_seconds, start_date_in_seconds_epoch, end_date_in_seconds_epoch, 
               is_temporary, created_at
        FROM resolved_schedule_slots 
        WHERE timeslot_id = %s
        """
        
        cursor.execute(query, (timeslot_id,))
        result = cursor.fetchone()
        
        if result:
            slot = ResolvedScheduleSlotV2(
                timeslot_id=result[0],
                schedule_id=result[1],
                room_id=result[2],
                day_name=result[3],
                day_order=result[4],
                start_time=result[5],
                end_time=result[6],
                subject=result[7] or "",
                teacher=result[8] or "",
                teacher_email=result[9] or "",
                start_hour=result[10],
                start_minute=result[11],
                end_hour=result[12],
                end_minute=result[13],
                time_start_in_seconds=result[14],
                start_date_in_seconds_epoch=result[15],
                end_date_in_seconds_epoch=result[16],
                is_temporary=result[17] or False
            )
            print(f"✅ Found resolved schedule slot with timeslot_id: {timeslot_id}")
            return slot
        else:
            print(f"⚠️ No resolved schedule slot found with timeslot_id: {timeslot_id}")
            return None
        
    except Exception as e:
        print(f"❌ Error getting resolved schedule slot by timeslot_id: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)