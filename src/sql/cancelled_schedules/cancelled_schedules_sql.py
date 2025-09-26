"""
SQL functions for managing cancelled schedules.
Tracks both future cancellations and cancellation metadata.
"""

import psycopg2
from psycopg2 import pool as pg_pool
from typing import Optional, List
import datetime
from dataclasses import dataclass

@dataclass
class CancelledSchedule:
    """Model for cancelled schedule entries."""
    timeslot_id: str
    cancellation_type: str  # "permanent_instance" or "temporary_complete"
    cancelled_at: datetime.datetime
    cancelled_date: str  # For permanent schedules: specific date (YYYY-MM-DD), for temporary: "all"
    reason: Optional[str] = None
    cancelled_by: Optional[str] = None


def pg_create_cancelled_schedules_table(conn_pool: pg_pool.SimpleConnectionPool) -> bool:
    """
    Create the cancelled_schedules table to track future schedule cancellations.
    
    Args:
        conn_pool: Database connection pool
        
    Returns:
        True if successful, False otherwise
    """
    conn = None
    cursor = None
    
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS cancelled_schedules (
            id SERIAL PRIMARY KEY,
            timeslot_id VARCHAR(255) NOT NULL,
            cancellation_type VARCHAR(50) NOT NULL, -- 'permanent_instance' or 'temporary_complete'
            cancelled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            cancelled_date VARCHAR(20) NOT NULL, -- 'YYYY-MM-DD' for permanent, 'all' for temporary
            reason TEXT,
            cancelled_by VARCHAR(255),
            UNIQUE(timeslot_id, cancelled_date) -- Prevent duplicate cancellations for same timeslot/date
        );
        
        -- Create index for faster lookups during job execution
        CREATE INDEX IF NOT EXISTS idx_cancelled_schedules_timeslot_date 
        ON cancelled_schedules(timeslot_id, cancelled_date);
        
        -- Create index for cleanup queries
        CREATE INDEX IF NOT EXISTS idx_cancelled_schedules_cancelled_at 
        ON cancelled_schedules(cancelled_at);
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        
        print("✅ cancelled_schedules table created successfully")
        return True
        
    except Exception as e:
        print(f"❌ Error creating cancelled_schedules table: {e}")
        if conn:
            conn.rollback()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_insert_cancelled_schedule(
    conn_pool: pg_pool.SimpleConnectionPool,
    cancelled_schedule: CancelledSchedule
) -> bool:
    """
    Insert a cancelled schedule record.
    
    Args:
        conn_pool: Database connection pool
        cancelled_schedule: CancelledSchedule object to insert
        
    Returns:
        True if successful, False otherwise
    """
    conn = None
    cursor = None
    
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO cancelled_schedules 
        (timeslot_id, cancellation_type, cancelled_at, cancelled_date, reason, cancelled_by)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (timeslot_id, cancelled_date) 
        DO UPDATE SET
            cancellation_type = EXCLUDED.cancellation_type,
            cancelled_at = EXCLUDED.cancelled_at,
            reason = EXCLUDED.reason,
            cancelled_by = EXCLUDED.cancelled_by
        """
        
        cursor.execute(insert_query, (
            cancelled_schedule.timeslot_id,
            cancelled_schedule.cancellation_type,
            cancelled_schedule.cancelled_at,
            cancelled_schedule.cancelled_date,
            cancelled_schedule.reason,
            cancelled_schedule.cancelled_by
        ))
        
        conn.commit()
        print(f"✅ Cancelled schedule inserted: {cancelled_schedule.timeslot_id} on {cancelled_schedule.cancelled_date}")
        return True
        
    except Exception as e:
        print(f"❌ Error inserting cancelled schedule: {e}")
        if conn:
            conn.rollback()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_is_schedule_cancelled(
    conn_pool: pg_pool.SimpleConnectionPool,
    timeslot_id: str,
    execution_date: str
) -> bool:
    """
    Check if a schedule is cancelled for a specific execution date.
    
    Args:
        conn_pool: Database connection pool
        timeslot_id: Timeslot ID to check
        execution_date: Date in YYYY-MM-DD format
        
    Returns:
        True if cancelled, False otherwise
    """
    conn = None
    cursor = None
    
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        # Check for both specific date cancellation and complete temporary cancellation
        check_query = """
        SELECT COUNT(*) FROM cancelled_schedules 
        WHERE timeslot_id = %s 
        """
        
        cursor.execute(check_query, (timeslot_id,))
        count = cursor.fetchone()[0]
        
        is_cancelled = count > 0
        if is_cancelled:
            print(f"🚫 Schedule {timeslot_id} is cancelled for {execution_date}")
        
        return is_cancelled
        
    except Exception as e:
        print(f"❌ Error checking schedule cancellation: {e}")
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_get_cancelled_schedules(
    conn_pool: pg_pool.SimpleConnectionPool,
    timeslot_id: Optional[str] = None
) -> List[CancelledSchedule]:
    """
    Get cancelled schedule records, optionally filtered by timeslot_id.
    
    Args:
        conn_pool: Database connection pool
        timeslot_id: Optional timeslot ID to filter by
        
    Returns:
        List of CancelledSchedule objects
    """
    conn = None
    cursor = None
    
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        if timeslot_id:
            query = """
            SELECT timeslot_id, cancellation_type, cancelled_at, cancelled_date, reason, cancelled_by
            FROM cancelled_schedules 
            WHERE timeslot_id = %s
            ORDER BY cancelled_at DESC
            """
            cursor.execute(query, (timeslot_id,))
        else:
            query = """
            SELECT timeslot_id, cancellation_type, cancelled_at, cancelled_date, reason, cancelled_by
            FROM cancelled_schedules 
            ORDER BY cancelled_at DESC
            """
            cursor.execute(query)
        
        rows = cursor.fetchall()
        cancelled_schedules = []
        
        for row in rows:
            cancelled_schedules.append(CancelledSchedule(
                timeslot_id=row[0],
                cancellation_type=row[1],
                cancelled_at=row[2],
                cancelled_date=row[3],
                reason=row[4],
                cancelled_by=row[5]
            ))
        
        return cancelled_schedules
        
    except Exception as e:
        print(f"❌ Error getting cancelled schedules: {e}")
        return []
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_remove_cancelled_schedule(
    conn_pool: pg_pool.SimpleConnectionPool,
    timeslot_id: str,
    cancelled_date: str
) -> bool:
    """
    Remove a cancelled schedule record (for cleanup or uncancellation).
    
    Args:
        conn_pool: Database connection pool
        timeslot_id: Timeslot ID
        cancelled_date: Cancelled date to remove
        
    Returns:
        True if successful, False otherwise
    """
    conn = None
    cursor = None
    
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        delete_query = """
        DELETE FROM cancelled_schedules 
        WHERE timeslot_id = %s 
        """
        
        cursor.execute(delete_query, (timeslot_id,))
        affected_rows = cursor.rowcount
        conn.commit()
        
        if affected_rows > 0:
            print(f"✅ Removed cancellation: {timeslot_id} on {cancelled_date}")
            return True
        else:
            print(f"⚠️ No cancellation found to remove: {timeslot_id} on {cancelled_date}")
            return False
        
    except Exception as e:
        print(f"❌ Error removing cancelled schedule: {e}")
        if conn:
            conn.rollback()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_clear_all_cancelled_schedules(
    conn_pool: pg_pool.SimpleConnectionPool
) -> bool:
    """
    Clear all cancelled schedule records from the table.
    
    Args:
        conn_pool: Database connection pool
        
    Returns:
        True if successful, False otherwise
    """
    conn = None
    cursor = None
    
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        # Clear all records from cancelled_schedules table
        clear_query = "DELETE FROM cancelled_schedules"
        cursor.execute(clear_query)
        affected_rows = cursor.rowcount
        conn.commit()
        
        print(f"🧹 Cleared {affected_rows} cancelled schedule records")
        return True
        
    except Exception as e:
        print(f"❌ Error clearing cancelled schedules: {e}")
        if conn:
            conn.rollback()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_cleanup_old_cancelled_schedules(
    conn_pool: pg_pool.SimpleConnectionPool,
    days_old: int = 30
) -> bool:
    """
    Clean up old cancelled schedule records to prevent table bloat.
    
    Args:
        conn_pool: Database connection pool
        days_old: Remove records older than this many days
        
    Returns:
        True if successful, False otherwise
    """
    conn = None
    cursor = None
    
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        cleanup_query = """
        DELETE FROM cancelled_schedules 
        WHERE cancelled_at < CURRENT_TIMESTAMP - INTERVAL '%s days'
        """
        
        cursor.execute(cleanup_query, (days_old,))
        affected_rows = cursor.rowcount
        conn.commit()
        
        print(f"🧹 Cleaned up {affected_rows} old cancelled schedule records")
        return True
        
    except Exception as e:
        print(f"❌ Error cleaning up cancelled schedules: {e}")
        if conn:
            conn.rollback()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)