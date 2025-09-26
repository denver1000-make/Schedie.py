from typing import List, Optional
from psycopg2 import pool
import psycopg2

from src.modelsV2.model import ScheduleWrapper


def pg_create_schedule_wrappers_table(conn_pool: pool.SimpleConnectionPool) -> bool:
    """
    Create the schedule_wrappers table for storing ScheduleWrapper data
    This table will be referenced by resolved_schedule_slots via foreign key
    
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
        CREATE TABLE IF NOT EXISTS schedule_wrappers (
            schedule_id VARCHAR(255) PRIMARY KEY,
            upload_date_epoch BIGINT NOT NULL,
            is_temporary BOOLEAN DEFAULT FALSE,
            is_synced_to_remote BOOLEAN DEFAULT TRUE,
            is_from_remote BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_schedule_wrappers_upload_date ON schedule_wrappers(upload_date_epoch)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_schedule_wrappers_is_temporary ON schedule_wrappers(is_temporary)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_schedule_wrappers_sync_status ON schedule_wrappers(is_synced_to_remote, is_from_remote)")
        
        conn.commit()
        print("✅ Created schedule_wrappers table with indexes")
        return True
        
    except Exception as e:
        print(f"❌ Error creating schedule_wrappers table: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_insert_schedule_wrapper(conn_pool: pool.SimpleConnectionPool, wrapper: ScheduleWrapper) -> bool:
    """
    Insert a ScheduleWrapper into the schedule_wrappers table
    
    Args:
        conn_pool: PostgreSQL connection pool
        wrapper: ScheduleWrapper object to insert
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        # Use ON CONFLICT to handle duplicate schedule_ids (upsert)
        insert_query = """
        INSERT INTO schedule_wrappers (schedule_id, upload_date_epoch, is_temporary, is_synced_to_remote, is_from_remote, updated_at)
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (schedule_id) 
        DO UPDATE SET 
            upload_date_epoch = EXCLUDED.upload_date_epoch,
            is_temporary = EXCLUDED.is_temporary,
            is_synced_to_remote = EXCLUDED.is_synced_to_remote,
            is_from_remote = EXCLUDED.is_from_remote,
            updated_at = CURRENT_TIMESTAMP
        """
        
        cursor.execute(insert_query, (
            wrapper.schedule_id,
            wrapper.upload_date_epoch,
            wrapper.is_temporary,
            wrapper.is_synced_to_remote,
            wrapper.is_from_remote
        ))
        
        conn.commit()
        print(f"✅ Inserted/Updated schedule wrapper: {wrapper.schedule_id}")
        return True
        
    except Exception as e:
        print(f"❌ Error inserting schedule wrapper: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_get_schedule_wrapper_by_id(conn_pool: pool.SimpleConnectionPool, schedule_id: str) -> Optional[ScheduleWrapper]:
    """
    Get a specific schedule wrapper by schedule_id
    
    Args:
        conn_pool: PostgreSQL connection pool
        schedule_id: The schedule ID to search for
        
    Returns:
        ScheduleWrapper object if found, None otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        query = """
        SELECT schedule_id, upload_date_epoch, is_temporary, created_at, updated_at
        FROM schedule_wrappers 
        WHERE schedule_id = %s
        """
        
        cursor.execute(query, (schedule_id,))
        result = cursor.fetchone()
        
        if result:
            wrapper = ScheduleWrapper(
                schedule_id=result[0],
                upload_date_epoch=result[1],
                is_temporary=result[2]
            )
            print(f"✅ Found schedule wrapper: {schedule_id}")
            return wrapper
        else:
            print(f"⚠️ No schedule wrapper found with schedule_id: {schedule_id}")
            return None
        
    except Exception as e:
        print(f"❌ Error getting schedule wrapper: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_delete_schedule_wrapper(conn_pool: pool.SimpleConnectionPool, schedule_id: str) -> bool:
    """
    Delete a schedule wrapper by schedule_id
    Note: This will also cascade delete all related resolved_schedule_slots
    
    Args:
        conn_pool: PostgreSQL connection pool
        schedule_id: The schedule ID to delete
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        delete_query = "DELETE FROM schedule_wrappers WHERE schedule_id = %s"
        cursor.execute(delete_query, (schedule_id,))
        
        deleted_count = cursor.rowcount
        conn.commit()
        
        print(f"✅ Deleted schedule wrapper: {schedule_id} (affected {deleted_count} rows)")
        return True
        
    except Exception as e:
        print(f"❌ Error deleting schedule wrapper: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_get_all_schedule_wrappers(conn_pool: pool.SimpleConnectionPool, is_temporary: Optional[bool] = None) -> List[ScheduleWrapper]:
    """
    Get all schedule wrappers, optionally filtered by temporary status
    
    Args:
        conn_pool: PostgreSQL connection pool
        is_temporary: Filter by temporary status (None for all)
        
    Returns:
        List of ScheduleWrapper objects
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        if is_temporary is not None:
            query = """
            SELECT schedule_id, upload_date_epoch, is_temporary, created_at, updated_at
            FROM schedule_wrappers 
            WHERE is_temporary = %s
            ORDER BY upload_date_epoch DESC
            """
            cursor.execute(query, (is_temporary,))
        else:
            query = """
            SELECT schedule_id, upload_date_epoch, is_temporary, created_at, updated_at
            FROM schedule_wrappers 
            ORDER BY upload_date_epoch DESC
            """
            cursor.execute(query)
        
        results = cursor.fetchall()
        
        wrappers = []
        for result in results:
            wrapper = ScheduleWrapper(
                schedule_id=result[0],
                upload_date_epoch=result[1],
                is_temporary=result[2]
            )
            wrappers.append(wrapper)
        
        print(f"✅ Retrieved {len(wrappers)} schedule wrappers")
        return wrappers
        
    except Exception as e:
        print(f"❌ Error getting schedule wrappers: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_get_latest_schedule_wrapper(conn_pool: pool.SimpleConnectionPool, is_temporary: Optional[bool] = None) -> Optional[ScheduleWrapper]:
    """
    Get the most recent schedule wrapper by upload_date_epoch
    
    Args:
        conn_pool: PostgreSQL connection pool
        is_temporary: Filter by temporary status (None for all)
        
    Returns:
        Latest ScheduleWrapper object if found, None otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        if is_temporary is not None:
            query = """
            SELECT schedule_id, upload_date_epoch, is_temporary, created_at, updated_at
            FROM schedule_wrappers 
            WHERE is_temporary = %s
            ORDER BY upload_date_epoch DESC
            LIMIT 1
            """
            cursor.execute(query, (is_temporary,))
        else:
            query = """
            SELECT schedule_id, upload_date_epoch, is_temporary, created_at, updated_at
            FROM schedule_wrappers 
            ORDER BY upload_date_epoch DESC
            LIMIT 1
            """
            cursor.execute(query)
        
        result = cursor.fetchone()
        
        if result:
            wrapper = ScheduleWrapper(
                schedule_id=result[0],
                upload_date_epoch=result[1],
                is_temporary=result[2]
            )
            print(f"✅ Found latest schedule wrapper: {wrapper.schedule_id}")
            return wrapper
        else:
            print("⚠️ No schedule wrappers found")
            return None
        
    except Exception as e:
        print(f"❌ Error getting latest schedule wrapper: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_clear_all_schedule_data(conn_pool: pool.SimpleConnectionPool) -> bool:
    """
    Clear all data from both schedule_wrappers and resolved_schedule_slots tables
    Due to foreign key CASCADE DELETE, deleting from schedule_wrappers will automatically 
    delete all related resolved_schedule_slots
    
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
        
        # Get counts before deletion for reporting
        cursor.execute("SELECT COUNT(*) FROM resolved_schedule_slots")
        slots_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM schedule_wrappers")
        wrappers_count = cursor.fetchone()[0]
        
        # Delete from parent table - this will CASCADE to child table
        cursor.execute("DELETE FROM schedule_wrappers")
        
        conn.commit()
        
        print(f"✅ Cleared all schedule data:")
        print(f"   - Deleted {wrappers_count} schedule wrappers")
        print(f"   - Cascaded deletion of {slots_count} resolved schedule slots")
        return True
        
    except Exception as e:
        print(f"❌ Error clearing schedule data: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_clear_temporary_schedules(conn_pool: pool.SimpleConnectionPool) -> bool:
    """
    Clear only temporary schedules and their related slots
    
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
        
        # Get counts before deletion for reporting
        cursor.execute("""
            SELECT COUNT(*) FROM resolved_schedule_slots rs
            JOIN schedule_wrappers sw ON rs.schedule_id = sw.schedule_id
            WHERE sw.is_temporary = true
        """)
        temp_slots_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM schedule_wrappers WHERE is_temporary = true")
        temp_wrappers_count = cursor.fetchone()[0]
        
        # Delete temporary schedule wrappers - this will CASCADE to their slots
        cursor.execute("DELETE FROM schedule_wrappers WHERE is_temporary = true")
        
        conn.commit()
        
        print(f"✅ Cleared temporary schedule data:")
        print(f"   - Deleted {temp_wrappers_count} temporary schedule wrappers")
        print(f"   - Cascaded deletion of {temp_slots_count} temporary resolved schedule slots")
        return True
        
    except Exception as e:
        print(f"❌ Error clearing temporary schedule data: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_clear_permanent_schedules(conn_pool: pool.SimpleConnectionPool) -> bool:
    """
    Clear only permanent (non-temporary) schedules and their related slots
    
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
        
        # Get counts before deletion for reporting
        cursor.execute("""
            SELECT COUNT(*) FROM resolved_schedule_slots rs
            JOIN schedule_wrappers sw ON rs.schedule_id = sw.schedule_id
            WHERE sw.is_temporary = false
        """)
        perm_slots_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM schedule_wrappers WHERE is_temporary = false")
        perm_wrappers_count = cursor.fetchone()[0]
        
        # Delete permanent schedule wrappers - this will CASCADE to their slots
        cursor.execute("DELETE FROM schedule_wrappers WHERE is_temporary = false")
        
        conn.commit()
        
        print(f"✅ Cleared permanent schedule data:")
        print(f"   - Deleted {perm_wrappers_count} permanent schedule wrappers")
        print(f"   - Cascaded deletion of {perm_slots_count} permanent resolved schedule slots")
        return True
        
    except Exception as e:
        print(f"❌ Error clearing permanent schedule data: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_get_schedule_wrappers_by_sync_status(conn_pool: pool.SimpleConnectionPool, is_synced_to_remote: Optional[bool] = None, is_from_remote: Optional[bool] = None) -> List[ScheduleWrapper]:
    """
    Get ScheduleWrapper objects filtered by sync status
    
    Args:
        conn_pool: PostgreSQL connection pool
        is_synced_to_remote: Filter by sync to remote status (None for all)
        is_from_remote: Filter by remote origin status (None for all)
        
    Returns:
        List of ScheduleWrapper objects
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        base_query = """
        SELECT schedule_id, upload_date_epoch, is_temporary, is_synced_to_remote, is_from_remote, created_at, updated_at
        FROM schedule_wrappers
        """
        
        conditions = []
        params = []
        
        if is_synced_to_remote is not None:
            conditions.append("is_synced_to_remote = %s")
            params.append(is_synced_to_remote)
            
        if is_from_remote is not None:
            conditions.append("is_from_remote = %s")
            params.append(is_from_remote)
        
        if conditions:
            query = base_query + " WHERE " + " AND ".join(conditions) + " ORDER BY upload_date_epoch DESC"
        else:
            query = base_query + " ORDER BY upload_date_epoch DESC"
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        schedule_wrappers = []
        for result in results:
            wrapper = ScheduleWrapper(
                schedule_id=result[0],
                upload_date_epoch=float(result[1]),
                is_temporary=result[2],
                is_synced_to_remote=result[3],
                is_from_remote=result[4]
            )
            schedule_wrappers.append(wrapper)
        
        print(f"✅ Retrieved {len(schedule_wrappers)} schedule wrappers with sync filters")
        return schedule_wrappers
        
    except Exception as e:
        print(f"❌ Error getting schedule wrappers by sync status: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)
