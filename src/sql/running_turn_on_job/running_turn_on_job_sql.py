from typing import List, Optional, Tuple
from psycopg2 import pool
import psycopg2

from src.modelsV2.model import RunningTurnOnJob, ResolvedScheduleSlotV2


def pg_create_running_turn_on_jobs_table(conn_pool: pool.SimpleConnectionPool) -> bool:
    """
    Create the running_turn_on_jobs table for storing RunningTurnOnJob data
    
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
        CREATE TABLE IF NOT EXISTS running_turn_on_jobs (
            id SERIAL PRIMARY KEY,
            timeslot_id VARCHAR(255) NOT NULL UNIQUE,
            is_temporary BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_running_jobs_timeslot_id ON running_turn_on_jobs(timeslot_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_running_jobs_is_temporary ON running_turn_on_jobs(is_temporary)")
        
        conn.commit()
        print("✅ Created running_turn_on_jobs table with indexes")
        return True
        
    except Exception as e:
        print(f"❌ Error creating running_turn_on_jobs table: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_insert_running_turn_on_job(conn_pool: pool.SimpleConnectionPool, job: RunningTurnOnJob) -> bool:
    """
    Insert a RunningTurnOnJob into the running_turn_on_jobs table
    
    Args:
        conn_pool: PostgreSQL connection pool
        job: RunningTurnOnJob object to insert
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        # Use ON CONFLICT to handle duplicate timeslot_ids (upsert)
        insert_query = """
        INSERT INTO running_turn_on_jobs (timeslot_id, is_temporary, updated_at)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (timeslot_id) 
        DO UPDATE SET 
            is_temporary = EXCLUDED.is_temporary,
            updated_at = CURRENT_TIMESTAMP
        """
        
        cursor.execute(insert_query, (
            job.timeslot_id,
            job.is_temporary
        ))
        
        conn.commit()
        print(f"✅ Inserted/Updated running turn on job: {job.timeslot_id}")
        return True
        
    except Exception as e:
        print(f"❌ Error inserting running turn on job: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_remove_running_turn_on_job(conn_pool: pool.SimpleConnectionPool, timeslot_id: str) -> bool:
    """
    Remove a RunningTurnOnJob from the running_turn_on_jobs table by timeslot_id
    
    Args:
        conn_pool: PostgreSQL connection pool
        timeslot_id: The timeslot ID to remove
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        delete_query = "DELETE FROM running_turn_on_jobs WHERE timeslot_id = %s"
        cursor.execute(delete_query, (timeslot_id,))
        
        deleted_count = cursor.rowcount
        conn.commit()
        
        if deleted_count > 0:
            print(f"✅ Removed running turn on job: {timeslot_id}")
            return True
        else:
            print(f"⚠️ No running turn on job found with timeslot_id: {timeslot_id}")
            return False
        
    except Exception as e:
        print(f"❌ Error removing running turn on job: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_get_running_turn_on_job(conn_pool: pool.SimpleConnectionPool, timeslot_id: str) -> Optional[RunningTurnOnJob]:
    """
    Get a specific RunningTurnOnJob by timeslot_id
    
    Args:
        conn_pool: PostgreSQL connection pool
        timeslot_id: The timeslot ID to search for
        
    Returns:
        RunningTurnOnJob object if found, None otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        query = """
        SELECT timeslot_id, is_temporary, created_at, updated_at
        FROM running_turn_on_jobs 
        WHERE timeslot_id = %s
        """
        
        cursor.execute(query, (timeslot_id,))
        result = cursor.fetchone()
        
        if result:
            job = RunningTurnOnJob(
                timeslot_id=result[0],
                is_temporary=result[1]
            )
            print(f"✅ Found running turn on job: {timeslot_id}")
            return job
        else:
            print(f"⚠️ No running turn on job found with timeslot_id: {timeslot_id}")
            return None
        
    except Exception as e:
        print(f"❌ Error getting running turn on job: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_get_all_running_turn_on_jobs(conn_pool: pool.SimpleConnectionPool, is_temporary: Optional[bool] = None) -> List[RunningTurnOnJob]:
    """
    Get all running turn on jobs, optionally filtered by temporary status
    
    Args:
        conn_pool: PostgreSQL connection pool
        is_temporary: Filter by temporary status (None for all)
        
    Returns:
        List of RunningTurnOnJob objects
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        if is_temporary is not None:
            query = """
            SELECT timeslot_id, is_temporary, created_at, updated_at
            FROM running_turn_on_jobs 
            WHERE is_temporary = %s
            ORDER BY created_at DESC
            """
            cursor.execute(query, (is_temporary,))
        else:
            query = """
            SELECT timeslot_id, is_temporary, created_at, updated_at
            FROM running_turn_on_jobs 
            ORDER BY created_at DESC
            """
            cursor.execute(query)
        
        results = cursor.fetchall()
        
        jobs = []
        for result in results:
            job = RunningTurnOnJob(
                timeslot_id=result[0],
                is_temporary=result[1]
            )
            jobs.append(job)
        
        print(f"✅ Retrieved {len(jobs)} running turn on jobs")
        return jobs
        
    except Exception as e:
        print(f"❌ Error getting running turn on jobs: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_get_running_job_with_schedule_slot(conn_pool: pool.SimpleConnectionPool, timeslot_id: str) -> Tuple[Optional[RunningTurnOnJob], Optional[ResolvedScheduleSlotV2]]:
    """
    Get a RunningTurnOnJob and its associated ResolvedScheduleSlotV2 by timeslot_id
    This uses a JOIN to fetch both the running job and the schedule slot information
    
    Args:
        conn_pool: PostgreSQL connection pool
        timeslot_id: The timeslot ID to search for
        
    Returns:
        Tuple of (RunningTurnOnJob, ResolvedScheduleSlotV2) or (None, None) if not found
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        # JOIN query to get both running job and resolved schedule slot
        query = """
        SELECT 
            rtoj.timeslot_id, rtoj.is_temporary as job_is_temporary, rtoj.created_at, rtoj.updated_at,
            rss.schedule_id, rss.room_id, rss.day_name, rss.day_order, rss.start_time, rss.end_time,
            rss.subject, rss.teacher, rss.teacher_email, rss.start_hour, rss.start_minute, 
            rss.end_hour, rss.end_minute, rss.time_start_in_seconds, rss.start_date_in_seconds_epoch, 
            rss.end_date_in_seconds_epoch, rss.is_temporary as slot_is_temporary
        FROM running_turn_on_jobs rtoj
        LEFT JOIN resolved_schedule_slots rss ON rtoj.timeslot_id = rss.timeslot_id
        WHERE rtoj.timeslot_id = %s
        """
        
        cursor.execute(query, (timeslot_id,))
        result = cursor.fetchone()
        
        if result:
            # Create RunningTurnOnJob object
            running_job = RunningTurnOnJob(
                timeslot_id=result[0],
                is_temporary=result[1]
            )
            
            # Create ResolvedScheduleSlotV2 object if schedule slot exists
            schedule_slot = None
            if result[4] is not None:  # Check if schedule_id exists (not NULL)
                schedule_slot = ResolvedScheduleSlotV2(
                    timeslot_id=result[0],
                    schedule_id=result[4],  # Add missing schedule_id
                    room_id=result[5],
                    day_name=result[6],
                    day_order=result[7],
                    start_time=result[8],
                    end_time=result[9],
                    subject=result[10] or "",
                    teacher=result[11] or "",
                    teacher_email=result[12] or "",
                    start_hour=result[13],
                    start_minute=result[14],
                    end_hour=result[15],
                    end_minute=result[16],
                    time_start_in_seconds=result[17],
                    start_date_in_seconds_epoch=result[18],
                    end_date_in_seconds_epoch=result[19],
                    is_temporary=result[20] or False
                )
            
            print(f"✅ Found running job and schedule slot for timeslot_id: {timeslot_id}")
            return running_job, schedule_slot
        else:
            print(f"⚠️ No running turn on job found with timeslot_id: {timeslot_id}")
            return None, None
        
    except Exception as e:
        print(f"❌ Error getting running job with schedule slot: {e}")
        return None, None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_get_all_running_jobs_with_schedule_slots(conn_pool: pool.SimpleConnectionPool, is_temporary: Optional[bool] = None) -> List[Tuple[RunningTurnOnJob, Optional[ResolvedScheduleSlotV2]]]:
    """
    Get all RunningTurnOnJobs with their associated ResolvedScheduleSlotV2 objects
    
    Args:
        conn_pool: PostgreSQL connection pool
        is_temporary: Filter by temporary status (None for all)
        
    Returns:
        List of (RunningTurnOnJob, ResolvedScheduleSlotV2 or None) tuples
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        # Base JOIN query
        base_query = """
        SELECT 
            rtoj.timeslot_id, rtoj.is_temporary as job_is_temporary, rtoj.created_at, rtoj.updated_at,
            rss.schedule_id, rss.room_id, rss.day_name, rss.day_order, rss.start_time, rss.end_time,
            rss.subject, rss.teacher, rss.teacher_email, rss.start_hour, rss.start_minute, 
            rss.end_hour, rss.end_minute, rss.time_start_in_seconds, rss.start_date_in_seconds_epoch, 
            rss.end_date_in_seconds_epoch, rss.is_temporary as slot_is_temporary
        FROM running_turn_on_jobs rtoj
        LEFT JOIN resolved_schedule_slots rss ON rtoj.timeslot_id = rss.timeslot_id
        """
        
        if is_temporary is not None:
            query = base_query + " WHERE rtoj.is_temporary = %s ORDER BY rtoj.created_at DESC"
            cursor.execute(query, (is_temporary,))
        else:
            query = base_query + " ORDER BY rtoj.created_at DESC"
            cursor.execute(query)
        
        results = cursor.fetchall()
        
        job_slot_pairs = []
        for result in results:
            # Create RunningTurnOnJob object
            running_job = RunningTurnOnJob(
                timeslot_id=result[0],
                is_temporary=result[1]
            )
            
            # Create ResolvedScheduleSlotV2 object if schedule slot exists
            schedule_slot = None
            if result[4] is not None:  # Check if schedule_id exists (not NULL)
                schedule_slot = ResolvedScheduleSlotV2(
                    timeslot_id=result[0],
                    schedule_id=result[4],  # Add missing schedule_id
                    room_id=result[5],
                    day_name=result[6],
                    day_order=result[7],
                    start_time=result[8],
                    end_time=result[9],
                    subject=result[10] or "",
                    teacher=result[11] or "",
                    teacher_email=result[12] or "",
                    start_hour=result[13],
                    start_minute=result[14],
                    end_hour=result[15],
                    end_minute=result[16],
                    time_start_in_seconds=result[17],
                    start_date_in_seconds_epoch=result[18],
                    end_date_in_seconds_epoch=result[19],
                    is_temporary=result[20] or False
                )
            
            job_slot_pairs.append((running_job, schedule_slot))
        
        print(f"✅ Retrieved {len(job_slot_pairs)} running job-schedule slot pairs")
        return job_slot_pairs
        
    except Exception as e:
        print(f"❌ Error getting all running jobs with schedule slots: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_clear_all_running_turn_on_jobs(conn_pool: pool.SimpleConnectionPool) -> bool:
    """
    Clear all running turn on jobs from the table
    
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
        
        # Get count before deletion for reporting
        cursor.execute("SELECT COUNT(*) FROM running_turn_on_jobs")
        jobs_count = cursor.fetchone()[0]
        
        # Delete all jobs
        cursor.execute("DELETE FROM running_turn_on_jobs")
        
        conn.commit()
        
        print(f"✅ Cleared all running turn on jobs: {jobs_count} entries deleted")
        return True
        
    except Exception as e:
        print(f"❌ Error clearing running turn on jobs: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)