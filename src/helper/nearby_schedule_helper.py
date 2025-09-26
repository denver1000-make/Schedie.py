"""
Helper functions for nearby schedule detection using unified schedule system
Works with both regular and temporary schedules via resolved_schedule_slots table
"""

import datetime
from typing import Optional
from psycopg2 import pool
from src.constants import DEVICE_TZ


def get_next_schedule_start_time_in_room(
    pg_pool: pool.SimpleConnectionPool, 
    room_id: str, 
    current_time: datetime.datetime
) -> Optional[datetime.datetime]:
    """
    Get the next schedule start time in a specific room using unified table system
    Works with both regular schedules and temporary schedules from resolved_schedule_slots
    
    Args:
        pg_pool: PostgreSQL connection pool
        room_id: Room to check for upcoming schedules
        current_time: Current time to search from
        
    Returns:
        Next schedule start time or None if no upcoming schedule found
    """
    conn = None
    cursor = None
    try:
        conn = pg_pool.getconn()
        cursor = conn.cursor()
        
        current_epoch = current_time.timestamp()
        
        # Query to find the next schedule in the room using unified table structure
        # All schedules (both regular and temporary) are now in resolved_schedule_slots
        query = """
        SELECT MIN(rss.start_date_in_seconds_epoch) as next_start
        FROM resolved_schedule_slots rss
        WHERE rss.room_id = %s
        AND rss.start_date_in_seconds_epoch > %s
        AND rss.start_date_in_seconds_epoch IS NOT NULL
        """
        
        cursor.execute(query, (room_id, current_epoch))
        
        result = cursor.fetchone()
        
        if result and result[0]:
            next_epoch = float(result[0])
            next_datetime = datetime.datetime.fromtimestamp(next_epoch, tz=DEVICE_TZ)
            print(f"🔍 Found next schedule in {room_id} at: {next_datetime}")
            return next_datetime
        else:
            print(f"🔍 No upcoming schedules found in {room_id}")
            return None
            
    except Exception as e:
        print(f"❌ Error finding next schedule in room {room_id}: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            pg_pool.putconn(conn)


def should_skip_turn_off(
    pg_pool: pool.SimpleConnectionPool,
    room_id: str,
    skip_threshold_minutes: int = 5
) -> tuple[bool, Optional[datetime.datetime]]:
    """
    Determine if turn off should be skipped due to nearby schedule
    
    Args:
        pg_pool: PostgreSQL connection pool
        room_id: Room to check
        skip_threshold_minutes: Minutes threshold for skipping turn off
        
    Returns:
        Tuple of (should_skip, next_schedule_time)
    """
    current_time = datetime.datetime.now(DEVICE_TZ)
    next_schedule = get_next_schedule_start_time_in_room(pg_pool, room_id, current_time)
    
    if next_schedule:
        time_until_next = (next_schedule - current_time).total_seconds()
        
        # If next schedule starts within the threshold, skip turn off
        if 0 <= time_until_next <= (skip_threshold_minutes * 60):
            minutes_until = time_until_next / 60
            print(f"⏭️ Will skip turn OFF - next schedule in {room_id} starts in {minutes_until:.1f} minutes")
            return True, next_schedule
        else:
            minutes_until = time_until_next / 60
            print(f"✅ Will proceed with turn OFF - next schedule in {room_id} starts in {minutes_until:.1f} minutes (beyond {skip_threshold_minutes}min threshold)")
            return False, next_schedule
    else:
        print(f"✅ Will proceed with turn OFF - no upcoming schedules in {room_id}")
        return False, None