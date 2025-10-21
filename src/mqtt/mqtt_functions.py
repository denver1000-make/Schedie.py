
import datetime
import json
from typing import Optional, List
from src.mqtt.mqtt_manager import publish_v2
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from src.modelsV2.model import RunningTurnOnJob, ResolvedScheduleSlotV2
from src.sql.running_turn_on_job.running_turn_on_job_sql import pg_insert_running_turn_on_job, pg_remove_running_turn_on_job, pg_get_running_job_with_schedule_slot
from src.sql.resolved_timeslot.resolved_timeslot import pg_get_resolved_slots_by_schedule_id
from src.sql.schedule.schedule_wrapper_sql import pg_get_latest_schedule_wrapper
from src.sql.cancelled_schedules.cancelled_schedules_sql import (
    CancelledSchedule, 
    pg_insert_cancelled_schedule, 
    pg_is_schedule_cancelled,
    pg_remove_cancelled_schedule,
    pg_create_cancelled_schedules_table
)
from src.constants import JOB_TURN_ON_SUFFIX, JOB_TURN_OFF_SUFFIX, DEVICE_TZ, JobStatus
from src.utils.logging_config import (
    get_mqtt_logger, 
    log_mqtt_publish, 
    log_schedule_operation,
    log_database_operation,
    log_cancellation_operation
)
import paho.mqtt.client as mqtt
import psycopg2.pool as pg_pool

# Initialize logger for this module
logger = get_mqtt_logger()

# Import global constants
from src.constants import (
    MINUTE_MARK_TO_WARN, MINUTE_MARK_TO_SKIP, JOB_ONGOING, JOB_STANDBY,
    CANCELLATION_DELAY_THRESHOLD, CANCELLATION_DELAY_DURATION
)

# ======================================
# CENTRALIZED MQTT FUNCTIONS
# ======================================

def turn_on_mqtt(room_id: str, mqtt_client: mqtt.Client) -> bool:
    """
    Centralized function to send turn on MQTT message.
    
    Args:
        room_id: Room ID to turn on
        mqtt_client: MQTT client instance
        
    Returns:
        True if successful, False otherwise
    """
    try:
        print(f"[TURN_ON_MQTT] Room {room_id} turning on")
        
        publish_v2(
            client=mqtt_client,
            topic=f"turn_on/{room_id}",
            msg=room_id,
            log=True
        )
        
        print(f"[MQTT] Topic: turn_on/{room_id}")
        print(f"       Payload: False")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to send turn_on MQTT for Room {room_id}: {e}")
        return False


def turn_off_mqtt(room_id: str, mqtt_client: mqtt.Client) -> bool:
    """
    Centralized function to send turn off MQTT message.
    
    Args:
        room_id: Room ID to turn off
        mqtt_client: MQTT client instance
        
    Returns:
        True if successful, False otherwise
    """
    try:
        print(f"[TURN_OFF_MQTT] Room {room_id} turning off")
        
        publish_v2(
            client=mqtt_client,
            topic=f"turn_off/{room_id}",
            msg=room_id,
            log=True
        )
        
        print(f"[MQTT] Topic: turn_off/{room_id}")
        print(f"       Payload: False")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to send turn_off MQTT for Room {room_id}: {e}")
        return False


def send_room_shutdown_warning_mqtt(
    room_id: str,
    subject: str,
    reason: str,
    mqtt_client: mqtt.Client,
    **kwargs
) -> bool:
    """
    Centralized function to send room shutdown warning MQTT messages.
    Uses topic: room_shutdown_warning/{room_id}
    Sends simplified payload (timeslot_id only) for ESP32 compatibility.
    
    Only triggered in two scenarios:
    1. When a running schedule is cancelled
    2. When a schedule is about to end with no nearby schedules
    
    Args:
        room_id: Room ID
        subject: Schedule subject
        reason: Reason for shutdown ("cancellation" or "schedule_ending")
        mqtt_client: MQTT client instance
        **kwargs: Additional fields (timeslot_id will be extracted if available)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Send only timeslot_id as payload for ESP32 compatibility
        # Extract timeslot_id from kwargs if available, otherwise use room_id
        payload = kwargs.get('timeslot_id', room_id)
        
        topic = f"room_shutdown_warning/{room_id}"
        
        # Use direct publish instead of publish_v2 for simple string payload
        mqtt_client.publish(
            topic=topic,
            payload=payload,
            qos=2,
            retain=False
        )
        
        print(f"[MQTT] Room shutdown warning sent to {topic}: {payload}")
        log_mqtt_publish(logger, topic, str(payload), True)
        
        return True
        
    except Exception as e:
        logger.error("Failed to send room shutdown warning: %s", str(e))
        return False


def send_heartbeat_mqtt(mqtt_client: mqtt.Client) -> bool:
    """
    Centralized function to send heartbeat MQTT message.
    
    Args:
        mqtt_client: MQTT client instance
        
    Returns:
        True if successful, False otherwise
    """
    try:
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        mqtt_client.publish(
            "hub_heartbeat", 
            qos=2, 
            payload=timestamp
        )
        
        print(f"[HEARTBEAT_MQTT] Sent heartbeat: {timestamp}")
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to send heartbeat MQTT: {e}")
        return False


def send_acknowledgment_mqtt(
    topic: str,
    payload: dict,
    mqtt_client: mqtt.Client
) -> bool:
    """
    Centralized function to send acknowledgment MQTT messages.
    
    Args:
        topic: MQTT topic to publish to
        payload: Acknowledgment payload dictionary
        mqtt_client: MQTT client instance
        
    Returns:
        True if successful, False otherwise
    """
    try:
        print(f"[ACK_MQTT] Publishing acknowledgment to {topic}")
        
        publish_v2(
            client=mqtt_client,
            topic=topic,
            msg=json.dumps(payload),
            log=True
        )
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to send acknowledgment MQTT: {e}")
        return False

def update_room_status(
    room_id: str,
    status: int,
    timeslot_id: str,
    subject: str, 
    mqtt_client: mqtt.Client
):
    mqtt_client.publish(
        payload=status,
        qos=2, 
        retain=True,
        topic=f"timeslots/{timeslot_id}/status"
    )
    

def update_timeslot_status(
    status: int,
    timeslot_id: str,
    subject: str, 
    mqtt_client: mqtt.Client,
    is_temp: bool
):
    payload = {
        "status": status,
        "isTemp": is_temp
    }
    
    mqtt_client.publish(
        payload=json.dumps(payload),
        qos=2, 
        retain=True,
        topic=f"timeslots/{timeslot_id}/state"
    )
    
    
    

def send_schedule_ending_mqtt(
    room_id: str,
    timeslot_id: str,
    subject: str,
    mqtt_client: mqtt.Client,
    **kwargs
) -> bool:
    """
    Centralized function to send schedule ending notification when room stays on.
    Uses topic: room_schedule_ending/{room_id}
    
    This is sent when a schedule ends but the room will stay on due to an upcoming schedule.
    
    Args:
        room_id: Room ID
        timeslot_id: Timeslot ID that is ending
        subject: Schedule subject that is ending
        mqtt_client: MQTT client instance
        **kwargs: Additional fields for the ending message
        
    Returns:
        True if successful, False otherwise
    """
    try:
        ending_payload = {
            "room_id": room_id,
            "timeslot_id": timeslot_id,
            "subject": subject,
            **kwargs
        }
        
        topic = f"room_schedule_ending/{room_id}"
        
        print(f"[SCHEDULE_ENDING] Room {room_id} schedule ending (staying on)")
        
        publish_v2(
            client=mqtt_client,
            topic=topic,
            msg=json.dumps(ending_payload),
            log=True
        )
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to send schedule ending MQTT: {e}")
        return False


def send_skipped_turn_off_mqtt(
    room_id: str,
    subject: str,
    mqtt_client: mqtt.Client,
    teacher_name: Optional[str] = None,
    **kwargs
) -> bool:
    """
    Centralized function to send skipped turn off notification.
    Uses topic: skipped_turn_off/{room_id}
    
    This is sent when turn off is skipped due to nearby schedules.
    
    Args:
        room_id: Room ID
        subject: Schedule subject
        teacher_name: Teacher name (optional)
        mqtt_client: MQTT client instance
        **kwargs: Additional fields for the message
        
    Returns:
        True if successful, False otherwise
    """
    try:
        skip_payload = {
            "room_id": room_id,
            "subject": subject,
            **kwargs
        }
        
        if teacher_name:
            skip_payload["teacher_name"] = teacher_name
        
        topic = f"skipped_turn_off/{room_id}"
        
        print(f"[SKIPPED_TURN_OFF] Room {room_id} turn off skipped")
        
        publish_v2(
            client=mqtt_client,
            topic=topic,
            msg=json.dumps(skip_payload),
            log=True
        )
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to send skipped turn off MQTT: {e}")
        return False


def send_finished_cancellation_mqtt(
    cancellation_id: str,
    timeslot_id: str,
    room_id: str,
    subject: str,
    mqtt_client: mqtt.Client,
    cancellation_type: str = "completed",
    **kwargs
) -> bool:
    """
    Centralized function to send finished cancellation notification.
    Uses topic: finished_cancellations/{cancellation_id}
    
    This is sent when a cancelled schedule has been fully processed and cleaned up.
    
    Args:
        cancellation_id: Unique cancellation ID for cleanup tracking
        timeslot_id: Timeslot ID that was cancelled
        room_id: Room ID
        subject: Schedule subject
        mqtt_client: MQTT client instance
        cancellation_type: Type of cancellation completion ("completed", "expired", "cleaned_up")
        **kwargs: Additional fields for the message
        
    Returns:
        True if successful, False otherwise
    """
    try:
        finished_payload = {
            "timeslot_id": timeslot_id,
            "room_id": room_id,
            "subject": subject,
            "cancellation_type": cancellation_type,
            "timestamp": datetime.datetime.now(DEVICE_TZ).isoformat(),
            **kwargs
        }
        
        topic = f"finished_cancellations/{cancellation_id}"
        
        print(f"[FINISHED_CANCELLATION] Cancellation {cancellation_id} completed for {room_id} - {subject}")
        
        publish_v2(
            client=mqtt_client,
            topic=topic,
            msg=json.dumps(finished_payload),
            log=True
        )
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to send finished cancellation MQTT: {e}")
        return False

def turn_on(
        room_id: str,
        job_turn_off_id: str,
        job_turn_on_id: str,
        mqtt_client: mqtt.Client,
        pg_pool: pg_pool.SimpleConnectionPool
):
    # Extract timeslot_id from job_turn_on_id (remove suffix)
    timeslot_id = None
    if job_turn_on_id.endswith(JOB_TURN_ON_SUFFIX):
        timeslot_id = job_turn_on_id[:-len(JOB_TURN_ON_SUFFIX)]
        
        # Check if there's a valid running job - this is the core control mechanism
        if timeslot_id and pg_pool:
            try:
                running_job, schedule_slot = pg_get_running_job_with_schedule_slot(pg_pool, timeslot_id)
                if not running_job:
                    print(f"[NO_RUNNING_JOB] Turn on skipped for Room {room_id} - no running job found")
                    return
                else:
                    print(f"[RUNNING_JOB_FOUND] Turn on proceeding for Room {room_id} - running job exists")
            except Exception as e:
                print(f"[ERROR] Failed to check running job for {timeslot_id}: {e}")
                return
    
    # Publish MQTT turn_on message using centralized function
    turn_on_mqtt(room_id, mqtt_client)
    

    # Extract timeslot_id from job_turn_on_id (remove suffix) for database logging
    if job_turn_on_id.endswith(JOB_TURN_ON_SUFFIX):
        timeslot_id = job_turn_on_id[:-len(JOB_TURN_ON_SUFFIX)]
        
        # Update room status to JOB_ONGOING
        try:
            # Get schedule info for the status update
            running_job, schedule_slot = pg_get_running_job_with_schedule_slot(pg_pool, timeslot_id)
            subject: str = schedule_slot.subject if schedule_slot else "Unknown Meeting"
            
            update_room_status(
                room_id=room_id,
                status=JOB_ONGOING,
                timeslot_id=timeslot_id,
                subject=subject,
                mqtt_client=mqtt_client
            )
            update_timeslot_status(
                mqtt_client=mqtt_client,
                status=JobStatus.JOB_ONGOING,
                subject=subject,
                timeslot_id=timeslot_id,
                is_temp=False
            )
            
            print(f"[STATUS] Updated room status to JOB_ONGOING")
        except Exception as e:
            print(f"[ERROR] Failed to update room status: {e}")
        
        # Log the running job in database for tracking
        running_job = RunningTurnOnJob(
            timeslot_id=timeslot_id,
            is_temporary=False  # Regular schedules are not temporary
        )
        
        try:
            success = pg_insert_running_turn_on_job(pg_pool, running_job)
            if success:
                print(f"[DATABASE] Inserted running job:")
                print(f"           Timeslot: {timeslot_id}")
                print(f"           Temporary: False")
            else:
                print(f"[ERROR] Failed to insert running job: {timeslot_id}")
        except Exception as e:
            print(f"[ERROR] Database insert failed for {timeslot_id}: {e}")


def get_next_schedule_in_room_with_details(
    pg_conn_pool: pg_pool.SimpleConnectionPool,
    room_id: str,
    current_end_hour: int,
    current_end_minute: int,
    day_name: str
) -> Optional[tuple[int, str, bool]]:
    """
    Smart proximity check that includes both permanent and temporary schedules.
    Returns next schedule info AND whether it's cancelled for smart skip decisions.
    
    Args:
        pg_conn_pool: Database connection pool
        room_id: Room ID to check
        current_end_hour: Current schedule end hour (24-hour format)  
        current_end_minute: Current schedule end minute
        day_name: Current day name
        
    Returns:
        Tuple of (minutes_until_next, timeslot_id, is_cancelled) or None if no schedule found
    """
    conn = None
    cursor = None
    try:
        # Create current time for comparison
        from src.constants import DEVICE_TZ
        import datetime
        
        today = datetime.date.today()
        
        conn = pg_conn_pool.getconn()
        cursor = conn.cursor()
        
        # All schedules (both permanent and temporary) use start_hour/start_minute
        # Temporary schedules also have start_date_in_seconds_epoch for specific dates
        
        current_end_time_seconds = current_end_hour * 60 + current_end_minute
        
        # Check schedules later today in the same room
        # Convert MINUTE_MARK_TO_SKIP to minutes for range check
        time_end_min_ranged = current_end_time_seconds + MINUTE_MARK_TO_SKIP
        
        logger.info(f"{current_end_time_seconds} Lower Bound")
        logger.info(f"{time_end_min_ranged} Upper Bound")
        
        today_query = """
        SELECT 
            rss.start_hour,
            rss.start_minute,
            rss.timeslot_id,
            rss.start_date_in_seconds_epoch,
            CASE WHEN rss.start_date_in_seconds_epoch IS NULL THEN 'permanent' ELSE 'temporary' END as schedule_type
        FROM resolved_schedule_slots rss
        WHERE rss.room_id = %s 
        AND rss.day_name = %s
        AND (rss.start_hour * 60 + rss.start_minute) >= %s
        AND (rss.start_hour * 60 + rss.start_minute) <= %s
        ORDER BY (rss.start_hour * 60 + rss.start_minute) ASC
        """
        
        cursor.execute(today_query, (
                room_id, 
                day_name, 
                current_end_time_seconds,
                time_end_min_ranged
            )
        )
        today_results = cursor.fetchall()
        
        # Process all candidates
        candidates = []
        current_date = today
        
        # Process today's schedules
        for row in today_results:
            start_hour, start_minute, timeslot_id, start_epoch, schedule_type = row
            
            # For temporary schedules, check if they're still valid (not expired)
            if schedule_type == 'temporary' and start_epoch:
                temp_date = datetime.datetime.fromtimestamp(float(start_epoch), tz=DEVICE_TZ).date()
                if temp_date != current_date:
                    continue  # Skip if not for today
            
            start_time_seconds = start_hour * 60 + start_minute
            minutes_gap = start_time_seconds - current_end_time_seconds
            
            if minutes_gap >= 0:  # Include schedules that start exactly when current ends
                candidates.append((minutes_gap, timeslot_id, f"{schedule_type}_today"))
        
        if candidates:
            # Sort by time gap and return the nearest
            candidates.sort(key=lambda x: x[0])
            minutes_gap, timeslot_id, schedule_type = candidates[0]
            
            print(f"✅ Found next schedule in {room_id}: {timeslot_id} in {minutes_gap} minutes ({schedule_type})")
            
            # Check if next schedule is cancelled
            from src.sql.cancelled_schedules.cancelled_schedules_sql import pg_is_schedule_cancelled
            
            is_next_cancelled = pg_is_schedule_cancelled(
                conn_pool=pg_conn_pool,
                timeslot_id=timeslot_id,
                execution_date="all"
            )
            
            return (minutes_gap, timeslot_id, is_next_cancelled)
        else:
            print(f"❌ No upcoming schedule found in {room_id}")
            return None
            
    except Exception as e:
        logger.error(f"Error checking next schedule details: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            pg_conn_pool.putconn(conn)

def check_skip_and_send_warning(
    pg_conn_pool: pg_pool.SimpleConnectionPool,
    room_id: str,
    current_end_hour: int,
    current_end_minute: int,
    day_name: str,
    subject: str,
    mqtt_client: mqtt.Client,
    timeslot_id: Optional[str] = None
) -> bool:
    """
    Check if turn_off should be skipped and send appropriate warning message via MQTT.
    
    Enhanced to account for cancellation: If the nearest schedule is cancelled,
    DO NOT skip the turn_off (proceed with turning room off).
    
    Args:
        pg_conn_pool: Database connection pool
        room_id: Room ID to check
        current_end_hour: Current schedule end hour (24-hour format)
        current_end_minute: Current schedule end minute
        day_name: Current day name
        subject: Schedule subject for warning message
        mqtt_client: MQTT client for sending messages
        
    Returns:
        True if turn_off should be skipped, False if should proceed
    """
    try:
        # Use global hardcoded setting instead of database lookup
        minute_mark_to_skip = MINUTE_MARK_TO_SKIP
        logger.debug(f"minute_mark_to_skip setting: {minute_mark_to_skip} minutes (hardcoded)")
        
        # Find next schedule in the same room
        next_schedule_info = get_next_schedule_in_room_with_details(
            pg_conn_pool, room_id, current_end_hour, current_end_minute, day_name
        )
        
        if next_schedule_info is None:
            # No upcoming schedule - send shutdown warning (room closing, no nearby schedules)
            print(f"[SCHEDULE_ENDING] Room {room_id} ({subject}) closing at {current_end_hour:02d}:{current_end_minute:02d}")
            print(f"                   No nearby schedules found")
            
            send_room_shutdown_warning_mqtt(
                room_id,
                subject,
                "schedule_ending",
                mqtt_client,
                end_time=f"{current_end_hour:02d}:{current_end_minute:02d}",
                message=f"Room {room_id} ({subject}) will close at {current_end_hour:02d}:{current_end_minute:02d}"
            )
            
            return False
        
        minutes_until_next, next_timeslot_id, is_next_cancelled = next_schedule_info
        
        # The cancellation status is already checked in get_next_schedule_in_room_with_details
        
        if is_next_cancelled:
            # Next schedule is cancelled - send shutdown warning (treat as no nearby schedule)
            print(f"[SCHEDULE_ENDING] Room {room_id} ({subject}) closing at {current_end_hour:02d}:{current_end_minute:02d}")
            print(f"                   Next schedule ({next_timeslot_id}) in {minutes_until_next} min is CANCELLED")
            
            send_room_shutdown_warning_mqtt(
                room_id,
                subject,
                "schedule_ending",
                mqtt_client,
                end_time=f"{current_end_hour:02d}:{current_end_minute:02d}",
                next_schedule_in=minutes_until_next,
                next_schedule_cancelled=True,
                message=f"Room {room_id} ({subject}) will close - next schedule is cancelled"
            )
            
            return False
        
        # Next schedule is NOT cancelled - apply normal proximity logic
        if minutes_until_next <= minute_mark_to_skip:
            # Will skip turn_off - send schedule ending message (room staying on due to nearby schedule)
            print(f"[SCHEDULE_SKIP] Room {room_id} ({subject}) staying ON")
            print(f"                 Next schedule ({next_timeslot_id}) in {minutes_until_next} minutes")
            print(f"                 Turn off will be SKIPPED - sending schedule ending notification")
            
            # Send schedule ending notification (NEW TOPIC - room_schedule_ending)
            send_schedule_ending_mqtt(
                room_id=room_id,
                timeslot_id=timeslot_id or "unknown",  # Use actual timeslot_id when available
                subject=subject,
                mqtt_client=mqtt_client,
                end_time=f"{current_end_hour:02d}:{current_end_minute:02d}",
                minutes_until_next=minutes_until_next,
                next_timeslot_id=next_timeslot_id,
                message=f"Schedule '{subject}' ending but room staying on - next schedule in {minutes_until_next} min"
            )
            
            return True
        else:
            # Will proceed with turn_off - send shutdown warning (far enough away)
            print(f"[SCHEDULE_ENDING] Room {room_id} ({subject}) closing at {current_end_hour:02d}:{current_end_minute:02d}")
            print(f"                   Next schedule ({next_timeslot_id}) in {minutes_until_next} min (too far)")
            
            send_room_shutdown_warning_mqtt(
                room_id,
                subject,
                "schedule_ending",
                mqtt_client,
                end_time=f"{current_end_hour:02d}:{current_end_minute:02d}",
                next_schedule_in=minutes_until_next,
                next_timeslot_id=next_timeslot_id,
                message=f"Room {room_id} ({subject}) will close - next schedule in {minutes_until_next} min"
            )
            
            return False
            
    except Exception as e:
        print(f"[ERROR] Skip check failed: {e}")
        print(f"        Proceeding with turn_off")
        
        # On error, send shutdown warning and proceed
        try:
            send_room_shutdown_warning_mqtt(
                room_id,
                subject,
                "schedule_ending",
                mqtt_client,
                end_time=f"{current_end_hour:02d}:{current_end_minute:02d}",
                message=f"Room {room_id} ({subject}) will close - error in schedule check"
            )
            
        except:
            pass
        return False

def _check_skip_only(
    pg_conn_pool: pg_pool.SimpleConnectionPool,
    room_id: str,
    current_end_hour: int,
    current_end_minute: int,
    day_name: str
) -> bool:
    """
    Check if turn_off should be skipped - NO WARNING MESSAGES.
    This is used by turn_off function to avoid duplicate warnings.
    """
    try:
        minute_mark_to_skip = MINUTE_MARK_TO_SKIP
        print(f"[SKIP_CHECK] Using skip threshold: {minute_mark_to_skip} minutes")
        
        # Find next schedule in the same room
        next_schedule_info = get_next_schedule_in_room_with_details(
            pg_conn_pool, room_id, current_end_hour, current_end_minute, day_name
        )
        
        if next_schedule_info is None:
            print(f"[SKIP_CHECK] No upcoming schedule - proceeding with turn off")
            return False
        
        minutes_until_next, next_timeslot_id, is_next_cancelled = next_schedule_info
        
        # The cancellation status is already checked in get_next_schedule_in_room_with_details
        
        if is_next_cancelled:
            print(f"[SKIP_CHECK] Next schedule is cancelled - proceeding with turn off")
            return False
        
        # Apply skip logic
        if minutes_until_next <= minute_mark_to_skip:
            print(f"[SKIP_CHECK] Next schedule in {minutes_until_next} min (<= {minute_mark_to_skip} min) - SKIPPING turn off")
            return True
        else:
            print(f"[SKIP_CHECK] Next schedule in {minutes_until_next} min (> {minute_mark_to_skip} min) - proceeding with turn off")
            return False
            
    except Exception as e:
        print(f"[ERROR] Skip check failed: {e} - proceeding with turn off")
        return False


def turn_off(
        room_id: str,
        job_turn_off_id: str,
        job_turn_on_id: str,
        isTemp: bool,
        mqtt_client: mqtt.Client,
        pg_conn_pool: pg_pool.SimpleConnectionPool
):
    """
    Smart turn_off function that checks for nearby schedules before turning off.
    Does NOT send warning messages - only the warning job does that.
    """
    # Extract timeslot_id to get schedule info for smart skip logic
    timeslot_id = None
    if job_turn_off_id.endswith(JOB_TURN_OFF_SUFFIX):
        timeslot_id = job_turn_off_id[:-len(JOB_TURN_OFF_SUFFIX)]
    
    # Core control mechanism: Check if there's a running job - if not, skip turn_off entirely
    if timeslot_id and pg_conn_pool:
        try:
            running_job, schedule_slot = pg_get_running_job_with_schedule_slot(pg_conn_pool, timeslot_id)
            if not running_job:
                print(f"[NO_RUNNING_JOB] Turn off skipped for Room {room_id} - no running job found")
                
                # Check if this was a cancelled schedule and clean up the cancellation record
                _cleanup_cancellation_if_exists(timeslot_id, pg_conn_pool, mqtt_client, isTemp)
                return
            else:
                print(f"[RUNNING_JOB_FOUND] Turn off proceeding for Room {room_id} - running job exists")
                
                # Clean up any cancellation records for this completed schedule
                _cleanup_cancellation_if_exists(timeslot_id, pg_conn_pool, mqtt_client, isTemp)
                
        except Exception as e:
            print(f"[ERROR] Failed to check running job for {timeslot_id}: {e}")
            return
    
    # Get the current schedule info for smart skip logic
    should_skip = False
    if timeslot_id and pg_conn_pool and not isTemp:  # Only do smart skip for permanent schedules
        try:
            # Get the running job and its associated schedule slot
            running_job, schedule_slot = pg_get_running_job_with_schedule_slot(pg_conn_pool, timeslot_id)
            
            if schedule_slot:
                print(f"[TURN_OFF] Room {room_id} schedule ending")
                print(f"           Subject: {schedule_slot.subject}")
                print(f"           End Time: {schedule_slot.day_name} {schedule_slot.end_hour:02d}:{schedule_slot.end_minute:02d}")
                
                # Check if we should skip turn_off based on nearby schedules (NO WARNING MESSAGES)
                should_skip = _check_skip_only(
                    pg_conn_pool,
                    room_id,
                    schedule_slot.end_hour,
                    schedule_slot.end_minute,
                    schedule_slot.day_name
                )
            else:
                print(f"[ERROR] Could not find schedule slot for timeslot: {timeslot_id}")
            
        except Exception as e:
            print(f"[ERROR] Smart skip logic failed: {e}")
    
    # Execute based on skip decision
    if should_skip:
        print(f"[SKIP] Turn off skipped for Room {room_id}")
        print(f"       Nearby schedule detected - room will stay on")
        
        # Send skipped turn off notification with subject and teacher info
        if timeslot_id and pg_conn_pool:
            try:
                running_job, schedule_slot = pg_get_running_job_with_schedule_slot(pg_conn_pool, timeslot_id)
                if schedule_slot:
                    send_skipped_turn_off_mqtt(
                        room_id=room_id,
                        subject=schedule_slot.subject or "Unknown Meeting",
                        mqtt_client=mqtt_client,
                        teacher_name=getattr(schedule_slot, 'teacher_name', None)
                    )
                else:
                    # Fallback if no schedule slot found
                    send_skipped_turn_off_mqtt(
                        room_id=room_id,
                        subject="Unknown Meeting",
                        mqtt_client=mqtt_client
                    )
            except Exception as e:
                print(f"[ERROR] Failed to send skip notification: {e}")
    else:
        # Normal turn off - use centralized MQTT function (NO WARNING MESSAGES)
        print(f"[TURN_OFF] Proceeding with turn off for Room {room_id}")
        turn_off_mqtt(room_id, mqtt_client)
    
    # Update room status to JOB_STANDBY (whether we turned off or skipped)
    if timeslot_id and pg_conn_pool:
        try:
            # Get schedule info for the status update
            running_job, schedule_slot = pg_get_running_job_with_schedule_slot(pg_conn_pool, timeslot_id)
            subject = schedule_slot.subject if schedule_slot else "Unknown Meeting"
            
            update_room_status(
                room_id=room_id,
                status=JOB_STANDBY,
                timeslot_id=timeslot_id,
                subject=subject,
                mqtt_client=mqtt_client
            )
            
            if isTemp:    
                update_timeslot_status(
                    status=JobStatus.JOB_REMOVE,
                    timeslot_id=timeslot_id,
                    subject=subject,
                    mqtt_client=mqtt_client,
                    is_temp=True
                )
            else:
                update_timeslot_status(
                    status=JobStatus.JOB_STANDBY,
                    timeslot_id=timeslot_id,
                    subject=subject,
                    mqtt_client=mqtt_client,
                    is_temp=False
                )
            
            status_action = "skipped" if should_skip else "turned_off"
            print(f"[STATUS] Updated room status to JOB_STANDBY ({status_action})")
        except Exception as e:
            print(f"[ERROR] Failed to update room status: {e}")
    
    # Always remove the running job record (whether we turned off or skipped)
    if timeslot_id and pg_conn_pool:
        try:
            success = pg_remove_running_turn_on_job(pg_conn_pool, timeslot_id)
            status = "skipped" if should_skip else "completed"
            
            if success:
                print(f"[DATABASE] Removed running job: {timeslot_id}")
                print(f"           Status: {status}")
            else:
                print(f"[ERROR] Failed to remove running job: {timeslot_id}")
                
        except Exception as e:
            print(f"[ERROR] Database removal failed for {timeslot_id}: {e}")
    
    if isTemp:
        print(f"[TEMPORARY] Job completed and removed")
        
        # Clean up temporary schedule from database when it finishes
        if timeslot_id and pg_conn_pool:
            try:
                _cleanup_finished_temporary_schedule(timeslot_id, pg_conn_pool, mqtt_client)
            except Exception as e:
                print(f"[ERROR] Failed to cleanup temporary schedule {timeslot_id}: {e}")


def _cleanup_finished_temporary_schedule(
    timeslot_id: str, 
    pg_conn_pool: pg_pool.SimpleConnectionPool,
    mqtt_client: Optional[mqtt.Client] = None
):
    """
    Clean up a finished temporary schedule from the database.
    Removes both the schedule wrapper and resolved schedule slots.
    Sends finished cancellation notification if it was cancelled.
    """
    try:
        # Find the schedule_id for this timeslot
        from src.sql.resolved_timeslot.resolved_timeslot import pg_get_resolved_slots_by_schedule_id
        from src.sql.schedule.schedule_wrapper_sql import pg_get_all_schedule_wrappers, pg_delete_schedule_wrapper
        
        # Get all temporary schedule wrappers to find the one containing this timeslot
        temp_wrappers = pg_get_all_schedule_wrappers(pg_conn_pool, is_temporary=True)
        
        target_schedule_id = None
        schedule_slot = None
        for wrapper in temp_wrappers:
            slots = pg_get_resolved_slots_by_schedule_id(pg_conn_pool, wrapper.schedule_id)
            for slot in slots:
                if slot.timeslot_id == timeslot_id:
                    target_schedule_id = wrapper.schedule_id
                    schedule_slot = slot
                    break
            if target_schedule_id:
                break
        
        if target_schedule_id:
            # Check if this was a cancelled temporary schedule and send finished notification
            if mqtt_client and schedule_slot:
                is_cancelled = pg_is_schedule_cancelled(pg_conn_pool, timeslot_id, "all")
                if is_cancelled:
                    # Clean up cancellation record and send notification
                    pg_remove_cancelled_schedule(pg_conn_pool, timeslot_id, "")
                    
                    # Send finished cancellation notification for temp schedule
                    cancellation_id = f"{timeslot_id}_temp_{int(datetime.datetime.now().timestamp())}"
                    send_finished_cancellation_mqtt(
                        cancellation_id=cancellation_id,
                        timeslot_id=timeslot_id,
                        room_id=schedule_slot.room_id,
                        subject=schedule_slot.subject,
                        mqtt_client=mqtt_client,
                        cancellation_type="expired_temp"
                    )
            
            # Delete the schedule wrapper (CASCADE will remove associated slots)
            success = pg_delete_schedule_wrapper(pg_conn_pool, target_schedule_id)
            if success:
                print(f"[CLEANUP] Removed temporary schedule {target_schedule_id} from database")
            else:
                print(f"[ERROR] Failed to remove temporary schedule {target_schedule_id}")
        else:
            print(f"[WARNING] Could not find temporary schedule for timeslot {timeslot_id}")
            
    except Exception as e:
        print(f"[ERROR] Error cleaning up temporary schedule for {timeslot_id}: {e}")


def _cleanup_cancellation_if_exists(
    timeslot_id: str, 
    pg_conn_pool: pg_pool.SimpleConnectionPool, 
    mqtt_client: mqtt.Client,
    is_temp: bool = False
) -> bool:
    """
    Helper function to clean up cancellation records if they exist.
    This prevents cancelled status from persisting after schedule completion.
    
    Args:
        timeslot_id: Timeslot ID to check
        pg_conn_pool: Database connection pool
        mqtt_client: MQTT client for status updates
        is_temp: Whether this is a temporary schedule
        
    Returns:
        True if cancellation was found and cleaned up, False otherwise
    """
    try:
        today = datetime.datetime.now(DEVICE_TZ).strftime('%Y-%m-%d')
        
        # Check if there's a cancellation record
        is_cancelled = pg_is_schedule_cancelled(pg_conn_pool, timeslot_id, "all")
        
        if is_cancelled:
            # Get schedule info for the cancellation notification
            try:
                running_job, schedule_slot = pg_get_running_job_with_schedule_slot(pg_conn_pool, timeslot_id)
                room_id = schedule_slot.room_id if schedule_slot else "unknown"
                subject = schedule_slot.subject if schedule_slot else "Unknown Schedule"
            except:
                room_id = "unknown"
                subject = "Unknown Schedule"
            
            # Clean up the cancellation record
            cleanup_success = pg_remove_cancelled_schedule(pg_conn_pool, timeslot_id, "")
            if cleanup_success:
                print(f"[CANCELLATION_CLEANUP] Removed cancellation record for {timeslot_id}")
                
                # Send finished cancellation notification
                cancellation_id = f"{timeslot_id}_{today}_{int(datetime.datetime.now().timestamp())}"
                send_finished_cancellation_mqtt(
                    cancellation_id=cancellation_id,
                    timeslot_id=timeslot_id,
                    room_id=room_id,
                    subject=subject,
                    mqtt_client=mqtt_client,
                    cancellation_type="completed"
                )
                
                # Update status: REMOVE for temp schedules, STANDBY for permanent
                if is_temp:
                    update_timeslot_status(
                        mqtt_client=mqtt_client,
                        status=JobStatus.JOB_REMOVE,
                        subject=subject,
                        timeslot_id=timeslot_id,
                        is_temp=True
                    )
                else:
                    update_timeslot_status(
                        mqtt_client=mqtt_client,
                        status=JobStatus.JOB_STANDBY,
                        subject=subject,
                        timeslot_id=timeslot_id,
                        is_temp=False
                    )
                return True
            else:
                print(f"[ERROR] Failed to remove cancellation record for {timeslot_id}")
        
        return False
        
    except Exception as e:
        print(f"[ERROR] Error during cancellation cleanup for {timeslot_id}: {e}")
        return False


def send_heartbeat(mqtt_client: mqtt.Client):
    """Legacy function - use send_heartbeat_mqtt() instead"""
    send_heartbeat_mqtt(mqtt_client)


def _execute_delayed_cancellation_turnoff(
    room_id: str,
    job_turn_off_id: str,
    job_turn_on_id: str,
    isTemp: bool,
    mqtt_client: mqtt.Client,
    pg_conn_pool: pg_pool.SimpleConnectionPool,
    timeslot_id: str
):
    """
    Execute the delayed turn off after cancellation warning period.
    This is called by APScheduler after the warning delay.
    No additional warnings - the warning was already sent during cancellation.
    """
    print(f"[DELAYED_TURNOFF] Executing delayed turn off for Room {room_id} after cancellation")
    
    try:
        # Use the normal turn_off function to ensure consistent behavior
        # This will handle all status updates and cleanup properly
        print(f"[DELAYED_TURNOFF] Using normal turn_off function for consistent cleanup")
        
        turn_off(
            room_id=room_id,
            job_turn_off_id=job_turn_off_id,
            job_turn_on_id=job_turn_on_id,
            isTemp=isTemp,
            mqtt_client=mqtt_client,
            pg_conn_pool=pg_conn_pool
        )
        
        print(f"[SUCCESS] Delayed turn off completed for Room {room_id} using normal turn_off mechanism")
        
    except Exception as e:
        print(f"[ERROR] Failed to execute delayed turn off for Room {room_id}: {e}")
def cancel_schedule(
    timeslot_id: str,
    pg_conn_pool: pg_pool.SimpleConnectionPool,
    mqtt_client: mqtt.Client,
    subject: str,
    scheduler: Optional[BackgroundScheduler] = None,
    reason: str = "User requested cancellation",
    cancelled_by: str = "system"
) -> bool:
    """
    Cancel a schedule - handles both running and future schedules, permanent and temporary.
    
    Args:
        timeslot_id: Timeslot ID to cancel
        pg_conn_pool: Database connection pool  
        mqtt_client: MQTT client for notifications
        scheduler: Optional APScheduler instance for delayed operations
        reason: Reason for cancellation
        cancelled_by: Who cancelled it
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Processing cancellation request for timeslot: {timeslot_id}")
        
        # First check if this is currently a running schedule
        running_job, schedule_slot = pg_get_running_job_with_schedule_slot(pg_conn_pool, timeslot_id)
        
        if running_job and schedule_slot:
            # This is a currently running schedule
            print(f"📍 Found running schedule: {schedule_slot.subject} in {schedule_slot.room_id}")
            return _cancel_running_schedule(
                timeslot_id, schedule_slot, pg_conn_pool, mqtt_client, scheduler, reason, cancelled_by
            )
        else:
            # This is a future schedule - need to find it and queue cancellation
            print(f"📅 Schedule not currently running - checking for future schedule")
            return _cancel_future_schedule(
                timeslot_id, pg_conn_pool, mqtt_client, reason, cancelled_by
            )
            
    except Exception as e:
        logger.error(f"Error cancelling schedule {timeslot_id}: {e}")
        return False


def _cancel_running_schedule(
    timeslot_id: str,
    schedule_slot: ResolvedScheduleSlotV2,
    pg_conn_pool: pg_pool.SimpleConnectionPool,
    mqtt_client: mqtt.Client,
    scheduler: Optional[BackgroundScheduler],
    reason: str,
    cancelled_by: str
) -> bool:
    """
    Cancel a currently running schedule with intelligent timing.
    
    Logic:
    - If schedule ends in > CANCELLATION_DELAY_THRESHOLD minutes: Give warning, then turn off after CANCELLATION_DELAY_DURATION
    - If schedule ends in ≤ CANCELLATION_DELAY_THRESHOLD minutes: Turn off immediately  
    """
    try:
        # Calculate time until schedule ends
        current_time = datetime.datetime.now(tz=DEVICE_TZ)
        
        if schedule_slot.is_temporary and schedule_slot.end_date_in_seconds_epoch:
            # Temporary schedule - use epoch time with timezone
            end_time = datetime.datetime.fromtimestamp(
                schedule_slot.end_date_in_seconds_epoch, 
                tz=DEVICE_TZ
            )
        else:
            # Regular schedule - calculate end time for today with timezone
            today = current_time.date()
            end_time = datetime.datetime.combine(
                today, 
                datetime.time(schedule_slot.end_hour, schedule_slot.end_minute),
                tzinfo=DEVICE_TZ
            )
            
            # If end time is before current time, it might be tomorrow
            if end_time < current_time:
                end_time += datetime.timedelta(days=1)
        
        time_until_end = end_time - current_time
        minutes_until_end = time_until_end.total_seconds() / 60
        
        print(f"⏰ Schedule ends in {minutes_until_end:.1f} minutes")
        
        if minutes_until_end > CANCELLATION_DELAY_THRESHOLD:
            # Send immediate cancellation warning
            print(f"📢 Schedule cancelled - sending immediate warning and scheduling delayed turn_off")
            
            # Send cancellation warning immediately
            send_room_shutdown_warning_mqtt(
                room_id=schedule_slot.room_id,
                subject=schedule_slot.subject,
                reason="cancellation",
                mqtt_client=mqtt_client,
                timeslot_id=timeslot_id,
                minutes_remaining=CANCELLATION_DELAY_DURATION,
                message=f"Schedule '{schedule_slot.subject}' cancelled - room will turn off in {CANCELLATION_DELAY_DURATION} minutes",
                cancellation_reason=reason
            )
            
            if scheduler:
                # Schedule delayed turn off using APScheduler
                turn_off_time = current_time + datetime.timedelta(minutes=CANCELLATION_DELAY_DURATION)
                
                print(f"🕐 Current time: {current_time.strftime('%Y-%m-%d %I:%M:%S %p %Z')} (timezone: {current_time.tzinfo})")
                print(f"🕐 Scheduled turn-off time: {turn_off_time.strftime('%Y-%m-%d %I:%M:%S %p %Z')} (timezone: {turn_off_time.tzinfo})")
                print(f"🕐 DateTrigger timezone: {DEVICE_TZ}")
                print(f"🕐 Time difference: {(turn_off_time - current_time).total_seconds()} seconds")
                
                # Create unique job ID for the delayed turn-off
                delayed_job_id = f"cancel_turnoff_{timeslot_id}_{int(current_time.timestamp())}"
                
                scheduler.add_job(
                    func=_execute_delayed_cancellation_turnoff,
                    trigger=DateTrigger(run_date=turn_off_time, timezone=DEVICE_TZ),
                    args=[
                        schedule_slot.room_id,
                        f"{timeslot_id}{JOB_TURN_OFF_SUFFIX}",
                        f"{timeslot_id}{JOB_TURN_ON_SUFFIX}",
                        schedule_slot.is_temporary,
                        mqtt_client,
                        pg_conn_pool,
                        timeslot_id
                    ],
                    id=delayed_job_id,
                    name=f"Delayed Cancellation Turn-Off: {schedule_slot.subject}",
                    replace_existing=True
                )
                
                print(f"✅ Scheduled delayed turn-off job: {delayed_job_id}")
            else:
                # No scheduler available - turn off immediately as fallback
                print(f"⚠️ No scheduler available - turning off immediately as fallback")
                turn_off_mqtt(schedule_slot.room_id, mqtt_client)
                # For immediate fallback, remove from running jobs now
                # Status cleanup will be handled by normal turn_off mechanism
                pg_remove_running_turn_on_job(pg_conn_pool, timeslot_id)

        else:
            # Turn off immediately
            print(f"🔌 Schedule ends soon (≤ 5 min) - turning off immediately")
            
            # Send immediate cancellation notice
            send_room_shutdown_warning_mqtt(
                schedule_slot.room_id,
                schedule_slot.subject,
                "cancellation",
                mqtt_client,
                message=f"Schedule '{schedule_slot.subject}' cancelled - turning off now",
                cancellation_reason=reason
            )
            
            # Turn off immediately
            turn_off_mqtt(schedule_slot.room_id, mqtt_client)
            
            # For immediate turn-off, remove from running jobs now
            # Status update will be handled by the normal turn_off cleanup mechanism
            pg_remove_running_turn_on_job(pg_conn_pool, timeslot_id)
        
        # Note: For delayed turn-off (> 5 min case), running job removal is handled by the delayed function
        
        # Handle permanent vs temporary schedule cancellation
        return _handle_schedule_type_cancellation(
            timeslot_id, schedule_slot, pg_conn_pool, reason, cancelled_by
        )
        
    except Exception as e:
        print(f"❌ Error cancelling running schedule: {e}")
        return False


def _cancel_future_schedule(
    timeslot_id: str,
    pg_conn_pool: pg_pool.SimpleConnectionPool,
    mqtt_client: mqtt.Client,
    reason: str,
    cancelled_by: str
) -> bool:
    """
    Cancel a future schedule by queuing it in the cancellation table.
    """
    try:
        # Find the schedule in resolved schedule slots
        latest_wrapper = pg_get_latest_schedule_wrapper(pg_conn_pool)
        if not latest_wrapper:
            print(f"❌ No schedule wrapper found")
            return False
            
        all_slots = pg_get_resolved_slots_by_schedule_id(pg_conn_pool, latest_wrapper.schedule_id)
        schedule_slot = None
        
        for slot in all_slots:
            if slot.timeslot_id == timeslot_id:
                schedule_slot = slot
                break
        
        if not schedule_slot:
            print(f"❌ Schedule slot not found: {timeslot_id}")
            return False
        
        print(f"📋 Found future schedule: {schedule_slot.subject} in {schedule_slot.room_id}")
        print(f"📋 Cancellation will be handled when schedule attempts to execute")
        
        # Handle permanent vs temporary schedule cancellation
        return _handle_schedule_type_cancellation(
            timeslot_id, schedule_slot, pg_conn_pool, reason, cancelled_by
        )
        
    except Exception as e:
        print(f"❌ Error cancelling future schedule: {e}")
        return False


def _handle_schedule_type_cancellation(
    timeslot_id: str,
    schedule_slot: ResolvedScheduleSlotV2,
    pg_conn_pool: pg_pool.SimpleConnectionPool,
    reason: str,
    cancelled_by: str
) -> bool:
    """
    Handle cancellation based on schedule type (permanent vs temporary).
    
    Permanent: Cancel only current instance (specific date)
    Temporary: Delete entire schedule from existence
    """
    try:
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        
        if schedule_slot.is_temporary:
            # TEMPORARY SCHEDULE: Delete entirely from database
            print(f"🗑️ Temporary schedule - deleting entirely from database")
            
            # Mark for complete cancellation - this will be cleaned up when the schedule completes
            cancelled_schedule = CancelledSchedule(
                timeslot_id=timeslot_id,
                cancellation_type="temporary_complete",
                cancelled_at=datetime.datetime.now(),
                cancelled_date="all",  # "all" means complete deletion
                reason=reason,
                cancelled_by=cancelled_by
            )
            
            success = pg_insert_cancelled_schedule(pg_conn_pool, cancelled_schedule)
            
            if success:
                print(f"✅ Temporary schedule {timeslot_id} marked for complete deletion")
                print(f"📋 Cancellation cleanup will occur when schedule completes or expires")
                return True
            else:
                print(f"❌ Failed to mark temporary schedule for deletion")
                return False
        else:
            # PERMANENT SCHEDULE: Cancel only this instance (today)
            print(f"📅 Permanent schedule - cancelling only today's instance ({current_date})")
            
            cancelled_schedule = CancelledSchedule(
                timeslot_id=timeslot_id,
                cancellation_type="permanent_instance", 
                cancelled_at=datetime.datetime.now(),
                cancelled_date=current_date,  # Specific date cancellation
                reason=reason,
                cancelled_by=cancelled_by
            )
            
            success = pg_insert_cancelled_schedule(pg_conn_pool, cancelled_schedule)
            
            if success:
                print(f"✅ Permanent schedule {timeslot_id} cancelled for {current_date}")
                return True
            else:
                print(f"❌ Failed to cancel permanent schedule instance")
                return False
                
    except Exception as e:
        print(f"❌ Error handling schedule type cancellation: {e}")
        return False
