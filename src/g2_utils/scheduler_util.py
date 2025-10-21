import datetime
from typing import Any, List
import apscheduler.schedulers.background as scheduler
from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from src.g2_utils.jobs.jobs_g2 import turn_off_proc, turn_on_proc, warning_proc
from src.sql_orm.cancellation.cancelled_schedule_orm import check_if_timeslot_cancelled
from src.sql_orm.schedule.resolved_schedule_slots_orm import ResolvedScheduleSlotOrm
from src.constants import DEVICE_TZ, MINUTE_MARK_TO_WARN, MINUTE_MARK_TO_SKIP
from paho.mqtt.client import Client

def schedule_resolved_slots(
    scheduler: scheduler.BackgroundScheduler, 
    mqtt_client: Client,
    resolved_slots: List[ResolvedScheduleSlotOrm],
    minute_mark_to_skip: int = MINUTE_MARK_TO_SKIP,
    minute_mark_to_warn: int = MINUTE_MARK_TO_WARN
):
    current_time = datetime.datetime.now(tz=DEVICE_TZ)
    
    for resolved_slot in resolved_slots:
        if resolved_slot.is_temporary and resolved_slot.start_date_in_seconds_epoch and resolved_slot.end_date_in_seconds_epoch:
            start_dt = datetime.datetime.fromtimestamp(resolved_slot.start_date_in_seconds_epoch, tz=DEVICE_TZ)
            end_dt = datetime.datetime.fromtimestamp(resolved_slot.end_date_in_seconds_epoch, tz=DEVICE_TZ)
            
            if start_dt <= current_time <= end_dt:
                turn_on_proc(resolved_slot.timeslot_id, mqtt_client)
                remaining_minutes = int((end_dt - current_time).total_seconds() / 60)
                if remaining_minutes <= minute_mark_to_warn:
                    warning_proc(resolved_slot.timeslot_id, minute_mark_to_skip, mqtt_client)
        
        else:
            current_day = current_time.strftime('%A')
            if resolved_slot.day_name == current_day:
                current_minutes = current_time.hour * 60 + current_time.minute
                start_minutes = resolved_slot.start_hour * 60 + resolved_slot.start_minute
                end_minutes = resolved_slot.end_hour * 60 + resolved_slot.end_minute
                
                if end_minutes < start_minutes:
                    end_minutes += 24 * 60
                
                if start_minutes <= current_minutes <= end_minutes:
                    turn_on_proc(resolved_slot.timeslot_id, mqtt_client)
                    
                    remaining_minutes = end_minutes - current_minutes
                    if remaining_minutes <= minute_mark_to_warn:
                        warning_proc(resolved_slot.timeslot_id, minute_mark_to_skip, mqtt_client)
    
    for resolved_slot in resolved_slots:
        
        start_trigger: BaseTrigger
        end_trigger: BaseTrigger
        
        day_name_map = {
                'Monday': 'mon', 'Tuesday': 'tue', 'Wednesday': 'wed',
                'Thursday': 'thu', 'Friday': 'fri', 'Saturday': 'sat', 'Sunday': 'sun'
            }
                
        scheduler_day = day_name_map.get(resolved_slot.day_name, resolved_slot.day_name.lower()[:3])
            
        
        if resolved_slot.is_temporary and resolved_slot.start_date_in_seconds_epoch and resolved_slot.end_date_in_seconds_epoch:
            
            dt_start = datetime.datetime.fromtimestamp(
                resolved_slot.start_date_in_seconds_epoch,
                tz=DEVICE_TZ  
            )
            dt_end = datetime.datetime.fromtimestamp(
                resolved_slot.end_date_in_seconds_epoch,
                tz=DEVICE_TZ  
            )
            
            start_trigger = DateTrigger(
                run_date=dt_start,
                timezone=DEVICE_TZ
            )
            
            end_trigger = DateTrigger(
                run_date=dt_end,
                timezone=DEVICE_TZ
            )
            
            scheduler.add_job(
                func=turn_on_proc,
                trigger=start_trigger,
                args=[resolved_slot.timeslot_id, mqtt_client],
                id=f"{resolved_slot.timeslot_id}_turn_on",
                replace_existing=True
            )
            
            scheduler.add_job(
                func=turn_off_proc,
                trigger=end_trigger,
                args=[resolved_slot.timeslot_id, mqtt_client, minute_mark_to_skip],
                id=f"{resolved_slot.timeslot_id}_turn_off",
                replace_existing=True
            )
            
            start_dt = datetime.datetime.fromtimestamp(resolved_slot.start_date_in_seconds_epoch, tz=DEVICE_TZ)
            end_dt = datetime.datetime.fromtimestamp(resolved_slot.end_date_in_seconds_epoch, tz=DEVICE_TZ)
            duration_minutes = int((end_dt - start_dt).total_seconds() / 60)
            
            if duration_minutes <= minute_mark_to_warn:
                warning_trigger = DateTrigger(
                    run_date=start_dt,
                    timezone=DEVICE_TZ
                )
            else:
                warning_dt = end_dt - datetime.timedelta(minutes=minute_mark_to_warn)
                warning_trigger = DateTrigger(
                    run_date=warning_dt,
                    timezone=DEVICE_TZ
                )
            
            scheduler.add_job(
                func=warning_proc,
                trigger=warning_trigger,
                args=[resolved_slot.timeslot_id, minute_mark_to_skip, mqtt_client],
                id=f"{resolved_slot.timeslot_id}_warning",
                replace_existing=True
            )
            
        else:
            
            start_trigger = CronTrigger(
                day_of_week=scheduler_day,
                hour=resolved_slot.start_hour,
                minute=resolved_slot.start_minute,
                second=0,  # Always set seconds
                timezone=DEVICE_TZ  # Always set timezone
            )
            
            end_trigger = CronTrigger(
                day_of_week=scheduler_day,
                hour=resolved_slot.end_hour,
                minute=resolved_slot.end_minute,
                second=0,  # Always set seconds
                timezone=DEVICE_TZ  # Always set timezone
            )
            
            scheduler.add_job(
                func=turn_on_proc,
                trigger=start_trigger,
                args=[resolved_slot.timeslot_id, mqtt_client],
                id=f"{resolved_slot.timeslot_id}_turn_on",
                replace_existing=True
            )
            
            scheduler.add_job(
                func=turn_off_proc,
                trigger=end_trigger,
                args=[resolved_slot.timeslot_id, mqtt_client, minute_mark_to_skip],
                id=f"{resolved_slot.timeslot_id}_turn_off",
                replace_existing=True
            )
            
            # Schedule warning job for regular schedules
            # Calculate schedule duration in minutes
            duration_minutes = (resolved_slot.end_hour * 60 + resolved_slot.end_minute) - (resolved_slot.start_hour * 60 + resolved_slot.start_minute)
            if duration_minutes < 0:  # Handle day overflow (e.g., 23:30 to 01:00)
                duration_minutes += 24 * 60
            
            # If duration <= MINUTE_MARK_TO_WARN, schedule warning at start time
            # Otherwise, schedule warning MINUTE_MARK_TO_WARN minutes before end
            if duration_minutes <= minute_mark_to_warn:
                warning_trigger = CronTrigger(
                    day_of_week=scheduler_day,
                    hour=resolved_slot.start_hour,
                    minute=resolved_slot.start_minute,
                    second=0,
                    timezone=DEVICE_TZ
                )
            else:
                # Calculate warning time (end_time - MINUTE_MARK_TO_WARN)
                warn_hour = resolved_slot.end_hour
                warn_minute = resolved_slot.end_minute - minute_mark_to_warn
                
                # Handle minute underflow
                while warn_minute < 0:
                    warn_minute += 60
                    warn_hour -= 1
                
                # Handle hour underflow (skip if warning would be on previous day)
                if warn_hour < 0:
                    warn_hour += 24
                
                warning_trigger = CronTrigger(
                    day_of_week=scheduler_day,
                    hour=warn_hour,
                    minute=warn_minute,
                    second=0,
                    timezone=DEVICE_TZ
                )
            
            scheduler.add_job(
                func=warning_proc,
                trigger=warning_trigger,
                args=[resolved_slot.timeslot_id, minute_mark_to_skip, mqtt_client],
                id=f"{resolved_slot.timeslot_id}_warning",
                replace_existing=True
            )

    