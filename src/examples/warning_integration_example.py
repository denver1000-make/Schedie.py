"""
Example usage of Schedule Warning functionality in main application
"""

import asyncio
import datetime
from src.schedulerv2.scheduler_v2 import init_scheduler
from src.schedulerv2.warning_funcs import schedule_warning_jobs_for_slots, clear_warning_jobs
from src.modelsV2.model import ResolvedScheduleSlotV2
from src.mqtt.mqtt_manager import init_mqtt


async def example_warning_integration():
    """
    Example of how to integrate warning functionality into your main application
    """
    
    # Initialize scheduler and MQTT (same as your main app)
    scheduler = await init_scheduler()
    # mqtt_client = init_mqtt("localhost", 1883, "username", "password")  # Use your real MQTT setup
    # pg_conn_pool = your_real_database_pool  # Use your real database pool
    
    print("⚠️ Note: This is a demo - replace with your real MQTT and DB connections")
    return  # Skip the actual execution in this demo
    
    # Example schedule slots (normally you'd get these from the database)
    example_slots = [
        ResolvedScheduleSlotV2(
            timeslot_id="demo_math_slot",
            schedule_id="demo_schedule",
            room_id="ROOM_101",
            day_name="Monday",
            day_order=1,
            start_hour=10,
            start_minute=0,
            end_hour=11,
            end_minute=0,
            start_time="10:00",
            end_time="11:00",
            subject="Mathematics",
            teacher="Dr. Smith",
            teacher_email="smith@school.edu",
            time_start_in_seconds=36000,
            start_date_in_seconds_epoch=None,
            end_date_in_seconds_epoch=None,
            is_temporary=False
        )
    ]
    
    print("🚀 Example: How to schedule warning jobs...")
    print("   Call schedule_warning_jobs_for_slots() after scheduling your main jobs")
    print("   It will read minute_mark_to_warn from your database settings")
    print("   And schedule MQTT warning messages X minutes before each schedule ends")
    
    # Example of how you'd call it in your real application:
    # schedule_warning_jobs_for_slots(
    #     slots=your_schedule_slots,
    #     scheduler_v2=your_scheduler,
    #     mqtt_client=your_mqtt_client,
    #     pg_conn_pool=your_pg_conn_pool
    # )


if __name__ == "__main__":
    # Run the example
    asyncio.run(example_warning_integration())
