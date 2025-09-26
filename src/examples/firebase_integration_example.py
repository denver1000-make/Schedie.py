"""
Example integration showing how to use the enhanced schedule processing
in your main application
"""
from typing import List
from src.modelsV2.model import ResolvedScheduleSlotV2
from src.sql.schedule_sql import pg_insert_resolved_slots, pg_mark_schedule_as_processed
from src.schedulerv2.job_funcs import schedule_v3_with_immediate_check
from datetime import datetime
from src.constants import DEVICE_TZ

def handle_new_schedule_from_firebase(resolved_slots: List[ResolvedScheduleSlotV2]):
    """
    Example callback function for handling new schedule data from Firebase
    This function would be passed to setup_schedule_raw_processor()
    
    Args:
        resolved_slots: List of ResolvedScheduleSlotV2 objects from Firebase
    """
    if not resolved_slots:
        print("⚠️ No resolved slots received")
        return
    
    # Extract schedule metadata
    first_slot = resolved_slots[0]
    schedule_id = f"firebase_{first_slot.room_id}_{int(datetime.now().timestamp())}"
    is_temp = any(slot.is_temporary for slot in resolved_slots)
    
    print(f"📅 Processing new schedule from Firebase:")
    print(f"   Schedule ID: {schedule_id}")
    print(f"   Slots: {len(resolved_slots)}")
    print(f"   Temporary: {is_temp}")
    print(f"   Rooms: {set(slot.room_id for slot in resolved_slots)}")
    
    try:
        # Here you would access your global variables from main_v3.py
        # For this example, we'll show the structure
        
        # 1. Insert into database
        # success = pg_insert_resolved_slots(pg_conn_pool, schedule_id, resolved_slots)
        # if not success:
        #     print("❌ Failed to insert schedule slots into database")
        #     return
        
        # 2. Mark as processed
        # pg_mark_schedule_as_processed(pg_conn_pool, schedule_id, {
        #     "topic": "firebase/schedule_raw",
        #     "slot_count": len(resolved_slots),
        #     "upload_date_epoch": datetime.now().timestamp(),
        #     "is_temp": is_temp
        # })
        
        # 3. Schedule the jobs
        # schedule_v3_with_immediate_check(
        #     list_resolved_slots=resolved_slots,
        #     scheduler_v2=scheduler_v2,
        #     mqtt_client=mqtt_client,
        #     pg_conn_pool=pg_conn_pool,
        #     isTemp=is_temp,
        #     current_time=datetime.now(tz=DEVICE_TZ)
        # )
        
        print("✅ Schedule processing completed successfully!")
        print(f"   {len(resolved_slots)} slots scheduled")
        
        # Show what would be scheduled
        for slot in resolved_slots:
            print(f"   📋 {slot.room_id}: {slot.day_name} {slot.start_time}-{slot.end_time} ({slot.subject})")
    
    except Exception as e:
        print(f"❌ Error processing schedule from Firebase: {e}")

# Example of how to integrate this in your main_v3.py:
"""
In your main_v3.py, you would modify the Firebase setup like this:

# Instead of:
# _schedule_raw_unsubscribe = setup_schedule_raw_listener(firestore_db)

# Use this for enhanced processing:
from src.firestore.schedule_raw_listener import setup_schedule_raw_processor

_schedule_raw_unsubscribe = setup_schedule_raw_processor(
    firestore_db, 
    callback=handle_new_schedule_from_firebase
)

This way, when your Kotlin app uploads to schedule_raw:
1. The data is automatically displayed (existing functionality)
2. The data is processed into ResolvedScheduleSlotV2 objects
3. Your callback function handles database insertion and job scheduling
4. Everything is automatically integrated into your existing scheduling system
"""
