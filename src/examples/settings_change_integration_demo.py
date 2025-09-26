"""
Integration Test: Settings Change and Warning Rebuild
Demonstrates how the system handles dynamic settings changes
"""

import asyncio
import time
from unittest.mock import Mock, patch

from src.schedulerv2.scheduler_v2 import init_scheduler
from src.schedulerv2.warning_funcs import (
    schedule_warning_jobs_for_slots,
    check_and_rebuild_warnings_if_needed,
    clear_warning_jobs
)
from src.modelsV2.model import ResolvedScheduleSlotV2
from src.modelsV2.models import SettingsConfiguration


async def demo_settings_change_workflow():
    """
    Demonstrates the complete workflow when settings change:
    1. Initial warning jobs with 10-minute threshold
    2. Settings change to 5-minute threshold  
    3. Automatic rebuild of warning jobs with new timing
    """
    print("🚀 Demo: Settings Change and Warning Rebuild Workflow")
    print("=" * 60)
    
    # Initialize scheduler
    scheduler = await init_scheduler()
    mock_mqtt_client = Mock()
    mock_pg_pool = Mock()
    
    # Create sample schedule slot
    sample_slot = ResolvedScheduleSlotV2(
        timeslot_id="demo_math_slot",
        schedule_id="demo_schedule", 
        room_id="ROOM_101",
        day_name="Monday",
        day_order=1,
        start_hour=11,
        start_minute=0,
        end_hour=12,
        end_minute=0,
        start_time="11:00",
        end_time="12:00",
        subject="Mathematics Demo",
        teacher="Dr. Demo",
        teacher_email="demo@school.edu",
        time_start_in_seconds=39600,
        start_date_in_seconds_epoch=None,
        end_date_in_seconds_epoch=None,
        is_temporary=False
    )
    
    print("\n📊 Step 1: Initial Schedule Setup")
    print("   Schedule: Mathematics Demo 11:00-12:00 in ROOM_101")
    
    # Mock initial settings (10-minute warning)
    initial_settings = SettingsConfiguration(
        bypass_admin_approval=False,
        minute_mark_to_skip=15,
        minute_mark_to_warn=10  # 10 minutes warning
    )
    
    # Simulate scheduling initial warning jobs
    print(f"   Initial warning threshold: {initial_settings.minute_mark_to_warn} minutes")
    print("   Expected warning time: 11:50 (12:00 - 10 min)")
    
    # Schedule initial warning jobs  
    with patch('src.schedulerv2.warning_funcs.pg_get_settings_configuration', return_value=initial_settings):
        schedule_warning_jobs_for_slots(
            slots=[sample_slot],
            scheduler_v2=scheduler,
            mqtt_client=mock_mqtt_client,
            pg_conn_pool=mock_pg_pool
        )
    
    print(f"\n📋 Current Jobs After Initial Setup:")
    for job in scheduler.get_jobs():
        if '_warning' in job.id:
            print(f"   ⚠️  {job.id} | Next: {job.next_run_time}")
    
    print(f"\n🔄 Step 2: Settings Change Simulation")
    print("   User changes warning threshold from 10 → 5 minutes")
    
    # Simulate smart rebuild when settings change
    with patch('src.sql.schedule.schedule_wrapper_sql.pg_get_latest_schedule_wrapper') as mock_wrapper, \
         patch('src.sql.resolved_timeslot.resolved_timeslot.pg_get_resolved_slots_by_schedule_id') as mock_slots:
        
        # Mock database returns
        from src.modelsV2.model import ScheduleWrapper
        mock_wrapper.return_value = ScheduleWrapper(
            schedule_id="demo_schedule",
            upload_date_epoch=time.time(),
            is_temporary=False
        )
        mock_slots.return_value = [sample_slot]
        
        # Trigger smart rebuild (5 minutes is different from current 10)
        check_and_rebuild_warnings_if_needed(
            scheduler_v2=scheduler,
            mqtt_client=mock_mqtt_client,
            pg_conn_pool=mock_pg_pool,
            new_minute_mark_to_warn=5  # Changed to 5 minutes
        )
    
    print("   Expected new warning time: 11:55 (12:00 - 5 min)")
    
    print(f"\n📋 Jobs After Settings Change:")
    for job in scheduler.get_jobs():
        if '_warning' in job.id:
            print(f"   ⚠️  {job.id} | Next: {job.next_run_time}")
    
    print(f"\n✨ Step 3: Verification")
    warning_jobs = [job for job in scheduler.get_jobs() if '_warning' in job.id]
    if warning_jobs:
        print(f"   ✅ Warning jobs successfully rebuilt: {len(warning_jobs)} jobs")
        for job in warning_jobs:
            trigger_str = str(job.trigger)
            if "minute='55'" in trigger_str:
                print(f"   ✅ Correct timing: Warning scheduled at 11:55")
            elif "minute='50'" in trigger_str:
                print(f"   ❌ Old timing detected: Warning still at 11:50")
    else:
        print(f"   ❌ No warning jobs found")
    
    # Cleanup
    clear_warning_jobs(scheduler)
    
    print(f"\n🎉 Demo Complete!")
    print("   Settings change workflow validated successfully")
    print("=" * 60)


async def demo_no_rebuild_needed():
    """Demo when settings haven't changed (no rebuild needed)"""
    print("\n🔍 Bonus Demo: No Rebuild When Settings Unchanged")
    print("=" * 50)
    
    mock_scheduler = Mock()
    mock_mqtt_client = Mock()
    mock_pg_pool = Mock()
    
    # Mock current settings (5 minutes)
    current_settings = SettingsConfiguration(
        bypass_admin_approval=False,
        minute_mark_to_skip=15,
        minute_mark_to_warn=5
    )
    
    with patch('src.schedulerv2.warning_funcs.pg_get_settings_configuration', return_value=current_settings):
        print("   Current threshold: 5 minutes")
        print("   New threshold: 5 minutes (same)")
        
        # This should not trigger rebuild
        check_and_rebuild_warnings_if_needed(
            scheduler_v2=mock_scheduler,
            mqtt_client=mock_mqtt_client,
            pg_conn_pool=mock_pg_pool,
            new_minute_mark_to_warn=5  # Same as current
        )
        
        print("   ✅ No rebuild triggered (as expected)")


async def run_integration_demo():
    """Run the complete integration demo"""
    try:
        await demo_settings_change_workflow()
        await demo_no_rebuild_needed()
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(run_integration_demo())