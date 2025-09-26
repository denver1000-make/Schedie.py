""""""

Schedule Migration and Cleanup Utilities - Updated for Unified SystemSchedule Migration and Cleanup Utilities

Migration utilities are now mostly informational since legacy tables have been removedHandles migrat    """

"""    Migrate a single TemporarySchedule to normalized tables (ScheduleWrapper + ResolvedScheduleSlotV2)

    

from typing import List, Optional    Args:

from psycopg2 import pool        pg_pool: PostgreSQL connection pool

import time        temp_schedule: TemporarySchedule to migrate

        

from src.modelsV2.model import TemporarySchedule, ScheduleWrapper, ResolvedScheduleSlotV2    Returns:

from src.sql.schedule.schedule_wrapper_sql import pg_delete_schedule_wrapper        bool: True if migration successful, False otherwise

from src.sql.resolved_timeslot.resolved_timeslot import pg_insert_resolved_time_slots    """

from src.sql.schedule.schedule_wrapper_sql import pg_insert_schedule_wrapper    try:

        print("📝 Legacy temporary_schedules table no longer exists")

        print("🎉 Individual migration not needed - all schedules use unified system")

def cleanup_completed_temporary_schedule(        return True  # Migration already completemporary schedule tables to unified normalized tables

    pg_pool: pool.SimpleConnectionPool, and cleanup of completed temporary schedules

    schedule_id: str,"""

    remove_from_legacy_table: bool = True

) -> bool:from typing import List, Optional

    """from psycopg2 import pool

    Completely remove a completed temporary schedule from unified databaseimport datetime

    

    Args:from src.modelsV2.model import TemporarySchedule, ScheduleWrapper, ResolvedScheduleSlotV2

        pg_pool: PostgreSQL connection poolfrom src.sql.schedule.schedule_wrapper_sql import pg_delete_schedule_wrapper

        schedule_id: The schedule ID to remove (same as temporary_schedule_id for temp schedules)# Legacy temporary_schedule SQL imports removed - tables no longer exist

        remove_from_legacy_table: Legacy parameter - ignored (table no longer exists)from src.sql.resolved_timeslot.resolved_timeslot import pg_insert_resolved_time_slots

        from src.sql.schedule.schedule_wrapper_sql import pg_insert_schedule_wrapper

    Returns:

        bool: True if cleanup successful, False otherwise

    """def cleanup_completed_temporary_schedule(

    try:    pg_pool: pool.SimpleConnectionPool, 

        print(f"🧹 Cleaning up completed temporary schedule: {schedule_id}")    schedule_id: str,

            remove_from_legacy_table: bool = True

        # Remove from unified tables (schedule_wrapper and resolved_schedule_slots via CASCADE)) -> bool:

        normalized_success = pg_delete_schedule_wrapper(pg_pool, schedule_id)    """

            Completely remove a completed temporary schedule from all databases

        if normalized_success:    

            print(f"✅ Removed from unified tables: {schedule_id}")    Args:

        else:        pg_pool: PostgreSQL connection pool

            print(f"⚠️ Failed to remove from unified tables: {schedule_id}")        schedule_id: The schedule ID to remove (same as temporary_schedule_id for temp schedules)

                remove_from_legacy_table: Whether to also remove from old temporary_schedules table

        # Legacy temporary_schedules table no longer exists        

        if remove_from_legacy_table:    Returns:

            print(f"📝 Legacy temporary_schedules table no longer exists - cleanup complete: {schedule_id}")        bool: True if cleanup successful, False otherwise

            """

        return normalized_success    try:

                print(f"🧹 Cleaning up completed temporary schedule: {schedule_id}")

    except Exception as e:        

        print(f"❌ Error during temporary schedule cleanup: {e}")        # Remove from normalized tables (schedule_wrapper and resolved_schedule_slots via CASCADE)

        return False        normalized_success = pg_delete_schedule_wrapper(pg_pool, schedule_id)

        

        if normalized_success:

def migrate_temporary_schedule_to_normalized(            print(f"✅ Removed from normalized tables: {schedule_id}")

    pg_pool: pool.SimpleConnectionPool,        else:

    temp_schedule: TemporarySchedule            print(f"⚠️ Failed to remove from normalized tables: {schedule_id}")

) -> bool:        

    """        # Optionally remove from legacy temporary_schedules table

    Migrate a single TemporarySchedule to normalized tables        if remove_from_legacy_table:

    Note: Migration not needed since legacy tables no longer exist            print(f"📝 Legacy temporary_schedules table no longer exists - cleanup complete: {schedule_id}")

            

    Args:        return normalized_success

        pg_pool: PostgreSQL connection pool        

        temp_schedule: TemporarySchedule to migrate    except Exception as e:

                print(f"❌ Error during temporary schedule cleanup: {e}")

    Returns:        return False

        bool: True (migration already complete)

    """

    try:def migrate_temporary_schedule_to_normalized(

        print(f"📝 Migration for {temp_schedule.temporary_schedule_id} not needed")    pg_pool: pool.SimpleConnectionPool,

        print("🎉 All schedules already use unified system - legacy tables removed")    temp_schedule: TemporarySchedule

        return True  # Migration already complete) -> bool:

            """

    except Exception as e:    Migrate a single TemporarySchedule to normalized tables (ScheduleWrapper + ResolvedScheduleSlotV2)

        print(f"❌ Error: {e}")    

        return False    Args:

        pg_pool: PostgreSQL connection pool

        temp_schedule: TemporarySchedule to migrate

def migrate_all_temporary_schedules_to_normalized(        

    pg_pool: pool.SimpleConnectionPool,    Returns:

    remove_from_legacy_after_migration: bool = False        bool: True if migration successful, False otherwise

) -> tuple[int, int]:    """

    """    try:

    Migrate all existing temporary schedules from old table to normalized tables        try:

    Note: Migration not needed since legacy tables no longer exist        print("� Legacy temporary_schedules table no longer exists")

            print("🎉 Migration already complete - all schedules use unified system")

    Args:        return 0, 0  # No schedules to migrate from non-existent table

        pg_pool: PostgreSQL connection pool        

        remove_from_legacy_after_migration: Legacy parameter - ignored        # Create ScheduleWrapper (normalized metadata)

                schedule_wrapper = ScheduleWrapper(

    Returns:            schedule_id=temp_schedule.temporary_schedule_id,  # Use temp_schedule_id as schedule_id

        tuple: (0, 0) - no schedules to migrate from non-existent table            upload_date_epoch=temp_schedule.upload_date,

    """            is_temporary=True,

    try:            is_synced_to_remote=temp_schedule.is_synced_to_remote,

        print("📝 Bulk migration not needed - legacy temporary_schedules table no longer exists")            is_from_remote=temp_schedule.is_from_remote

        print("🎉 Migration already complete - all schedules use unified system")        )

        return 0, 0  # No schedules to migrate from non-existent table        

                # Create ResolvedScheduleSlotV2 (normalized schedule slot)

    except Exception as e:        schedule_slot = ResolvedScheduleSlotV2(

        print(f"❌ Error: {e}")            timeslot_id=temp_schedule.temporary_schedule_id,

        return 0, 0            schedule_id=temp_schedule.temporary_schedule_id,

            room_id=temp_schedule.room_id,

            day_name=temp_schedule.day_name,

def cleanup_all_completed_temporary_schedules(            day_order=temp_schedule.day_order,

    pg_pool: pool.SimpleConnectionPool,            start_time=f"{temp_schedule.start_hour:02d}:{temp_schedule.start_minute:02d}",

    include_legacy_table_cleanup: bool = True            end_time=f"{temp_schedule.end_hour:02d}:{temp_schedule.end_minute:02d}",

) -> int:            subject="Temporary Class",  # Default subject for temporary schedules

    """            teacher=temp_schedule.teacher,

    Clean up all completed temporary schedules from database            teacher_email=temp_schedule.teacher_email,

    Now only works with unified tables            start_hour=temp_schedule.start_hour,

                start_minute=temp_schedule.start_minute,

    Args:            end_hour=temp_schedule.end_hour,

        pg_pool: PostgreSQL connection pool            end_minute=temp_schedule.end_minute,

        include_legacy_table_cleanup: Legacy parameter - ignored (table no longer exists)            time_start_in_seconds=None,  # Can be calculated if needed

                    start_date_in_seconds_epoch=temp_schedule.start_date_time,

    Returns:            end_date_in_seconds_epoch=temp_schedule.end_date_time,

        int: Number of schedules cleaned up            is_temporary=True

    """        )

    try:        

        print("🧹 Cleaning up completed temporary schedules from unified system...")        # Insert ScheduleWrapper first (parent record)

                wrapper_success = pg_insert_schedule_wrapper(pg_pool, schedule_wrapper)

        # In the unified system, temporary schedules are automatically cleaned up        if not wrapper_success:

        # by the turn_off_with_cleanup function after execution            print(f"❌ Failed to insert schedule wrapper for: {temp_schedule.temporary_schedule_id}")

        print("📝 Note: Unified system has automatic cleanup - manual cleanup may not be needed")            return False

                

        # Could add logic here to find and clean completed schedules if needed        # Insert ResolvedScheduleSlotV2 (child record)

        # For now, return 0 since automatic cleanup handles this        slots_success = pg_insert_resolved_time_slots(pg_pool, [schedule_slot], schedule_wrapper.schedule_id)

        return 0        if not slots_success:

                    print(f"❌ Failed to insert resolved slot for: {temp_schedule.temporary_schedule_id}")

    except Exception as e:            return False

        print(f"❌ Error during cleanup: {e}")        

        return 0        print(f"✅ Successfully migrated temporary schedule: {temp_schedule.temporary_schedule_id}")

        return True

        

def get_migration_status(pg_pool: pool.SimpleConnectionPool) -> dict:    except Exception as e:

    """        print(f"❌ Error migrating temporary schedule {temp_schedule.temporary_schedule_id}: {e}")

    Get status information about temporary schedule migration        return False

    

    Returns:

        dict: Migration status informationdef migrate_all_temporary_schedules_to_normalized(

    """    pg_pool: pool.SimpleConnectionPool,

    try:    remove_from_legacy_after_migration: bool = False

        return {) -> tuple[int, int]:

            "migration_complete": True,    """

            "legacy_tables_exist": False,    Migrate all existing temporary schedules from old table to normalized tables

            "unified_system_active": True,    

            "automatic_cleanup_enabled": True,    Args:

            "message": "Migration complete - all schedules use unified system"        pg_pool: PostgreSQL connection pool

        }        remove_from_legacy_after_migration: Whether to remove from old table after successful migration

                

    except Exception as e:    Returns:

        print(f"❌ Error getting migration status: {e}")        tuple: (successful_migrations, total_schedules)

        return {    """

            "migration_complete": False,    try:

            "error": str(e)        print("🚀 Starting migration of all temporary schedules to normalized tables...")

        }        
        # Get all temporary schedules from legacy table
        temp_schedules = pg_get_all_temporary_schedules(pg_pool)
        total_schedules = len(temp_schedules)
        
        if total_schedules == 0:
            print("ℹ️ No temporary schedules found to migrate")
            return 0, 0
        
        print(f"📊 Found {total_schedules} temporary schedules to migrate")
        
        successful_migrations = 0
        
        for temp_schedule in temp_schedules:
            migration_success = migrate_temporary_schedule_to_normalized(pg_pool, temp_schedule)
            
            if migration_success:
                successful_migrations += 1
                
                # Optionally remove from legacy table after successful migration
                if remove_from_legacy_after_migration:
                    removal_success = pg_delete_temporary_schedule(pg_pool, temp_schedule.temporary_schedule_id)
                    if removal_success:
                        print(f"🗑️ Removed from legacy table: {temp_schedule.temporary_schedule_id}")
                    else:
                        print(f"⚠️ Failed to remove from legacy table: {temp_schedule.temporary_schedule_id}")
        
        print(f"✅ Migration complete: {successful_migrations}/{total_schedules} successful")
        return successful_migrations, total_schedules
        
    except Exception as e:
        print(f"❌ Error during bulk migration: {e}")
        return 0, 0


def cleanup_all_completed_temporary_schedules(
    pg_pool: pool.SimpleConnectionPool,
    include_legacy_table_cleanup: bool = True
) -> int:
    """
    Clean up all completed temporary schedules from database
    
    Args:
        pg_pool: PostgreSQL connection pool
        include_legacy_table_cleanup: Whether to also clean legacy temporary_schedules table
        
    Returns:
        int: Number of schedules cleaned up
    """
    try:
        print("🧹 Starting cleanup of all completed temporary schedules...")
        
        # Get all temporary schedules marked as completed
        temp_schedules = pg_get_all_temporary_schedules(pg_pool)
        completed_schedules = [ts for ts in temp_schedules if ts.is_completed]
        
        if not completed_schedules:
            print("ℹ️ No completed temporary schedules found to clean up")
            return 0
        
        print(f"📊 Found {len(completed_schedules)} completed temporary schedules to clean up")
        
        cleaned_count = 0
        
        for temp_schedule in completed_schedules:
            cleanup_success = cleanup_completed_temporary_schedule(
                pg_pool, 
                temp_schedule.temporary_schedule_id,
                remove_from_legacy_table=include_legacy_table_cleanup
            )
            
            if cleanup_success:
                cleaned_count += 1
        
        print(f"✅ Cleanup complete: {cleaned_count} schedules removed")
        return cleaned_count
        
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        return 0


def get_migration_status(pg_pool: pool.SimpleConnectionPool) -> dict:
    """
    Get status information about temporary schedule migration
    
    Returns:
        dict: Migration status information
    """
    try:
        # Count legacy temporary schedules
        temp_schedules = pg_get_all_temporary_schedules(pg_pool)
        legacy_count = len(temp_schedules)
        legacy_completed = len([ts for ts in temp_schedules if ts.is_completed])
        legacy_active = legacy_count - legacy_completed
        
        # Count normalized schedules with is_temporary=True
        from src.sql.schedule.schedule_wrapper_sql import pg_get_all_schedule_wrappers
        
        all_wrappers = pg_get_all_schedule_wrappers(pg_pool)
        temporary_wrappers = [w for w in all_wrappers if w.is_temporary]
        normalized_count = len(temporary_wrappers)
        
        return {
            "legacy_temporary_schedules": legacy_count,
            "legacy_completed": legacy_completed,
            "legacy_active": legacy_active,
            "normalized_temporary_schedules": normalized_count,
            "migration_needed": legacy_count > 0,
            "cleanup_needed": legacy_completed > 0
        }
        
    except Exception as e:
        print(f"❌ Error getting migration status: {e}")
        return {}