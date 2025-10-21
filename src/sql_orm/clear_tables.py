"""
Centralized table clearing functions for all ORM models.
"""

from src.sql_orm.connection.sqlalchemy_pg import get_session
from src.sql_orm.schedule.resolved_schedule_slots_orm import ResolvedScheduleSlotOrm
from src.sql_orm.schedule.schedule_wrapper_orm import ScheduleWrapperOrm
from src.sql_orm.turn_on_jobs.turn_on_job_orm import RunningTurnOnJobOrm
from src.sql_orm.cancellation.cancelled_schedule_orm import CancelledScheduleOrm

def clear_all_tables():
    """
    Clear all tables in the correct order (respecting foreign key constraints).
    Order matters: child tables first, then parent tables.
    """
    print("🧹 Starting table cleanup...")
    
    try:
        session = get_session()
        try:
            # Clear child tables first (tables with foreign keys)
            session.query(CancelledScheduleOrm).delete()
            print("✅ Cleared all cancelled schedules")
            
            session.query(RunningTurnOnJobOrm).delete()
            print("✅ Cleared all running turn on jobs")
            
            session.query(ResolvedScheduleSlotOrm).delete()
            print("✅ Cleared all resolved schedule slots")
            
            # Clear parent tables last
            session.query(ScheduleWrapperOrm).delete()
            print("✅ Cleared all schedule wrappers")
            
            session.commit()
        finally:
            session.close()
        
        print("✅ All tables cleared successfully!")
        
    except Exception as e:
        print(f"❌ Error during table cleanup: {e}")
        raise e