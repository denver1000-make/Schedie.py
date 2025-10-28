
from sqlalchemy import Table, Column, Integer, ForeignKey, String, Float
from sqlalchemy.orm import relationship, Mapped, mapped_column
import sqlalchemy as sa
from src.sql_orm.connection.sqlalchemy_pg import get_session
from src.sql_orm.connection.base import Base

class ScheduleWrapperOrm(Base):
    __tablename__ = "schedule_wrappers_v2"
    
    schedule_id: Mapped[str] = mapped_column(sa.String, primary_key=True)
    upload_date_epoch: Mapped[float] = mapped_column(sa.Float)
    is_temporary: Mapped[bool] = mapped_column(sa.Boolean, default=False)
    is_synced_to_remote: Mapped[bool] = mapped_column(sa.Boolean, default=True)
    is_from_remote: Mapped[bool] = mapped_column(sa.Boolean, default=True)
    in_use: Mapped[bool] = mapped_column(sa.Boolean)
    
    # Relationship to ResolvedScheduleSlotV2 (one-to-many)
    schedule_slots = relationship("ResolvedScheduleSlotOrm", back_populates="schedule_wrapper")

def get_in_use_schedule() -> ScheduleWrapperOrm | None:
    session = get_session()
    try:
        return session.query(ScheduleWrapperOrm).filter(
            ScheduleWrapperOrm.in_use == True
        ).first()
    finally:
        session.close()

def schedule_exists(schedule_id: str) -> bool:
    """Check if a schedule_id already exists in the database"""
    session = get_session()
    try:
        return session.query(ScheduleWrapperOrm).filter(
            ScheduleWrapperOrm.schedule_id == schedule_id
        ).first() is not None
    finally:
        session.close()
    
    
def insert_schedule_wrapper(schedule_wrapper_orm: ScheduleWrapperOrm):
    session = get_session()
    try:
        session.add(schedule_wrapper_orm)
        session.commit()
        return schedule_wrapper_orm
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def set_all_schedule_wrappers_not_in_use():
    session = get_session()
    try:
        updated_count = session.query(ScheduleWrapperOrm).update(
            {ScheduleWrapperOrm.in_use: False}
        )
        session.commit()
        return updated_count
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def get_schedule_wrapper_by_timeslot_id(timeslot_id: str) -> ScheduleWrapperOrm | None:
    """
    Get schedule wrapper by looking up through resolved schedule slots.
    
    Args:
        timeslot_id: The timeslot ID to find the schedule wrapper for
        
    Returns:
        ScheduleWrapperOrm: The schedule wrapper if found, None otherwise
    """
    session = get_session()
    try:
        from src.sql_orm.schedule.resolved_schedule_slots_orm import ResolvedScheduleSlotOrm
        
        result = session.query(ScheduleWrapperOrm).join(
            ResolvedScheduleSlotOrm,
            ScheduleWrapperOrm.schedule_id == ResolvedScheduleSlotOrm.schedule_id
        ).filter(
            ResolvedScheduleSlotOrm.timeslot_id == timeslot_id
        ).first()
        
        return result
    finally:
        session.close()


def delete_schedule_wrapper_by_id(schedule_id: str) -> bool:
    """
    Delete a schedule wrapper by schedule_id.
    Manually deletes related resolved_schedule_slots first, then the wrapper.
    
    Args:
        schedule_id: The schedule ID to delete
        
    Returns:
        bool: True if deletion successful, False otherwise
    """
    session = get_session()
    try:
        # Import here to avoid circular imports
        from src.sql_orm.schedule.resolved_schedule_slots_orm import ResolvedScheduleSlotOrm
        
        # First, manually delete all resolved schedule slots for this schedule_id
        slots_deleted = session.query(ResolvedScheduleSlotOrm).filter(
            ResolvedScheduleSlotOrm.schedule_id == schedule_id
        ).delete()
        
        print(f"🗑️ Deleted {slots_deleted} resolved schedule slots for schedule_id: {schedule_id}")
        
        # Then delete the schedule wrapper
        wrapper_deleted = session.query(ScheduleWrapperOrm).filter(
            ScheduleWrapperOrm.schedule_id == schedule_id
        ).delete()
        
        session.commit()
        
        if wrapper_deleted > 0:
            print(f"✅ Deleted schedule wrapper: {schedule_id}")
            return True
        else:
            print(f"⚠️ Schedule wrapper not found: {schedule_id}")
            return False
    except Exception as e:
        session.rollback()
        print(f"❌ Failed to delete schedule wrapper {schedule_id}: {e}")
        return False
    finally:
        session.close()


def clear_all_schedule_wrappers():
    """
    Clear all schedule wrappers from the database.
    """
    session = get_session()
    try:
        session.query(ScheduleWrapperOrm).delete()
        session.commit()
        print("✅ Cleared all schedule wrappers")
    except Exception as e:
        session.rollback()
        print(f"❌ Failed to clear schedule wrappers: {e}")
        raise e
    finally:
        session.close()