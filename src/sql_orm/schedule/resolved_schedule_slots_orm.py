from typing import Any, List, Optional
import sqlalchemy as sa
from sqlalchemy.orm import relationship, Mapped, mapped_column
from src.sql_orm.connection.sqlalchemy_pg import get_session, get_session_factory
from src.sql_orm.connection.base import Base
import sqlalchemy.exc as sa_exception

class ResolvedScheduleSlotOrm(Base):
    __tablename__ = 'resolved_schedule_slots_v2'
    
    timeslot_id: Mapped[str] = mapped_column(sa.String, primary_key=True)
    schedule_id: Mapped[str] = mapped_column(sa.String, sa.ForeignKey('schedule_wrappers_v2.schedule_id'))
    room_id: Mapped[str] = mapped_column(sa.String)
    day_name: Mapped[str] = mapped_column(sa.String)
    day_order: Mapped[int] = mapped_column(sa.Integer)
    start_time: Mapped[str] = mapped_column(sa.String)
    end_time: Mapped[str] = mapped_column(sa.String)
    subject: Mapped[str] = mapped_column(sa.String)
    teacher: Mapped[str] = mapped_column(sa.String)
    teacher_email: Mapped[Optional[str]] = mapped_column(sa.String, nullable=True)
    start_hour: Mapped[int] = mapped_column(sa.Integer)
    start_minute: Mapped[int] = mapped_column(sa.Integer)
    end_hour: Mapped[int] = mapped_column(sa.Integer)
    end_minute: Mapped[int] = mapped_column(sa.Integer)
    time_start_in_seconds: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    start_date_in_seconds_epoch: Mapped[Optional[float]] = mapped_column(sa.Float, nullable=True)
    end_date_in_seconds_epoch: Mapped[Optional[float]] = mapped_column(sa.Float, nullable=True)
    is_temporary: Mapped[bool] = mapped_column(sa.Boolean, default=False)
    
    # Relationships
    running_turn_on_job = relationship("RunningTurnOnJobOrm", back_populates="schedule_slot", uselist=False)
    schedule_wrapper = relationship("ScheduleWrapperOrm", back_populates="schedule_slots")
    cancelled_schedule = relationship("CancelledScheduleOrm", back_populates="schedule_slot", uselist=False)
    
def check_timeslot_id_exists(timeslot_id: str) -> bool:
    """
    Check if a timeslot_id already exists in the database.
    
    Args:
        timeslot_id: The timeslot ID to check
        
    Returns:
        bool: True if timeslot_id exists, False otherwise
    """
    try:
        session = get_session()
        try:
            result = session.query(ResolvedScheduleSlotOrm).filter(
                ResolvedScheduleSlotOrm.timeslot_id == timeslot_id
            ).first()
            return result is not None
        finally:
            session.close()
    except sa_exception.SQLAlchemyError:
        return False

def get_resolved_slots_by_schedule_id(
    schedule_id: str
) -> List[ResolvedScheduleSlotOrm]:
    session = get_session()
    try:
        return session.query(ResolvedScheduleSlotOrm).filter(
            ResolvedScheduleSlotOrm.schedule_id == schedule_id
        ).all()
    finally:
        session.close()

def get_nearby_schedules_for_room_and_day(
    room_id: str,
    current_end_hour: int,
    current_end_minute: int,
    minute_mark_to_skip: int,
    day_name: str
) -> ResolvedScheduleSlotOrm | None:
    current_end_time_minutes = current_end_hour * 60 + current_end_minute
    max_search_time_minutes = current_end_time_minutes + minute_mark_to_skip
    
    session = get_session()
    try:
        schedule_start_minutes = (ResolvedScheduleSlotOrm.start_hour * 60) + ResolvedScheduleSlotOrm.start_minute
        
        return session.query(ResolvedScheduleSlotOrm).filter(
            sa.and_(
                ResolvedScheduleSlotOrm.day_name == day_name,
                ResolvedScheduleSlotOrm.room_id == room_id,
                schedule_start_minutes >= current_end_time_minutes,
                schedule_start_minutes <= max_search_time_minutes
            )
        ).order_by(
            schedule_start_minutes - current_end_time_minutes
        ).first()
    finally:
        session.close()

def insert_resolved_schedule_slot(resolved_slot: ResolvedScheduleSlotOrm):
    """
    Insert a single resolved schedule slot into the database.
    Returns the inserted object or raises an exception if it fails.
    """
    session = get_session()
    try:
        session.add(resolved_slot)
        session.commit()
        return resolved_slot
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def insert_resolved_schedule_slots(resolved_slots: List[ResolvedScheduleSlotOrm]):
    session = get_session()
    try:
        existing_timeslot_ids = set()
        if resolved_slots:
            timeslot_ids = [slot.timeslot_id for slot in resolved_slots]
            existing_slots = session.query(ResolvedScheduleSlotOrm.timeslot_id).filter(
                ResolvedScheduleSlotOrm.timeslot_id.in_(timeslot_ids)
            ).all()
            existing_timeslot_ids = {slot.timeslot_id for slot in existing_slots}
        
        # Filter out slots that already exist
        new_slots = [slot for slot in resolved_slots if slot.timeslot_id not in existing_timeslot_ids]
        
        if len(new_slots) < len(resolved_slots):
            skipped_count = len(resolved_slots) - len(new_slots)
        
        # Only insert new slots
        if new_slots:
            session.add_all(new_slots)
            session.commit()
            return [slot.timeslot_id for slot in new_slots]
        else:
            return []
        
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def clear_all_resolved_schedule_slots():
    """
    Clear all resolved schedule slots from the database.
    """
    session = get_session()
    try:
        session.query(ResolvedScheduleSlotOrm).delete()
        session.commit()
        print("✅ Cleared all resolved schedule slots")
    except Exception as e:
        session.rollback()
        print(f"❌ Failed to clear resolved schedule slots: {e}")
        raise e
    finally:
        session.close()
