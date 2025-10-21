import sqlalchemy as sa
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy import ForeignKey
from src.sql_orm.connection.sqlalchemy_pg import get_session
from src.sql_orm.connection.base import Base
from typing import Optional
import datetime
import sqlalchemy.exc as sa_exception


class CancelledScheduleOrm(Base):
    __tablename__ = 'cancelled_schedules'
    
    # cancellation_id is now the primary key for date-specific cancellations
    cancellation_id: Mapped[str] = mapped_column(sa.String(255), primary_key=True)  # 'id' from JSON
    timeslot_id: Mapped[str] = mapped_column(sa.String(255), ForeignKey('resolved_schedule_slots_v2.timeslot_id'))
    cancellation_type: Mapped[str] = mapped_column(sa.String(50))  # 'permanent_instance' or 'temporary_complete'
    cancelled_at: Mapped[datetime.datetime] = mapped_column(sa.DateTime, default=datetime.datetime.utcnow)
    cancelled_date: Mapped[str] = mapped_column(sa.String(20))  # 'YYYY-MM-DD' for permanent, 'all' for temporary
    reason: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    cancelled_by: Mapped[Optional[str]] = mapped_column(sa.String(255), nullable=True)
    room_id: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)  # 'roomId' from JSON
    teacher_name: Mapped[Optional[str]] = mapped_column(sa.String(255), nullable=True)  # 'teacherName' from JSON
    teacher_id: Mapped[Optional[str]] = mapped_column(sa.String(255), nullable=True)  # 'teacherId' from JSON
    teacher_email: Mapped[Optional[str]] = mapped_column(sa.String(255), nullable=True)  # 'teacherEmail' from JSON
    day_name: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)  # 'day' from JSON
    year: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)  # 'year' from JSON
    month: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)  # 'month' from JSON
    day_of_month: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)  # 'day_of_month' from JSON
    
    # Subject and timing info (duplicated from schedule but useful for historical tracking)
    subject: Mapped[Optional[str]] = mapped_column(sa.String(255), nullable=True)
    start_time: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)  # e.g., "6:24am"
    end_time: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)    # e.g., "6:28am"
    
    # Relationship to ResolvedScheduleSlotOrm (one-to-one since timeslot_id is PK)
    schedule_slot = relationship("ResolvedScheduleSlotOrm", back_populates="cancelled_schedule")
    
    # Add indexes for performance
    __table_args__ = (
        sa.Index('idx_cancelled_schedules_cancelled_at', 'cancelled_at'),
    )

def check_cancellation_id_exists(cancellation_id: str) -> bool:
    """
    Check if a cancellation_id already exists in the database.
    
    Args:
        cancellation_id: The cancellation ID to check
        
    Returns:
        bool: True if cancellation_id exists, False otherwise
    """
    try:
        session = get_session()
        try:
            result = session.query(CancelledScheduleOrm).filter(
                CancelledScheduleOrm.cancellation_id == cancellation_id
            ).first()
            return result is not None
        finally:
            session.close()
    except sa_exception.SQLAlchemyError:
        return False

def check_if_timeslot_cancelled(
    timeslot_id: str
) -> CancelledScheduleOrm | None:
    """
    Legacy function - checks for any cancellation of timeslot without date filtering.
    Consider using check_if_timeslot_cancelled_on_date for date-specific checking.
    """
    try:
        session = get_session()
        try:
            return session.query(
                CancelledScheduleOrm
            ).filter(
                CancelledScheduleOrm.timeslot_id == timeslot_id
            ).first()
        finally:
            session.close()
    except sa_exception.SQLAlchemyError:
        return None


def check_if_timeslot_cancelled_on_date(
    timeslot_id: str,
    date_str: str  # Format: 'YYYY-MM-DD'
) -> CancelledScheduleOrm | None:
    """
    Check if a specific timeslot is cancelled on a specific date.
    This is the preferred function for date-specific cancellation checking.
    
    Args:
        timeslot_id: The timeslot ID to check
        date_str: Date in 'YYYY-MM-DD' format
        
    Returns:
        CancelledScheduleOrm: Cancellation record if found, None otherwise
    """
    try:
        session = get_session()
        try:
            return session.query(
                CancelledScheduleOrm
            ).filter(
                sa.and_(
                    CancelledScheduleOrm.timeslot_id == timeslot_id,
                    CancelledScheduleOrm.cancelled_date == date_str
                )
            ).first()
        finally:
            session.close()
    except sa_exception.SQLAlchemyError:
        return None

def remove_cancelled_info_orm_by_timeslot_id(timeslot_id: str):
    try:
        session = get_session()
        try:
            count = session.query(
                CancelledScheduleOrm
            ).filter(
                CancelledScheduleOrm.timeslot_id == timeslot_id
            ).delete()
            session.commit()
            return count > 0
        finally:
            session.close()
    except sa_exception.SQLAlchemyError as e:
        print(f"Error occurred in lifting the cancellation for timeslot {timeslot_id}")
        print(f"Error {e}")
        return False
    

def insert_cancellation_info(cancellation_info: CancelledScheduleOrm) -> bool:
    session = get_session()
    try:
        session.add(cancellation_info)
        session.commit()
        return True
    except sa_exception.SQLAlchemyError as e:
        session.rollback()
        print(f"Error inserting cancellation info: {e}")
        return False
    finally:
        session.close()

def clear_all_cancelled_schedules():
    """
    Clear all cancelled schedules from the database.
    """
    try:
        session = get_session()
        try:
            session.query(CancelledScheduleOrm).delete()
            session.commit()
            print("✅ Cleared all cancelled schedules")
        finally:
            session.close()
    except sa_exception.SQLAlchemyError as e:
        print(f"❌ Failed to clear cancelled schedules: {e}")
        raise e