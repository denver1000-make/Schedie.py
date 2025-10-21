
import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, Mapped, mapped_column
from src.sql_orm.connection.base import Base
from src.sql_orm.connection.sqlalchemy_pg import get_session
import sqlalchemy.exc as sa_exception

class RunningTurnOnJobOrm(Base):
    __tablename__ = 'running_turn_on_jobs'
    
    timeslot_id: Mapped[str] = mapped_column(sa.String, ForeignKey('resolved_schedule_slots_v2.timeslot_id'), primary_key=True)
    is_temporary: Mapped[bool] = mapped_column(sa.Boolean, default=False)
    
    # Relationship to ResolvedScheduleSlotOrm
    schedule_slot = relationship("ResolvedScheduleSlotOrm", back_populates="running_turn_on_job")

def insert_running_job(running_job: RunningTurnOnJobOrm) -> bool:
    try:
        session = get_session()
        try:
            session.add(running_job)
            session.commit()
            return True
        finally:
            session.close()
    except sa_exception.SQLAlchemyError as e:
        print(f"Error In Inserting Running Job {e}")
        return False

def running_job_exists(timeslot_id: str) -> bool:
    """
    Check if a running turn-on job exists for the given timeslot_id.
    
    Args:
        timeslot_id: The timeslot ID to check
        
    Returns:
        bool: True if the job exists, False otherwise
    """
    session = get_session()
    try:
        # Method 1: Using exists() - Most efficient for just checking existence
        return session.query(
            session.query(RunningTurnOnJobOrm)
            .filter(RunningTurnOnJobOrm.timeslot_id == timeslot_id)
            .exists()
        ).scalar()
    finally:
        session.close()

def get_running_job(timeslot_id: str) -> RunningTurnOnJobOrm | None:
    """
    Get the running turn-on job for the given timeslot_id.
    
    Args:
        timeslot_id: The timeslot ID to search for
        
    Returns:
        RunningTurnOnJob | None: The job if it exists, None otherwise
    """
    session = get_session()
    try:
        # Method 2: Using first() - Returns the actual object or None
        running_job = session.query(RunningTurnOnJobOrm).filter(
            RunningTurnOnJobOrm.timeslot_id == timeslot_id
        ).first()
        
        # Only expunge if running_job is not None
        if running_job is not None:
            session.expunge(running_job)
        
        return running_job
    finally:
        session.close()

def remove_running_job(timeslot_id: str) -> bool:
    try:
        session = get_session()
        try:
            count = session.query(
                RunningTurnOnJobOrm
            ).filter(
                RunningTurnOnJobOrm.timeslot_id == timeslot_id
            ).delete(
                synchronize_session=False
            )
            session.commit()
            return count>0
        finally:
            session.close()
    except sa_exception.SQLAlchemyError as e:
        print(f"Error Removing Turn On Job: {e}")
        return False

def clear_all_running_turn_on_jobs():
    """
    Clear all running turn on jobs from the database.
    """
    try:
        session = get_session()
        try:
            session.query(RunningTurnOnJobOrm).delete()
            session.commit()
            print("✅ Cleared all running turn on jobs")
        finally:
            session.close()
    except sa_exception.SQLAlchemyError as e:
        print(f"❌ Failed to clear running turn on jobs: {e}")
        raise e
