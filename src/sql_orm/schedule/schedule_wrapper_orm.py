
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