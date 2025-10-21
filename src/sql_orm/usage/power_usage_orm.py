from typing import List, Optional
import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column
from src.sql_orm.connection.sqlalchemy_pg import get_session
from src.sql_orm.connection.base import Base
from datetime import datetime, date
import sqlalchemy.exc as sa_exception


class PowerUsageOrm(Base):
    """
    TimescaleDB-compatible ORM model for power usage data from ESP32 devices.
    Stores real-time power consumption data for rooms with timestamp indexing.
    """
    __tablename__ = 'power_usage'
    
    # Primary key combination for TimescaleDB hypertable
    timestamp: Mapped[datetime] = mapped_column(sa.DateTime, primary_key=True, default=datetime.utcnow)
    room_id: Mapped[str] = mapped_column(sa.String(50), primary_key=True)
    
    # Power data
    power_watts: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    
    # Optional metadata
    device_id: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    voltage: Mapped[Optional[float]] = mapped_column(sa.Float, nullable=True)
    current: Mapped[Optional[float]] = mapped_column(sa.Float, nullable=True)
    frequency: Mapped[Optional[float]] = mapped_column(sa.Float, nullable=True)
    power_factor: Mapped[Optional[float]] = mapped_column(sa.Float, nullable=True)
    
    # Additional tracking
    created_at: Mapped[datetime] = mapped_column(sa.DateTime, default=datetime.utcnow)


def insert_power_usage(power_usage: PowerUsageOrm) -> bool:
    """
    Insert power usage data into the database.
    
    Args:
        power_usage: PowerUsageOrm object to insert
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        session = get_session()
        try:
            session.add(power_usage)
            session.commit()
            return True
        finally:
            session.close()
    except sa_exception.SQLAlchemyError as e:
        print(f"❌ Failed to insert power usage: {e}")
        return False


def get_power_usage_by_room_and_date_range(
    room_id: str,
    start_date: datetime,
    end_date: datetime,
    limit: Optional[int] = None
) -> List[PowerUsageOrm]:
    """
    Get power usage data for a specific room within a date range.
    
    Args:
        room_id: Room identifier (e.g., "RM301")
        start_date: Start of date range (inclusive)
        end_date: End of date range (inclusive)
        limit: Optional limit on number of records returned
        
    Returns:
        List[PowerUsageOrm]: List of power usage records
    """
    try:
        session = get_session()
        try:
            query = session.query(PowerUsageOrm).filter(
                sa.and_(
                    PowerUsageOrm.room_id == room_id,
                    PowerUsageOrm.timestamp >= start_date,
                    PowerUsageOrm.timestamp <= end_date
                )
            ).order_by(PowerUsageOrm.timestamp.desc())
            
            if limit:
                query = query.limit(limit)
                
            return query.all()
        finally:
            session.close()
    except sa_exception.SQLAlchemyError as e:
        print(f"❌ Failed to query power usage: {e}")
        return []


def get_power_usage_summary_by_date_range(
    start_date: datetime,
    end_date: datetime,
    room_ids: Optional[List[str]] = None
) -> List[dict]:
    """
    Get power usage summary (avg, min, max, total) for rooms within date range.
    
    Args:
        start_date: Start of date range (inclusive)
        end_date: End of date range (inclusive)
        room_ids: Optional list of specific room IDs to include
        
    Returns:
        List[dict]: Summary statistics per room
    """
    try:
        session = get_session()
        try:
            query = session.query(
                PowerUsageOrm.room_id,
                sa.func.avg(PowerUsageOrm.power_watts).label('avg_watts'),
                sa.func.min(PowerUsageOrm.power_watts).label('min_watts'),
                sa.func.max(PowerUsageOrm.power_watts).label('max_watts'),
                sa.func.count(PowerUsageOrm.power_watts).label('readings_count'),
                sa.func.sum(PowerUsageOrm.power_watts).label('total_watts')
            ).filter(
                sa.and_(
                    PowerUsageOrm.timestamp >= start_date,
                    PowerUsageOrm.timestamp <= end_date
                )
            )
            
            if room_ids:
                query = query.filter(PowerUsageOrm.room_id.in_(room_ids))
                
            query = query.group_by(PowerUsageOrm.room_id)
            
            results = query.all()
            
            return [
                {
                    'room_id': result.room_id,
                    'avg_watts': float(result.avg_watts) if result.avg_watts else 0.0,
                    'min_watts': result.min_watts or 0,
                    'max_watts': result.max_watts or 0,
                    'readings_count': result.readings_count or 0,
                    'total_watts': result.total_watts or 0
                }
                for result in results
            ]
        finally:
            session.close()
    except sa_exception.SQLAlchemyError as e:
        print(f"❌ Failed to query power usage summary: {e}")
        return []


def get_hourly_usage_averages(
    room_id: str,
    start_date: datetime,
    end_date: datetime
) -> List[dict]:
    """
    Get hourly average power usage for a room within date range.
    Useful for creating time-series charts.
    
    Args:
        room_id: Room identifier
        start_date: Start of date range
        end_date: End of date range
        
    Returns:
        List[dict]: Hourly averages with timestamps
    """
    try:
        session = get_session()
        try:
            # Group by hour and calculate averages
            query = session.query(
                sa.func.date_trunc('hour', PowerUsageOrm.timestamp).label('hour'),
                sa.func.avg(PowerUsageOrm.power_watts).label('avg_watts')
            ).filter(
                sa.and_(
                    PowerUsageOrm.room_id == room_id,
                    PowerUsageOrm.timestamp >= start_date,
                    PowerUsageOrm.timestamp <= end_date
                )
            ).group_by(
                sa.func.date_trunc('hour', PowerUsageOrm.timestamp)
            ).order_by('hour')
            
            results = query.all()
            
            return [
                {
                    'timestamp': result.hour,
                    'avg_watts': float(result.avg_watts) if result.avg_watts else 0.0
                }
                for result in results
            ]
        finally:
            session.close()
    except sa_exception.SQLAlchemyError as e:
        print(f"❌ Failed to query hourly usage averages: {e}")
        return []


def get_latest_power_reading(room_id: str) -> Optional[PowerUsageOrm]:
    """
    Get the latest power reading for a specific room.
    
    Args:
        room_id: Room identifier
        
    Returns:
        PowerUsageOrm: Latest power reading or None if not found
    """
    try:
        session = get_session()
        try:
            return session.query(PowerUsageOrm).filter(
                PowerUsageOrm.room_id == room_id
            ).order_by(PowerUsageOrm.timestamp.desc()).first()
        finally:
            session.close()
    except sa_exception.SQLAlchemyError as e:
        print(f"❌ Failed to query latest power reading: {e}")
        return None


def clear_old_power_usage_data(days_to_keep: int = 30) -> int:
    """
    Clear power usage data older than specified days.
    Useful for data retention management.
    
    Args:
        days_to_keep: Number of days of data to retain
        
    Returns:
        int: Number of records deleted
    """
    try:
        session = get_session()
        try:
            from datetime import timedelta
            cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
            
            deleted_count = session.query(PowerUsageOrm).filter(
                PowerUsageOrm.timestamp < cutoff_date
            ).delete()
            
            session.commit()
            print(f"✅ Deleted {deleted_count} old power usage records")
            return deleted_count
        finally:
            session.close()
    except sa_exception.SQLAlchemyError as e:
        print(f"❌ Failed to clear old power usage data: {e}")
        return 0