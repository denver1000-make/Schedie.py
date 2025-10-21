"""
Dashboard API functions for power usage data.
Provides flexible query interface for external dashboards and reporting tools.
"""
from typing import List, Dict, Optional, Union
from datetime import datetime, date, timedelta
import json

from src.sql_orm.usage.power_usage_orm import (
    PowerUsageOrm,
    get_power_usage_by_room_and_date_range,
    get_power_usage_summary_by_date_range,
    get_hourly_usage_averages,
    get_latest_power_reading
)


def get_usage_data_for_dashboard(
    room_ids: Optional[List[str]] = None,
    start_date: Optional[Union[str, datetime, date]] = None,
    end_date: Optional[Union[str, datetime, date]] = None,
    granularity: str = "hourly",  # "raw", "hourly", "daily", "summary"
    limit: Optional[int] = None
) -> Dict:
    """
    Main dashboard API function for retrieving usage data with flexible filtering.
    
    Args:
        room_ids: List of room IDs to include (None = all rooms)
        start_date: Start date (string in YYYY-MM-DD format, datetime, or date object)
        end_date: End date (string in YYYY-MM-DD format, datetime, or date object)
        granularity: Data granularity - "raw", "hourly", "daily", or "summary"
        limit: Maximum number of records to return (for raw data)
        
    Returns:
        Dict: Formatted data suitable for dashboard consumption
    """
    try:
        # Parse dates
        start_dt, end_dt = _parse_date_range(start_date, end_date)
        
        # Default to last 24 hours if no dates provided
        if not start_dt or not end_dt:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=1)
        
        result = {
            "status": "success",
            "query": {
                "room_ids": room_ids,
                "start_date": start_dt.isoformat(),
                "end_date": end_dt.isoformat(),
                "granularity": granularity,
                "limit": limit
            },
            "data": [],
            "metadata": {
                "total_records": 0,
                "rooms_included": [],
                "date_range_hours": (end_dt - start_dt).total_seconds() / 3600
            }
        }
        
        if granularity == "summary":
            # Get summary statistics per room
            summary_data = get_power_usage_summary_by_date_range(
                start_date=start_dt,
                end_date=end_dt,
                room_ids=room_ids
            )
            
            result["data"] = summary_data
            result["metadata"]["total_records"] = len(summary_data)
            result["metadata"]["rooms_included"] = [item["room_id"] for item in summary_data]
            
        elif granularity == "hourly":
            # Get hourly averages for each room
            if room_ids:
                rooms_to_query = room_ids
            else:
                # Get all rooms that have data in the range
                summary = get_power_usage_summary_by_date_range(start_dt, end_dt)
                rooms_to_query = [item["room_id"] for item in summary]
            
            hourly_data = []
            for room_id in rooms_to_query:
                room_hourly = get_hourly_usage_averages(room_id, start_dt, end_dt)
                for entry in room_hourly:
                    hourly_data.append({
                        "room_id": room_id,
                        "timestamp": entry["timestamp"].isoformat(),
                        "avg_watts": entry["avg_watts"]
                    })
            
            result["data"] = hourly_data
            result["metadata"]["total_records"] = len(hourly_data)
            result["metadata"]["rooms_included"] = rooms_to_query
            
        elif granularity == "raw":
            # Get raw data for each room
            if room_ids:
                rooms_to_query = room_ids
            else:
                # Get all rooms that have data in the range
                summary = get_power_usage_summary_by_date_range(start_dt, end_dt)
                rooms_to_query = [item["room_id"] for item in summary]
            
            raw_data = []
            for room_id in rooms_to_query:
                room_data = get_power_usage_by_room_and_date_range(
                    room_id=room_id,
                    start_date=start_dt,
                    end_date=end_dt,
                    limit=limit
                )
                
                for record in room_data:
                    raw_data.append({
                        "room_id": record.room_id,
                        "timestamp": record.timestamp.isoformat(),
                        "power_watts": record.power_watts,
                        "voltage": record.voltage,
                        "current": record.current,
                        "frequency": record.frequency,
                        "power_factor": record.power_factor
                    })
            
            # Sort by timestamp descending
            raw_data.sort(key=lambda x: x["timestamp"], reverse=True)
            
            # Apply limit if specified
            if limit and len(raw_data) > limit:
                raw_data = raw_data[:limit]
            
            result["data"] = raw_data
            result["metadata"]["total_records"] = len(raw_data)
            result["metadata"]["rooms_included"] = list(set(item["room_id"] for item in raw_data))
            
        return result
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "data": [],
            "metadata": {}
        }


def get_current_power_status() -> Dict:
    """
    Get current power status for all rooms with recent readings.
    
    Returns:
        Dict: Current status for all active rooms
    """
    try:
        # Get summary for last hour to find active rooms
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)
        
        summary_data = get_power_usage_summary_by_date_range(
            start_date=start_time,
            end_date=end_time
        )
        
        current_status = []
        for room_summary in summary_data:
            room_id = room_summary["room_id"]
            
            # Get latest reading for this room
            latest_reading = get_latest_power_reading(room_id)
            
            if latest_reading:
                # Calculate minutes since last reading
                time_diff = datetime.now() - latest_reading.timestamp
                minutes_ago = time_diff.total_seconds() / 60
                
                status_entry = {
                    "room_id": room_id,
                    "current_watts": latest_reading.power_watts,
                    "last_updated": latest_reading.timestamp.isoformat(),
                    "minutes_ago": round(minutes_ago, 1),
                    "status": "active" if minutes_ago < 5 else "stale",
                    "avg_watts_last_hour": room_summary["avg_watts"]
                }
                
                current_status.append(status_entry)
        
        return {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "active_rooms": len([r for r in current_status if r["status"] == "active"]),
            "total_rooms": len(current_status),
            "data": current_status
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "data": []
        }


def get_room_usage_trends(
    room_id: str,
    days: int = 7
) -> Dict:
    """
    Get usage trends for a specific room over the specified number of days.
    
    Args:
        room_id: Room identifier
        days: Number of days to analyze
        
    Returns:
        Dict: Trend analysis data
    """
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Get daily averages
        daily_data = []
        for i in range(days):
            day_start = start_date + timedelta(days=i)
            day_end = day_start + timedelta(days=1)
            
            day_summary = get_power_usage_summary_by_date_range(
                start_date=day_start,
                end_date=day_end,
                room_ids=[room_id]
            )
            
            if day_summary:
                daily_data.append({
                    "date": day_start.date().isoformat(),
                    "avg_watts": day_summary[0]["avg_watts"],
                    "min_watts": day_summary[0]["min_watts"],
                    "max_watts": day_summary[0]["max_watts"],
                    "readings_count": day_summary[0]["readings_count"]
                })
            else:
                daily_data.append({
                    "date": day_start.date().isoformat(),
                    "avg_watts": 0,
                    "min_watts": 0,
                    "max_watts": 0,
                    "readings_count": 0
                })
        
        # Calculate trend metrics
        valid_days = [d for d in daily_data if d["avg_watts"] > 0]
        
        if len(valid_days) >= 2:
            trend_direction = "increasing" if valid_days[-1]["avg_watts"] > valid_days[0]["avg_watts"] else "decreasing"
            avg_change = (valid_days[-1]["avg_watts"] - valid_days[0]["avg_watts"]) / max(valid_days[0]["avg_watts"], 1)
        else:
            trend_direction = "insufficient_data"
            avg_change = 0
        
        return {
            "status": "success",
            "room_id": room_id,
            "analysis_period_days": days,
            "daily_data": daily_data,
            "trend": {
                "direction": trend_direction,
                "avg_change_percent": round(avg_change * 100, 2),
                "valid_days": len(valid_days),
                "total_days": days
            }
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "room_id": room_id,
            "data": []
        }


def _parse_date_range(start_date, end_date):
    """
    Parse various date input formats into datetime objects.
    
    Args:
        start_date: Start date in various formats
        end_date: End date in various formats
        
    Returns:
        tuple: (start_datetime, end_datetime)
    """
    def parse_single_date(date_input):
        if date_input is None:
            return None
        elif isinstance(date_input, datetime):
            return date_input
        elif isinstance(date_input, date):
            return datetime.combine(date_input, datetime.min.time())
        elif isinstance(date_input, str):
            try:
                # Try parsing YYYY-MM-DD format
                return datetime.strptime(date_input, "%Y-%m-%d")
            except ValueError:
                try:
                    # Try parsing YYYY-MM-DD HH:MM:SS format
                    return datetime.strptime(date_input, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    try:
                        # Try parsing ISO format
                        return datetime.fromisoformat(date_input.replace('Z', '+00:00'))
                    except ValueError:
                        raise ValueError(f"Invalid date format: {date_input}")
        else:
            raise ValueError(f"Unsupported date type: {type(date_input)}")
    
    start_dt = parse_single_date(start_date)
    end_dt = parse_single_date(end_date)
    
    return start_dt, end_dt


def export_usage_data_csv(
    room_ids: Optional[List[str]] = None,
    start_date: Optional[Union[str, datetime, date]] = None,
    end_date: Optional[Union[str, datetime, date]] = None,
    granularity: str = "hourly"
) -> str:
    """
    Export usage data as CSV string for dashboard downloads.
    
    Args:
        room_ids: List of room IDs to include
        start_date: Start date
        end_date: End date
        granularity: Data granularity
        
    Returns:
        str: CSV formatted data
    """
    try:
        data = get_usage_data_for_dashboard(
            room_ids=room_ids,
            start_date=start_date,
            end_date=end_date,
            granularity=granularity
        )
        
        if data["status"] != "success" or not data["data"]:
            return "# No data available for the specified criteria\n"
        
        # Generate CSV header based on granularity
        if granularity == "summary":
            header = "room_id,avg_watts,min_watts,max_watts,readings_count,total_watts\n"
            rows = []
            for item in data["data"]:
                rows.append(f"{item['room_id']},{item['avg_watts']},{item['min_watts']},{item['max_watts']},{item['readings_count']},{item['total_watts']}")
        else:
            header = "room_id,timestamp,power_watts\n"
            rows = []
            for item in data["data"]:
                if granularity == "hourly":
                    rows.append(f"{item['room_id']},{item['timestamp']},{item['avg_watts']}")
                else:  # raw
                    rows.append(f"{item['room_id']},{item['timestamp']},{item['power_watts']}")
        
        return header + "\n".join(rows)
        
    except Exception as e:
        return f"# Error generating CSV: {str(e)}\n"