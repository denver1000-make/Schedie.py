"""
Centralized logging configuration for the schedule management system.

This module provides standardized logging with timestamps, function names,
and appropriate log levels for all system components.
"""

import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import Optional, Union


class ScheduleSystemLogger:
    """
    Centralized logger for the schedule management system.
    Provides consistent formatting and handling across all modules.
    """
    
    _loggers = {}
    _configured = False
    
    @classmethod
    def setup_logging(
        cls, 
        log_level: str = "INFO",
        log_file: Optional[str] = None,
        console_output: bool = True
    ) -> None:
        """
        Configure the logging system for the entire application.
        
        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: Optional log file path. If None, uses default location
            console_output: Whether to output logs to console
        """
        if cls._configured:
            return
            
        # Set up log directory
        log_dir = Path("/app/logs")
        log_dir.mkdir(exist_ok=True)
        
        if log_file is None:
            log_file = str(log_dir / "schedule_system.log")
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear any existing handlers
        root_logger.handlers.clear()
        
        # Create formatter with function names and timestamps
        formatter = logging.Formatter(
            fmt='%(asctime)s | %(levelname)-8s | %(name)-25s | %(funcName)-20s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        if console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            console_handler.setLevel(getattr(logging, log_level.upper()))
            root_logger.addHandler(console_handler)
        
        # File handler with rotation
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)  # File gets all levels
        root_logger.addHandler(file_handler)
        
        cls._configured = True
        
        # Log the configuration
        logger = cls.get_logger("logging_config")
        logger.info(f"Logging system configured - Level: {log_level}, File: {log_file}")
    
    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """
        Get a logger for a specific module or component.
        
        Args:
            name: Logger name (typically module name)
            
        Returns:
            Configured logger instance
        """
        if name not in cls._loggers:
            # Ensure logging is configured
            if not cls._configured:
                cls.setup_logging()
            
            logger = logging.getLogger(name)
            cls._loggers[name] = logger
            
        return cls._loggers[name]


def get_mqtt_logger() -> logging.Logger:
    """Get logger specifically for MQTT operations."""
    return ScheduleSystemLogger.get_logger("mqtt_functions")


def get_scheduler_logger() -> logging.Logger:
    """Get logger specifically for scheduler operations."""
    return ScheduleSystemLogger.get_logger("scheduler_v2")


def get_sql_logger() -> logging.Logger:
    """Get logger specifically for SQL operations."""
    return ScheduleSystemLogger.get_logger("sql_operations")


def get_main_logger() -> logging.Logger:
    """Get logger for main application."""
    return ScheduleSystemLogger.get_logger("main_app")


def get_cancellation_logger() -> logging.Logger:
    """Get logger for cancellation operations."""
    return ScheduleSystemLogger.get_logger("cancellation_system")


def log_mqtt_publish(logger: logging.Logger, topic: str, message: str, success: bool = True) -> None:
    """
    Standardized logging for MQTT publish operations.
    
    Args:
        logger: Logger instance to use
        topic: MQTT topic
        message: Message content
        success: Whether the publish was successful
    """
    if success:
        logger.info(f"MQTT_PUBLISH | Topic: {topic} | Message: {message}")
    else:
        logger.error(f"MQTT_PUBLISH_FAILED | Topic: {topic} | Message: {message}")


def log_schedule_operation(
    logger: logging.Logger, 
    operation: str, 
    room_id: str, 
    timeslot_id: Optional[str] = None,
    details: Optional[str] = None
) -> None:
    """
    Standardized logging for schedule operations.
    
    Args:
        logger: Logger instance to use
        operation: Type of operation (turn_on, turn_off, skip, warning, etc.)
        room_id: Room ID
        timeslot_id: Optional timeslot ID
        details: Additional details
    """
    message_parts = [f"SCHEDULE_{operation.upper()}", f"Room: {room_id}"]
    
    if timeslot_id:
        message_parts.append(f"Timeslot: {timeslot_id}")
    
    if details:
        message_parts.append(f"Details: {details}")
        
    message = " | ".join(message_parts)
    logger.info(message)


def log_database_operation(
    logger: logging.Logger,
    operation: str,
    table: str,
    success: bool,
    details: Optional[str] = None,
    error: Optional[Exception] = None
) -> None:
    """
    Standardized logging for database operations.
    
    Args:
        logger: Logger instance to use
        operation: Database operation (INSERT, UPDATE, DELETE, SELECT)
        table: Database table name
        success: Whether operation was successful
        details: Additional details
        error: Exception if operation failed
    """
    message_parts = [f"DB_{operation.upper()}", f"Table: {table}"]
    
    if details:
        message_parts.append(f"Details: {details}")
    
    message = " | ".join(message_parts)
    
    if success:
        logger.info(message)
    else:
        if error:
            logger.error(f"{message} | Error: {error}")
        else:
            logger.error(message)


def log_cancellation_operation(
    logger: logging.Logger,
    operation: str,
    timeslot_id: str,
    cancellation_type: Optional[str] = None,
    success: bool = True,
    details: Optional[str] = None
) -> None:
    """
    Standardized logging for cancellation operations.
    
    Args:
        logger: Logger instance to use
        operation: Cancellation operation (CANCEL, CHECK, VALIDATE)
        timeslot_id: Timeslot ID
        cancellation_type: Type of cancellation (permanent_instance, temporary_complete)
        success: Whether operation was successful
        details: Additional details
    """
    message_parts = [f"CANCELLATION_{operation.upper()}", f"Timeslot: {timeslot_id}"]
    
    if cancellation_type:
        message_parts.append(f"Type: {cancellation_type}")
    
    if details:
        message_parts.append(f"Details: {details}")
        
    message = " | ".join(message_parts)
    
    if success:
        logger.info(message)
    else:
        logger.error(message)


# Initialize logging on module import
ScheduleSystemLogger.setup_logging()