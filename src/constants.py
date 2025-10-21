from zoneinfo import ZoneInfo


DEVICE_TZ = ZoneInfo("Asia/Manila")

# Job naming constants
JOB_TURN_ON_SUFFIX = "_t_on"
JOB_TURN_OFF_SUFFIX = "_t_off"

# Schedule timing constants (centralized)
MINUTE_MARK_TO_WARN = 2  # Minutes before schedule end to send warning
MINUTE_MARK_TO_SKIP = 2  # Minutes threshold for skipping turn off when next schedule starts soon

# Cancellation timing constants
CANCELLATION_DELAY_THRESHOLD = 2  # Minutes threshold to determine if delayed turn-off is needed
CANCELLATION_DELAY_DURATION = 2 # Minutes to delay turn-off after cancellation warning

# Job status constants  
JOB_ONGOING = 1
JOB_STANDBY = 0
JOB_CANCELLED = 2
JOB_REMOVE = -1

class JobStatus:
    JOB_ONGOING = 1
    JOB_STANDBY = 0
    JOB_CANCELLED = 2
    JOB_REMOVE = -1