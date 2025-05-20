from dataclasses import dataclass
from typing import Optional
from dataclasses import dataclass, field


@dataclass
class TimeSlot:
    startTime: Optional[str] = None
    endTime: Optional[str] = None
    subject: Optional[str] = None
    teacher: Optional[str] = None
    teacherEmail: Optional[str] = None
    timeStartInSeconds: Optional[int] = None


@dataclass
class Cancellation:
    id: str = ""  # this is the Firestore document ID
    teacherId: str = ""
    teacherName: str = ""
    teacherEmail: str = ""
    day: str = ""
    roomId: str = ""
    timeSlot: TimeSlot = field(default_factory=TimeSlot)
    isAccepted: Optional[bool] = None  # null means pending
