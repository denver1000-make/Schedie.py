import os
from typing import List, Callable, Dict

import firebase_admin
from firebase_admin import firestore, credentials
from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.firestore_v1.types import DocumentChange
from google.protobuf.internal.well_known_types import Timestamp

from modelsV2.model import ScheduleV2, ScheduleOfDay, TimeSlot, ResolvedScheduleSlot

# Constants
SCHEDULE_COLLECTION_PATH = "schedules"
SYSTEM_SETTINGS_PATH = "settings"
CLASS_CANCELLATION_REQUESTS = "classCancellationsRequest"
TEMPORARY_SCHEDULE_COLLECTION = "temporary_schedules"


def init_schedule():
    cred = credentials.Certificate(os.getenv("SERVICE_ACCOUNT_PATH"))
    firebase_admin.initialize_app(cred)
    db = firestore.client()


# Firestore access
def get_schedules_collection(db):
    return db.collection(SCHEDULE_COLLECTION_PATH).stream()


def register_schedule_snapshot(db, callback: Callable[[List[DocumentSnapshot], List[DocumentChange], Timestamp], None]):
    db.collection(SCHEDULE_COLLECTION_PATH).on_snapshot(callback)


def register_temporary_schedule_handler(db, callback: Callable[
    [List[DocumentSnapshot], List[DocumentChange], Timestamp], None]):
    db.collection(TEMPORARY_SCHEDULE_COLLECTION).on_snapshot(callback)


def process_changes_from_firestore(docs: List[DocumentChange]) -> List[ScheduleV2]:
    all_schedules = []

    for doc in docs:
        doc_data = doc.document.to_dict()
        room_id = doc_data.get("roomId")
        schedule_map = doc_data.get("scheduleOfDayMap", {})
        schedule_days = []

        for day_key, day in schedule_map.items():
            day_name = day.get("dayName")
            day_order = day.get("dayOrder", 0)
            raw_hours = day.get("hours", [])

            time_slots = [
                TimeSlot(
                    start_time=slot.get("startTime"),
                    end_time=slot.get("endTime"),
                    subject=slot.get("subject"),
                    teacher=slot.get("teacher"),
                    teacher_email=slot.get("teacherEmail"),
                    time_start_in_seconds=int(slot.get("timeStartInSeconds", 0)),
                    end_time_date=slot.get("endDateTime"),
                    start_time_date=slot.get("startDateTime"),
                )
                for slot in raw_hours
            ]

            # Sort time slots within the day
            time_slots.sort(key=lambda t: t.time_start_in_seconds)

            schedule_days.append(ScheduleOfDay(
                day_name=day_name,
                day_order=day_order,
                hours=time_slots
            ))

        # Sort days by day_order
        schedule_days.sort(key=lambda d: d.day_order)

        all_schedules.append(ScheduleV2(
            room_id=room_id,
            schedule_days=schedule_days
        ))

    return all_schedules


def process_schedule_from_firestore(docs: List[DocumentSnapshot]) -> List[ScheduleV2]:
    all_schedules = []

    for doc in docs:
        doc_data = doc.to_dict()
        room_id = doc_data.get("roomId")
        schedule_map = doc_data.get("scheduleOfDayMap", {})
        schedule_days = []

        for day_key, day in schedule_map.items():
            day_name = day.get("dayName")
            day_order = day.get("dayOrder", 0)
            raw_hours = day.get("hours", [])

            time_slots = [
                TimeSlot(
                    start_time=slot.get("startTime"),
                    end_time=slot.get("endTime"),
                    subject=slot.get("subject"),
                    teacher=slot.get("teacher"),
                    teacher_email=slot.get("teacherEmail"),
                    time_start_in_seconds=int(slot.get("timeStartInSeconds", 0))
                )
                for slot in raw_hours
            ]

            # Sort time slots within the day
            time_slots.sort(key=lambda t: t.time_start_in_seconds)

            schedule_days.append(ScheduleOfDay(
                day_name=day_name,
                day_order=day_order,
                hours=time_slots
            ))

        # Sort days by day_order
        schedule_days.sort(key=lambda d: d.day_order)

        all_schedules.append(ScheduleV2(
            room_id=room_id,
            schedule_days=schedule_days
        ))

    return all_schedules


def extract_resolved_slots_by_day(schedules: List[ScheduleV2]) -> Dict[str, List[ResolvedScheduleSlot]]:
    resolved_slots_by_day: Dict[str, List[ResolvedScheduleSlot]] = {}
    for sched in schedules:
        for day in sched.schedule_days:
            resolved_list = resolved_slots_by_day.setdefault(day.day_name, [])
            for slot in day.hours:
                resolved = ResolvedScheduleSlot(
                    room_id=sched.room_id,
                    day_name=day.day_name,
                    day_order=day.day_order,
                    start_time=slot.start_time,
                    end_time=slot.end_time,
                    subject=slot.subject or "",
                    teacher=slot.teacher or "",
                    teacher_email=slot.teacher_email or "",
                    time_start_in_seconds=slot.time_start_in_seconds,
                    start_date_in_schedule=slot.start_time_date,
                    end_date_in_schedule=slot.end_time_date
                )
                resolved_list.append(resolved)

    # Optional: sort time slots for each day by time
    for day, slots in resolved_slots_by_day.items():
        slots.sort(key=lambda x: x.time_start_in_seconds)

    return resolved_slots_by_day
