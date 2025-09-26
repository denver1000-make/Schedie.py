

# Constants
SCHEDULE_COLLECTION_PATH = "schedules"
SYSTEM_SETTINGS_PATH = "settings"
CLASS_CANCELLATION_REQUESTS = "classCancellationsRequest"
TEMPORARY_SCHEDULE_COLLECTION = "temporary_schedules"


# def init_schedule() -> FirestoreClient:
#     cred = credentials.Certificate(os.getenv("SERVICE_ACCOUNT_PATH"))
#     firebase_admin.initialize_app(cred)
#     return firestore.client()


# # Firestore access
# def get_schedules_collection(db):
#     return db.collection(SCHEDULE_COLLECTION_PATH).stream()


# def register_schedule_snapshot(db, callback: Callable[[List[DocumentSnapshot], List[DocumentChange], Timestamp], None]):
#     db.collection(SCHEDULE_COLLECTION_PATH).on_snapshot(callback)


# def register_temporary_schedule_handler(db, callback: Callable[
#     [List[DocumentSnapshot], List[DocumentChange], Timestamp], None]):
#     db.collection(TEMPORARY_SCHEDULE_COLLECTION).on_snapshot(callback)


# def process_changes_from_firestore(docs: List[DocumentChange]) -> List[ScheduleV2]:
#     """
#     Process Firestore document changes and convert to ScheduleV2 objects
#     Updated to handle the new data structure from Kotlin app
#     """
#     all_schedules = []

#     for doc in docs:
#         doc_data = doc.document.to_dict()
        
#         # Handle the outer document structure
#         schedules_list = doc_data.get("schedules", [])
        
#         # Process each schedule in the schedules array
#         for schedule_data in schedules_list:
#             room_id = schedule_data.get("roomId")
#             schedule_of_day_map = schedule_data.get("scheduleOfDayMap", [])  # Now it's a list
#             schedule_days = []

#             # Process each day in scheduleOfDayMap
#             for day_data in schedule_of_day_map:
#                 day_name = day_data.get("dayName")
#                 day_order = day_data.get("dayOrder", 0)
#                 raw_hours = day_data.get("hours", [])

#                 time_slots = []
#                 for slot in raw_hours:
#                     # Map the new TimeSlotV2 structure to our existing TimeSlot model
#                     time_slot = TimeSlot(
#                         start_time=slot.get("startTime"),
#                         end_time=slot.get("endTime"),
#                         subject=slot.get("subject"),
#                         teacher=slot.get("teacher"),
#                         teacher_email=slot.get("teacherEmail"),
#                         time_start_in_seconds=int(slot.get("timeStartInSeconds", 0)),
#                         # Map epoch timestamps to datetime objects if they exist
#                         start_time_date=None,  # We can convert startEpochInSeconds if needed
#                         end_time_date=None     # We can convert endEpochInSeconds if needed
#                     )
#                     time_slots.append(time_slot)

#                 # Sort time slots within the day
#                 time_slots.sort(key=lambda t: t.time_start_in_seconds)

#                 schedule_days.append(ScheduleOfDay(
#                     day_name=day_name,
#                     day_order=day_order,
#                     hours=time_slots
#                 ))

#             # Sort days by day_order
#             schedule_days.sort(key=lambda d: d.day_order)

#             all_schedules.append(ScheduleV2(
#                 room_id=room_id,
#                 schedule_days=schedule_days
#             ))

#     return all_schedules


# def process_schedule_from_firestore(docs: List[DocumentSnapshot]) -> List[ScheduleV2]:
#     """
#     Process Firestore document snapshots and convert to ScheduleV2 objects
#     Updated to handle the new data structure from Kotlin app
#     """
#     all_schedules = []

#     for doc in docs:
#         doc_data = doc.to_dict()
#         if not doc_data:
#             continue
            
#         # Handle the outer document structure
#         schedules_list = doc_data.get("schedules", [])
        
#         # Process each schedule in the schedules array
#         for schedule_data in schedules_list:
#             room_id = schedule_data.get("roomId", "Unknown")
#             schedule_of_day_map = schedule_data.get("scheduleOfDayMap", [])  # Now it's a list
#             schedule_days = []

#             # Process each day in scheduleOfDayMap
#             for day_data in schedule_of_day_map:
#                 day_name = day_data.get("dayName", "Unknown")
#                 day_order = day_data.get("dayOrder", 0)
#                 raw_hours = day_data.get("hours", [])

#                 time_slots = []
#                 for slot in raw_hours:
#                     # Map the new TimeSlotV2 structure to our existing TimeSlot model
#                     time_slot = TimeSlot(
#                         start_time=slot.get("startTime", ""),
#                         end_time=slot.get("endTime", ""),
#                         subject=slot.get("subject"),
#                         teacher=slot.get("teacher"),
#                         teacher_email=slot.get("teacherEmail"),
#                         time_start_in_seconds=int(slot.get("timeStartInSeconds", 0))
#                         # Note: startEpochInSeconds and endEpochInSeconds from Kotlin
#                         # are not used in the current TimeSlot model, but we preserve
#                         # the timeslotId information in the processing pipeline
#                     )
#                     time_slots.append(time_slot)

#                 # Sort time slots within the day
#                 time_slots.sort(key=lambda t: t.time_start_in_seconds)

#                 schedule_days.append(ScheduleOfDay(
#                     day_name=day_name,
#                     day_order=day_order,
#                     hours=time_slots
#                 ))

#             # Sort days by day_order
#             schedule_days.sort(key=lambda d: d.day_order)

#             all_schedules.append(ScheduleV2(
#                 room_id=room_id,
#                 schedule_days=schedule_days
#             ))

#     return all_schedules


# def extract_resolved_slots_by_day(schedules: List[ScheduleV2]) -> Dict[str, List[ResolvedScheduleSlot]]:
#     """
#     Extract resolved slots by day from schedules
#     Updated to handle potential None values in datetime fields
#     """
#     resolved_slots_by_day: Dict[str, List[ResolvedScheduleSlot]] = {}
#     for sched in schedules:
#         for day in sched.schedule_days:
#             resolved_list = resolved_slots_by_day.setdefault(day.day_name, [])
#             for slot in day.hours:
#                 # Handle None datetime values by using a default or current time
#                 from datetime import datetime
#                 default_datetime = datetime.now()
                
#                 resolved = ResolvedScheduleSlot(
#                     room_id=sched.room_id,
#                     day_name=day.day_name,
#                     day_order=day.day_order,
#                     start_time=slot.start_time,
#                     end_time=slot.end_time,
#                     subject=slot.subject or "",
#                     teacher=slot.teacher or "",
#                     teacher_email=slot.teacher_email or "",
#                     time_start_in_seconds=slot.time_start_in_seconds,
#                     start_date_in_schedule=slot.start_time_date or default_datetime,
#                     end_date_in_schedule=slot.end_time_date or default_datetime
#                 )
#                 resolved_list.append(resolved)

#     # Optional: sort time slots for each day by time
#     for day, slots in resolved_slots_by_day.items():
#         slots.sort(key=lambda x: x.time_start_in_seconds)

#     return resolved_slots_by_day


# def extract_resolved_slots_v2_from_firestore(docs: List[DocumentSnapshot]) -> List[ResolvedScheduleSlotV2]:
#     """
#     Extract ResolvedScheduleSlotV2 objects directly from Firestore documents
#     This function is optimized for the new data structure with timeslotId
#     """
#     resolved_slots = []
    
#     for doc in docs:
#         doc_data = doc.to_dict()
#         if not doc_data:
#             continue
            
#         # Get document-level metadata
#         schedule_id = doc_data.get("schedule_id", "unknown")
#         is_temporary = doc_data.get("is_temporary", False)
        
#         # Handle the schedules array
#         schedules_list = doc_data.get("schedules", [])
        
#         for schedule_data in schedules_list:
#             room_id = schedule_data.get("roomId", "Unknown")
#             schedule_of_day_map = schedule_data.get("scheduleOfDayMap", [])
            
#             for day_data in schedule_of_day_map:
#                 day_name = day_data.get("dayName", "Unknown")
#                 day_order = day_data.get("dayOrder", 0)
#                 hours = day_data.get("hours", [])
                
#                 for time_slot in hours:
#                     # Create ResolvedScheduleSlotV2 directly with all the new fields
#                     resolved_slot = ResolvedScheduleSlotV2(
#                         timeslot_id=time_slot.get("timeslotId", ""),
#                         room_id=room_id,
#                         day_name=day_name,
#                         day_order=day_order,
#                         start_time=time_slot.get("startTime", ""),
#                         end_time=time_slot.get("endTime", ""),
#                         subject=time_slot.get("subject", ""),
#                         teacher=time_slot.get("teacher", ""),
#                         teacher_email=time_slot.get("teacherEmail", ""),
#                         time_start_in_seconds=int(time_slot.get("timeStartInSeconds", 0)),
#                         start_date_in_seconds_epoch=time_slot.get("startEpochInSeconds"),
#                         end_date_in_seconds_epoch=time_slot.get("endEpochInSeconds"),
#                         is_temporary=time_slot.get("isTemporary", is_temporary)
#                     )
#                     resolved_slots.append(resolved_slot)
    
#     # Sort by day order, then by time
#     resolved_slots.sort(key=lambda x: (x.day_order, x.time_start_in_seconds))
#     return resolved_slots


# def process_schedule_raw_document(doc_data: Dict) -> List[ResolvedScheduleSlotV2]:
#     """
#     Process a single schedule_raw document and return ResolvedScheduleSlotV2 objects
#     This is designed specifically for the schedule_raw collection structure
#     """
#     resolved_slots = []
    
#     # Get document-level metadata
#     schedule_id = doc_data.get("schedule_id", "unknown")
#     is_temporary = doc_data.get("is_temporary", False)
    
#     # Handle the schedules array
#     schedules_list = doc_data.get("schedules", [])
    
#     for schedule_data in schedules_list:
#         room_id = schedule_data.get("roomId", "Unknown")
#         schedule_of_day_map = schedule_data.get("scheduleOfDayMap", [])
        
#         for day_data in schedule_of_day_map:
#             day_name = day_data.get("dayName", "Unknown")
#             day_order = day_data.get("dayOrder", 0)
#             hours = day_data.get("hours", [])
            
#             for time_slot in hours:
#                 # Create ResolvedScheduleSlotV2 directly with all the new fields
#                 resolved_slot = ResolvedScheduleSlotV2(
#                     timeslot_id=time_slot.get("timeslotId", ""),
#                     room_id=room_id,
#                     day_name=day_name,
#                     day_order=day_order,
#                     start_time=time_slot.get("startTime", ""),
#                     end_time=time_slot.get("endTime", ""),
#                     subject=time_slot.get("subject", ""),
#                     teacher=time_slot.get("teacher", ""),
#                     teacher_email=time_slot.get("teacherEmail", ""),
#                     time_start_in_seconds=int(time_slot.get("timeStartInSeconds", 0)),
#                     start_date_in_seconds_epoch=time_slot.get("startEpochInSeconds"),
#                     end_date_in_seconds_epoch=time_slot.get("endEpochInSeconds"),
#                     is_temporary=time_slot.get("isTemporary", is_temporary)
#                 )
#                 resolved_slots.append(resolved_slot)
    
#     # Sort by day order, then by time
#     resolved_slots.sort(key=lambda x: (x.day_order, x.time_start_in_seconds))
#     return resolved_slots
