import os
from datetime import datetime
from typing import List
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.firestore_v1.types import DocumentChange
from google.protobuf.internal.well_known_types import Timestamp

from firestore.firestore_settings import listen_to_settings, init_firestore
from firestore.schedule_firestore import process_schedule_from_firestore, \
    extract_resolved_slots_by_day, register_temporary_schedule_handler, process_changes_from_firestore
from mqtt.mqtt_manager import publish, init_mqtt
from sql.db_connection import get_db_connection
from sql.jobs_sql import (
    init_job_db,
    insert_job,
    clear_all_jobs,
    fetch_job_by_id,
    fetch_jobs_after_start_seconds_for_room_and_day,
    remove_job_by_job_id)
from sql.schedule_sql import (
    create_schedule_tables as make_schedule_table,
    log_job_pair_run,
    clear_all_schedule_data as purge_schedule_data,
    DB_FOR_SCHEDULE_NAME, fetch_all_job_logs)
from sql.settings_sql import create_settings_table as make_settings_table, set_setting

list_of_turn_offs_to_skip: List[str] = []
my_tz = ZoneInfo("Asia/Manila")

from schedulerv2.scheduler_v2 import (init_scheduler,
                                      parse_time,
                                      gen_job_name,
                                      generate_cron_trig, )

import sys

if __name__ == "__main__":
    load_dotenv(dotenv_path=sys.argv[1])
    service_acc = os.getenv("SERVICE_ACCOUNT_PATH")
    firestore_db = init_firestore(service_account_path=service_acc)
    scheduler_v2 = init_scheduler()
    init_job_db()
    mqtt_url = os.getenv("MQTT_URL")
    mqtt_username = os.getenv("MQTT_USERNAME")
    mqtt_port = os.getenv("MQTT_PORT")
    mqtt_pass = os.getenv("MQTT_PASSWORD")
    schedule_db_conn = get_db_connection(DB_FOR_SCHEDULE_NAME)
    init_mqtt(
        mqtt_url=mqtt_url,
        mqtt_port=int(mqtt_port),
        mqtt_username=mqtt_username,
        mqtt_password=mqtt_pass
    )
    make_settings_table()
    make_schedule_table()

    purge_schedule_data()
    clear_all_jobs()
    scheduler_v2.remove_all_jobs()


    def turn_on(room_id: str, day_name: str, start_time: str, end_time: str, job_turn_off_id: str, job_turn_on_id: str):
        publish("turn_on", msg=room_id, log=True)
        log_job_pair_run(job_turn_off_id=job_turn_off_id, job_turn_on_id=job_turn_on_id, room_id=room_id)


    def turn_off(room_id: str, day_name: str, start_time: str, end_time: str, job_turn_off_id: str,
                 job_turn_on_id: str):
        if not list_of_turn_offs_to_skip.__contains__(job_turn_off_id):
            publish("turn_off", msg=room_id, log=True)
        else:
            print("Skipped turn off.")
        scheduler_v2.print_jobs()


    def detect_rooms_to_warn():
        for job_log in fetch_all_job_logs():
            current_job_in_queue = fetch_job_by_id(job_log.job_turn_off_id)
            if current_job_in_queue:
                print("[INFO] Processing job:", current_job_in_queue.end_time)
                time_obj_of_time_end = datetime.strptime(current_job_in_queue.end_time, "%I:%M%p").time()
                corrected_end_time = datetime.combine(date=datetime.now(tz=my_tz).date(),
                                                      time=time_obj_of_time_end,
                                                      tzinfo=my_tz)
                date_time_now = datetime.now(tz=my_tz)
                distance_of_turn_off_to_now = (corrected_end_time - date_time_now).total_seconds() / 60
                # TODO(REPLACE WITH ACTUAL SETTINGS)
                if int(distance_of_turn_off_to_now) == 1:
                    selected_jobs = fetch_jobs_after_start_seconds_for_room_and_day(
                        min_start_seconds=current_job_in_queue.start_seconds,
                        room_id=current_job_in_queue.room_id,
                        day_order=current_job_in_queue.day_order,
                        job_type="turn_on")
                    if len(selected_jobs) > 0:
                        best_match = selected_jobs[0]
                        diff = (best_match.start_seconds - current_job_in_queue.start_seconds) / 60
                        # TODO("Replace 2 with actual settings")
                        if 0 >= diff <= 2:
                            publish("warning/skipped_turn_off", msg=best_match.room_id, log=True)
                            list_of_turn_offs_to_skip.append(current_job_in_queue.end_time)
                        else:
                            publish("warning", msg=best_match.room_id, log=True)
                        remove_job_by_job_id(current_job_in_queue.job_id)


    def schedule(list_of_day=None, day_and_timeslot_dict=None):
        for day in list_of_day:
            resolved_time_slots = day_and_timeslot_dict[day]
            for timeslot in resolved_time_slots:
                parsed_start_time = parse_time(timeslot.start_time)
                parsed_end_time = parse_time(timeslot.end_time)
                cron_turn_on_job = generate_cron_trig(time_arg=parsed_start_time, full_day_name=timeslot.day_name)
                cron_turn_off_job = generate_cron_trig(time_arg=parsed_end_time, full_day_name=timeslot.day_name)
                turn_on_name = gen_job_name(
                    job_type="turn_on",
                    room_id=timeslot.room_id,
                    start_time=timeslot.start_time,
                    end_time=timeslot.end_time,
                    day_name=timeslot.day_name)
                turn_off_name = gen_job_name(job_type="turn_off",
                                             room_id=timeslot.room_id,
                                             start_time=timeslot.start_time,
                                             end_time=timeslot.end_time,
                                             day_name=timeslot.day_name)

                scheduler_v2.add_job(turn_on, trigger=CronTrigger.from_crontab(cron_turn_on_job), args=[
                    timeslot.room_id,
                    timeslot.day_name,
                    timeslot.start_time,
                    timeslot.end_time,
                    turn_off_name,
                    turn_on_name])

                scheduler_v2.add_job(turn_off, trigger=CronTrigger.from_crontab(cron_turn_off_job), args=[
                    timeslot.room_id,
                    timeslot.day_name,
                    timeslot.start_time,
                    timeslot.end_time,
                    turn_off_name,
                    turn_on_name
                ])

                insert_job(
                    room_id=timeslot.room_id,
                    job_type="turn_on",
                    job_id=turn_on_name,
                    end_time=timeslot.end_time,
                    start_time=timeslot.start_time,
                    teacher=timeslot.teacher,
                    day_order=timeslot.day_order,
                    start_seconds=timeslot.time_start_in_seconds,
                    subject=timeslot.subject,
                    teacher_email=timeslot.teacher_email)

                insert_job(
                    room_id=timeslot.room_id,
                    job_type="turn_off",
                    job_id=turn_off_name,
                    end_time=timeslot.end_time,
                    start_time=timeslot.start_time,
                    teacher=timeslot.teacher,
                    day_order=timeslot.day_order,
                    start_seconds=timeslot.time_start_in_seconds,
                    subject=timeslot.subject,
                    teacher_email=timeslot.teacher_email)


    def schedule_temporary(list_of_day=None, day_and_timeslot_dict=None):
        for day in list_of_day:
            resolved_time_slots = day_and_timeslot_dict[day]
            for timeslot in resolved_time_slots:
                # parsed_start_time = parse_time(timeslot.start_time)
                # parsed_end_time = parse_time(timeslot.end_time)
                # cron_turn_on_job = generate_cron_trig(time_arg=parsed_start_time, full_day_name=timeslot.day_name)
                # cron_turn_off_job = generate_cron_trig(time_arg=parsed_end_time, full_day_name=timeslot.day_name)
                turn_on_name = gen_job_name(
                    job_type="turn_on",
                    room_id=timeslot.room_id,
                    start_time=timeslot.start_time,
                    end_time=timeslot.end_time,
                    day_name=timeslot.day_name)
                turn_off_name = gen_job_name(job_type="turn_off",
                                             room_id=timeslot.room_id,
                                             start_time=timeslot.start_time,
                                             end_time=timeslot.end_time,
                                             day_name=timeslot.day_name)

                scheduler_v2.add_job(turn_on, trigger=DateTrigger(run_date=timeslot.start_date_in_schedule), args=[
                    timeslot.room_id,
                    timeslot.day_name,
                    timeslot.start_time,
                    timeslot.end_time,
                    turn_off_name,
                    turn_on_name])

                scheduler_v2.add_job(turn_off, trigger=DateTrigger(run_date=timeslot.end_date_in_schedule), args=[
                    timeslot.room_id,
                    timeslot.day_name,
                    timeslot.start_time,
                    timeslot.end_time,
                    turn_off_name,
                    turn_on_name
                ])

                scheduler_v2.print_jobs()

                insert_job(
                    room_id=timeslot.room_id,
                    job_type="turn_on",
                    job_id=turn_on_name,
                    end_time=timeslot.end_time,
                    start_time=timeslot.start_time,
                    teacher=timeslot.teacher,
                    day_order=timeslot.day_order,
                    start_seconds=timeslot.time_start_in_seconds,
                    subject=timeslot.subject,
                    teacher_email=timeslot.teacher_email)

                insert_job(
                    room_id=timeslot.room_id,
                    job_type="turn_off",
                    job_id=turn_off_name,
                    end_time=timeslot.end_time,
                    start_time=timeslot.start_time,
                    teacher=timeslot.teacher,
                    day_order=timeslot.day_order,
                    start_seconds=timeslot.time_start_in_seconds,
                    subject=timeslot.subject,
                    teacher_email=timeslot.teacher_email)


    def on_snapshot_for_schedule(doc_snapshot: List[DocumentSnapshot], changes: List[DocumentChange],
                                 read_time: Timestamp):
        purge_schedule_data()
        clear_all_jobs()
        scheduler_v2.remove_all_jobs()
        list_of_sched = process_schedule_from_firestore(doc_snapshot)
        day_and_timeslot_dict = extract_resolved_slots_by_day(list_of_sched)
        list_of_day = day_and_timeslot_dict.keys()

        # schedule(list_of_day, day_and_timeslot_dict)
        # for day in list_of_day:
        #     resolved_time_slots = day_and_timeslot_dict[day]
        #     for timeslot in resolved_time_slots:
        #         parsed_start_time = parse_time(timeslot.start_time)
        #         parsed_end_time = parse_time(timeslot.end_time)
        #         cron_turn_on_job = generate_cron_trig(time_arg=parsed_start_time, full_day_name=timeslot.day_name)
        #         cron_turn_off_job = generate_cron_trig(time_arg=parsed_end_time, full_day_name=timeslot.day_name)
        #         turn_on_name = gen_job_name(
        #             job_type="turn_on",
        #             room_id=timeslot.room_id,
        #             start_time=timeslot.start_time,
        #             end_time=timeslot.end_time,
        #             day_name=timeslot.day_name)
        #         turn_off_name = gen_job_name(job_type="turn_off",
        #                                      room_id=timeslot.room_id,
        #                                      start_time=timeslot.start_time,
        #                                      end_time=timeslot.end_time,
        #                                      day_name=timeslot.day_name)
        #         scheduler_v2.add_job(turn_on, trigger=CronTrigger.from_crontab(cron_turn_on_job), args=[
        #             timeslot.room_id,
        #             timeslot.day_name,
        #             timeslot.start_time,
        #             timeslot.end_time,
        #             turn_off_name,
        #             turn_on_name])
        #
        #         scheduler_v2.add_job(turn_off, trigger=CronTrigger.from_crontab(cron_turn_off_job), args=[
        #             timeslot.room_id,
        #             timeslot.day_name,
        #             timeslot.start_time,
        #             timeslot.end_time,
        #             turn_off_name,
        #             turn_on_name
        #         ])
        #
        #         insert_job(
        #             room_id=timeslot.room_id,
        #             job_type="turn_on",
        #             job_id=turn_on_name,
        #             end_time=timeslot.end_time,
        #             start_time=timeslot.start_time,
        #             teacher=timeslot.teacher,
        #             day_order=timeslot.day_order,
        #             start_seconds=timeslot.time_start_in_seconds,
        #             subject=timeslot.subject,
        #             teacher_email=timeslot.teacher_email)
        #
        #         insert_job(
        #             room_id=timeslot.room_id,
        #             job_type="turn_off",
        #             job_id=turn_off_name,
        #             end_time=timeslot.end_time,
        #             start_time=timeslot.start_time,
        #             teacher=timeslot.teacher,
        #             day_order=timeslot.day_order,
        #             start_seconds=timeslot.time_start_in_seconds,
        #             subject=timeslot.subject,
        #             teacher_email=timeslot.teacher_email)


    def on_temporary_schedule_handler(doc_snapshot: List[DocumentSnapshot], changes: List[DocumentChange],
                                      read_time: Timestamp):
        list_of_sched = process_changes_from_firestore(changes)
        day_and_timeslot_dict = extract_resolved_slots_by_day(list_of_sched)
        list_of_day = day_and_timeslot_dict.keys()
        schedule_temporary(list_of_day, day_and_timeslot_dict)
        print(list_of_sched)


    # register_schedule_snapshot(db=firestore_db, callback=on_snapshot_for_schedule)
    register_temporary_schedule_handler(db=firestore_db, callback=on_temporary_schedule_handler)
    scheduler_v2.add_job(detect_rooms_to_warn, trigger=IntervalTrigger(seconds=1))


    def on_snapshot_for_settings(doc_snapshot, changes, read_time):
        for doc in doc_snapshot:
            print(f"[FIRESTORE] Settings updated: {doc.id}")
            settings = doc.to_dict()
            for key, value in settings.items():
                set_setting(key, value)


    listen_to_settings(firestore_db, on_snapshot=on_snapshot_for_settings)

    # Keep the program running
    print("[SYSTEM] Main loop running. Press Ctrl+C to exit.")
    try:
        import time

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[SYSTEM] Exiting gracefully.")
