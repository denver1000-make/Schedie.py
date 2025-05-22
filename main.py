import json
import sys
import os
import paho.mqtt.client as mqtt
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
import time
from models import TimeSlot, Cancellation
import settings_manager
from typing import List


from settings_manager import (
    load_settings,
    set_the_minute_to_warn,
    set_minute_gap_to_ignore_turn_off_job,
    KEY_OF_MINUTE_GAP_TO_IGNORE_TURN_OFF_JOB,
    KEY_OF_MINUTE_MARK_JSON,
    JSON_KEY_OF_MINUTE_GAP,
    JSON_KEY_OF_MINUTE_TO_WARN)
from firestore_manager import FirestoreManager

from mqtt_manager import MQTTManager, MQTTCallbackManager
from scheduler import ScheduleManager, parse_time, generate_cron_trig, TURN_ON_JOB, TURN_OFF_JOB, gen_job_name
import threading
from mqtt_topics import SettingsTopic, ScheduleTopic, TURN_ON_BASE_TOPIC, TURN_OFF_BASE_TOPIC
from zoneinfo import ZoneInfo

class ScheduleInfo:
    def __init__(self, room_id, schedule_start, schedule_end, day_name: str):
        self.room_id = room_id
        self.schedule_start = schedule_start
        self.schedule_end = schedule_end
        self.day_name = day_name

my_tz = ZoneInfo("Asia/Manila")
list_of_scheduled_room_shutdown: List[ScheduleInfo] = []
list_of_active_cancellations: List[Cancellation] = []

minute_to_ignore_next_start_job = -1

load_dotenv(dotenv_path=sys.argv[1])
path_of_setting_json = sys.argv[2]

minute_mark_to_warn = load_settings(path_of_setting_json, setting_key=KEY_OF_MINUTE_MARK_JSON)
gap = load_settings(path_of_setting_json, setting_key=KEY_OF_MINUTE_GAP_TO_IGNORE_TURN_OFF_JOB)

mqtt_url = os.getenv("MQTT_URL")
mqtt_username = os.getenv("MQTT_USERNAME")
mqtt_port = os.getenv("MQTT_PORT")
mqtt_pass = os.getenv("MQTT_PASSWORD")


mqtt_callback_manager = MQTTCallbackManager()
mqtt_manager = MQTTManager(
    mqtt_url=mqtt_url,
    mqtt_port=int(mqtt_port),
    mqtt_password=mqtt_pass,
    mqtt_username=mqtt_username)

firestore_manager = FirestoreManager()

schedule_manager = ScheduleManager()

#Infinitely sends the settings for the app to get
def publish_minute_mark_settings():
    global minute_mark_to_warn
    global gap
    mqtt_manager.publish(topic=SettingsTopic.PUBLISH_WARNING_SETTING, msg=minute_mark_to_warn, log=False)
    mqtt_manager.publish(topic=SettingsTopic.PUBLISH_MINUTE_GAP, msg=gap, log=False)



schedule_manager.register_pub_job_for_settings(
    publish_minute_mark_settings
)

def on_collect_schedule(client: mqtt.Client, msg: mqtt.MQTTMessage):
    process_schedule()


def on_minute_mark_to_warn_changed(minute_mark):
    global minute_mark_to_warn
    minute_mark_to_warn = minute_mark


def set_minute_mark_to_warn_callback(client: mqtt.Client, msg: mqtt.MQTTMessage):
    minute_to_warn = int(msg.payload.decode())
    set_the_minute_to_warn(path_of_setting_json=path_of_setting_json, minute_to_warn=minute_to_warn, success=on_minute_mark_to_warn_changed)

def set_minute_gap_to_warn_mqtt_callback(client: mqtt.Client, msg: mqtt.MQTTMessage):
    global gap
    tmp_gap = int(msg.payload.decode())
    set_minute_gap_to_ignore_turn_off_job(gap=tmp_gap, path_of_setting_json=path_of_setting_json)
    gap = tmp_gap


# Gets called when a set setting is published, inserts the decoded data into settings.json
def set_settings_received(client: mqtt.Client, msg: mqtt.MQTTMessage):
    global path_of_setting_json, gap, minute_mark_to_warn
    json_setting_str = msg.payload.decode()
    obj = json.loads(json_setting_str)
    received_min_to_warn = obj.get(JSON_KEY_OF_MINUTE_TO_WARN)
    received_min_gap = obj.get(JSON_KEY_OF_MINUTE_GAP)

    print(f"Minute To Warn { received_min_to_warn }")
    print(f"Minute Gap { received_min_gap }")

    settings_manager.set_the_minute_to_warn(
        path_of_setting_json=path_of_setting_json,
        minute_to_warn=received_min_to_warn,
        success=on_minute_mark_to_warn_changed)

    settings_manager.set_minute_gap_to_ignore_turn_off_job(
        path_of_setting_json=path_of_setting_json,
        gap=received_min_gap)
    gap = received_min_gap
    minute_mark_to_warn = received_min_to_warn


mqtt_callback_manager.register_callback(topic="set_settings", msg_cbn=set_settings_received)
mqtt_callback_manager.register_callback(topic=ScheduleTopic.SUB_COLLECT_SCHEDULE, msg_cbn=on_collect_schedule)
mqtt_callback_manager.register_callback(topic=SettingsTopic.SUB_SET_MINUTE_TO_WARN, msg_cbn=set_minute_mark_to_warn_callback)
mqtt_callback_manager.register_callback(topic=SettingsTopic.SUB_SET_MINUTE_GAP, msg_cbn=set_minute_gap_to_warn_mqtt_callback)

mqtt_manager.set_on_message_received(mqtt_callback_manager.on_message)


def process_schedule():
    docs = firestore_manager.get_schedules_collection()
    schedule_manager.purge_all_jobs()
    schedule_manager.set_warning_job(periodic_warning_job)
    for doc in docs:
        print(doc.get("roomId"))
        rm_id = doc.get("roomId")
        sched = doc.to_dict().get("scheduleOfDayMap")
        for day in sched.values():
            print(day)
            day_name = day.get("dayName")
            print(f"Day Name: {day_name}")
            hours = day.get("hours")
            for hour in hours:
                start_time = hour["startTime"]
                end_time = hour["endTime"]
                formatted_start_time = parse_time(start_time)
                formatted_end_time = parse_time(end_time)
                try:
                    cron_start = generate_cron_trig(formatted_start_time, day_name)
                    cron_end = generate_cron_trig(formatted_end_time, day_name)
                    print(f"Cron Start {cron_start}")
                    print(f"Cron End {cron_end}")
                    cron_trig_for_start_job = CronTrigger.from_crontab(cron_start)
                    cron_trig_for_stop_job = CronTrigger.from_crontab(cron_end)

                    schedule_manager.set_turn_on_job(
                        job=turn_on_job,
                        cron=cron_trig_for_start_job,
                        rm_id=rm_id,
                        start_time=start_time,
                        end_time=end_time,
                        day_name=day_name)

                    schedule_manager.set_turn_off_job(
                        job=turn_off_job,
                        cron=cron_trig_for_stop_job,
                        rm_id=rm_id,
                        start_time=start_time,
                        end_time=end_time,
                        day_name=day_name)

                except ValueError as e:
                    print("Invalid Cron", e)


def on_snapshot(collection_snapshot, changes, read_time):
    cancellations = []
    for doc in collection_snapshot:
        data = doc.to_dict()
        cancellation = Cancellation(
            id=doc.id,
            teacherId=data.get("teacherId", ""),
            teacherName=data.get("teacherName", ""),
            teacherEmail=data.get("teacherEmail", ""),
            day=data.get("day", ""),
            roomId=data.get("roomId", ""),
            timeSlot=TimeSlot(**data.get("timeSlot", {})),
            accepted=data.get("accepted")
        )

        cancellations.append(cancellation)

    for c in cancellations:
        print(f"{c.timeSlot.startTime} {c.accepted}")
        if c.accepted:
            list_of_active_cancellations.append(cancellation)
            print(vars(c))


    print(list_of_active_cancellations)


firestore_manager.register_on_snapshot(on_snapshot)

def turn_on_job(room_id, start_time, end_time, day_name: str):
    print("Turn on job is performed")
    global list_of_scheduled_room_shutdown
    list_of_scheduled_room_shutdown.insert(1, ScheduleInfo(room_id=room_id,
                                                           schedule_start=start_time,
                                                           schedule_end=end_time,
                                                           day_name=day_name))
    for active_cancellation in list_of_active_cancellations:
        active_timeslot = active_cancellation.timeSlot
        if active_timeslot.startTime == start_time and active_timeslot.endTime == end_time:
            list_of_active_cancellations.remove(active_cancellation)
            return
    mqtt_manager.publish(topic=TURN_ON_BASE_TOPIC, msg=room_id, log=True)


def turn_off_job(room_id, start_time, end_time, day_name: str):
    global gap
    now = datetime.now(my_tz)
    print("Turn off job is performed")
    all_jobs = schedule_manager.scheduler.get_jobs()
    is_job_skip = schedule_manager.look_for_job_within_gap_and_job_type(
        job_type=TURN_ON_JOB,
        room_id=room_id,
        end_time=end_time,
        minute_to_ignore_next_start_job=gap,
        day_name=day_name,
        start_time=start_time,
        now=now)
    if not is_job_skip:
        mqtt_manager.publish(topic=TURN_OFF_BASE_TOPIC, msg=room_id, log=True)


def periodic_warning_job():
    global list_of_scheduled_room_shutdown
    global minute_mark_to_warn
    global gap
    date_time_of_now = datetime.now(my_tz)
    for v in list_of_scheduled_room_shutdown:
        end_time_from_list = datetime.strptime(v.schedule_end, "%H:%M%p").time()
        parsed_end_time_from_list = datetime.combine(date=datetime.now(tz=my_tz).date(), time=end_time_from_list, tzinfo=my_tz)
        difference = parsed_end_time_from_list - date_time_of_now
        if int(minute_mark_to_warn) != -1:
            seconds = int(minute_mark_to_warn) * 60
            if (float(seconds) * .7) >= float((difference.total_seconds())) <= float(seconds):
                formatted_time_end = v.schedule_end
                formatted_time_start = v.schedule_start
                is_turn_off_job_close_to_turn_on_job = schedule_manager.look_for_job_within_gap_and_job_type(
                    job_type=TURN_OFF_JOB,
                    start_time=formatted_time_start,
                    end_time=formatted_time_end,
                    room_id=v.room_id,
                    now=date_time_of_now,
                    minute_to_ignore_next_start_job=gap,
                    day_name=v.day_name
                )

                if is_turn_off_job_close_to_turn_on_job:
                    mqtt_manager.publish(f"warning/skipped_turn_off", f"{v.room_id}", log=True)
                else:
                    mqtt_manager.publish(f"warning/{v.room_id}", f"to shutdown, in {minute_mark_to_warn}", log=True)
                list_of_scheduled_room_shutdown.remove(v)


def find_active_cancellation(
        room_id: str,
        start_time: str,
        end_time: str,
        day_name: str
) -> Cancellation | None:
    for cancellation in list_of_active_cancellations:
        ts = cancellation.timeSlot
        if (
                cancellation.roomId == room_id and
                cancellation.day.lower() == day_name.lower() and
                ts.startTime == start_time and
                ts.endTime == end_time
        ):
            return cancellation
    return None


if __name__ == "__main__":
    schedule_manager.set_warning_job(periodic_warning_job)
    mqtt_manager.connect_client()
