import sys
import os
import firebase_admin
import paho.mqtt.client as mqtt
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from firebase_admin import firestore, credentials
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
import time
from settings_manager import load_settings, set_the_minute_to_warn, set_minute_gap_to_ignore_turn_off_job, KEY_OF_MINUTE_GAP_TO_IGNORE_TURN_OFF_JOB, KEY_OF_MINUTE_MARK_JSON
from mqtt_manager import MQTTManager, MQTTCallbackManager
from scheduler import ScheduleManager, parse_time, generate_cron_trig
import threading
import SettingsTopic, ScheduleTopic, TURN_ON_BASE_TOPIC, TURN_OFF_BASE_TOPIC from mqtt_topics

class ScheduleInfo:
    def __init__(self, room_id, schedule_start, schedule_end):
        self.room_id = room_id
        self.schedule_start = schedule_start
        self.schedule_end = schedule_end


list_of_scheduled_room_shutdown = []
minute_to_ignore_next_start_job = 1

load_dotenv(dotenv_path=sys.argv[1])
path_of_setting_json = sys.argv[2]

minute_mark_to_warn = load_settings(path_of_setting_json, setting_key=KEY_OF_MINUTE_MARK_JSON)
gap = load_settings(path_of_setting_json, setting_key=KEY_OF_MINUTE_GAP_TO_IGNORE_TURN_OFF_JOB)

mqtt_url = os.getenv("MQTT_URL")
mqtt_username = os.getenv("MQTT_USERNAME")
mqtt_port = os.getenv("MQTT_PORT")
mqtt_pass = os.getenv("MQTT_PASSWORD")

cred = credentials.Certificate(os.getenv("SERVICE_ACCOUNT_PATH"))
app = firebase_admin.initialize_app(cred)

firestoreDb = firestore.client(app=app)

mqtt_callback_manager = MQTTCallbackManager()
mqtt_manager = MQTTManager(
    mqtt_url=mqtt_url,
    mqtt_port=int(mqtt_port),
    mqtt_password=mqtt_pass,
    mqtt_username=mqtt_username)

schedule_manager = ScheduleManager()


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


mqtt_callback_manager.register_callback(topic=ScheduleTopic.SUB_COLLECT_SCHEDULE msg_cbn=on_collect_schedule)
mqtt_callback_manager.register_callback(topic=SettingsTopic.SUB_SET_MINUTE_TO_WARN, msg_cbn=set_minute_mark_to_warn_callback)
mqtt_callback_manager.register_callback(topic=SettingsTopic.SUB_SET_MINUTE_GAP, msg_cbn=set_minute_gap_to_warn_mqtt_callback)
mqtt_manager.set_on_message_received(mqtt_callback_manager.on_message)


def process_schedule():
    docs = firestoreDb.collection("schedules").stream()
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
                        end_time=end_time)

                    schedule_manager.set_turn_off_job(
                        job=turn_off_job,
                        cron=cron_trig_for_stop_job, rm_id=rm_id)

                except ValueError as e:
                    print("Invalid Cron", e)


def turn_on_job(room_id, start_time, end_time):
    print("Turn on job is performed")
    global list_of_scheduled_room_shutdown
    list_of_scheduled_room_shutdown.insert(1, ScheduleInfo(room_id=room_id,
                                                           schedule_start=parse_time(start_time).strftime("%H:%M"),
                                                           schedule_end=parse_time(end_time).strftime("%H:%M")))

    mqtt_manager.publish(topic=TURN_ON_BASE_TOPIC, msg=room_id, log=True)


def turn_off_job(room_id):
    now = datetime.now(timezone.utc)
    print("Turn off job is performed")
    all_jobs = schedule_manager.scheduler.get_jobs()
    for job in all_jobs:
        if job.name == TURN_ON_BASE_TOPIC and job.args[0] == room_id:
            gap = (job.next_run_time - now).total_seconds() / 60.0
            if gap <= minute_to_ignore_next_start_job:
                print(f"SKIPPED OFF JOB FOR ROOM {room_id}, a schedule is close")
                return
    mqtt_manager.publish(topic=TURN_OFF_BASE_TOPIC, msg=room_id, True)


def periodic_warning_job():
    global list_of_scheduled_room_shutdown
    global minute_mark_to_warn
    date_time_of_now = datetime.now()
    if len(list_of_scheduled_room_shutdown) > 0:
        print(f"Length of turn off jobs on the way, {len(list_of_scheduled_room_shutdown)}")
    for v in list_of_scheduled_room_shutdown:
        end_time_from_list = datetime.strptime(v.schedule_end, "%H:%M").time()
        parsed_end_time_from_list = datetime.combine(date=datetime.today().date(), time=end_time_from_list)
        difference = parsed_end_time_from_list - date_time_of_now
        if int(minute_mark_to_warn) != -1:
            seconds = int(minute_mark_to_warn) * 60
            if (float(seconds) * .7) >= float((difference.total_seconds())) <= float(seconds):
                mqtt_manager.publish(f"warning/{v.room_id}", f"to shutdown, in {minute_mark_to_warn}", log=false)
                list_of_scheduled_room_shutdown.remove(v)


if __name__ == "__main__":
    schedule_manager.set_warning_job(periodic_warning_job)
    mqtt_manager.connect_client()
