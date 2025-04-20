import sys
import os
import firebase_admin
import paho.mqtt.client as mqtt
from firebase_admin import firestore, credentials, initialize_app
from dotenv import load_dotenv
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import time

scheduler = BackgroundScheduler()
scheduler.start()

load_dotenv(dotenv_path=sys.argv[1])

mqtt_url = os.getenv("MQTT_URL")
mqtt_username = os.getenv("MQTT_USERNAME")
mqtt_port = os.getenv("MQTT_PORT")
mqtt_pass = os.getenv("MQTT_PASSWORD")
cred = credentials.Certificate(os.getenv("SERVICE_ACCOUNT_PATH"))
app = firebase_admin.initialize_app(cred)

firestoreDb = firestore.client(app=app)


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("collect_schedule")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    if msg.topic == "collect_schedule":
        print("Collect Schedule Initiated")
        process_sched()
    else:
        print(msg.topic)


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, transport="websockets")
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.username_pw_set(username=mqtt_username, password=mqtt_pass)

mqttc.connect(mqtt_url, 9001)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.

def process_sched():
    docs = firestoreDb.collection("schedules").stream()
    scheduler.remove_all_jobs()
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

                    scheduler.add_job(
                        slave_action,
                        trigger=cron_trig_for_start_job, args=[rm_id, "turn_on"])

                    scheduler.add_job(
                        slave_action,
                        trigger=cron_trig_for_stop_job, args=[rm_id, "turn_off"])

                except ValueError as e:
                    print("Invalid Cron", e)


def parse_time(raw_time):
    return datetime.strptime(raw_time, "%I:%M%p").time()


def generate_cron_trig(time_arg: time, full_day_name: str) -> str:
    return f"{time_arg.minute} {time_arg.hour} * * {full_day_name[:3].upper()}"


def slave_action(room_id, msg_prepend):
    topic = msg_prepend + "/" + room_id
    mqttc.publish(topic=topic, payload="From Schedie(Python)")


mqttc.loop_forever()
