from psycopg2 import pool
import paho.mqtt.client as mqtt

from helper.json_parsing_helper import parse_schedule_json
from helper.postgres_helper import get_schedule_slots_by_id
from modelsV2.model import ResolvedScheduleSlotV2
from schedulerv2.job_funcs import schedule_v3_with_immediate_check
from sql.schedule_sql import pg_get_processed_schedule_info, pg_insert_resolved_slots, pg_is_schedule_processed, pg_mark_schedule_as_processed
pg_conn_pool = None

def temp_schedule_received_mqtt(client: mqtt.Client, message: mqtt.MQTTMessage):
        import json
        import time
        jsonData = json.loads(message.payload)
        schedule_id = jsonData.get("schedule_id", f"temp_{int(time.time())}")
        
        # Check if this temp schedule has already been processed
        if pg_is_schedule_processed(pg_conn_pool, schedule_id):
            processed_info = pg_get_processed_schedule_info(pg_conn_pool, schedule_id)
            print(f"🔄 Temp Schedule {schedule_id} already processed at {processed_info.get('processed_at')}")
            
            return
            
        slots = [ResolvedScheduleSlotV2(**d) for d in jsonData.get("resolved_slots", [])]
        
        # Mark this temp schedule as processed
        pg_mark_schedule_as_processed(pg_conn_pool, schedule_id, {
            "topic": message.topic,
            "slot_count": len(slots),
            "upload_date_epoch": jsonData.get("upload_date_epoch"),
            "is_temp": True
        })
        
        # Insert the resolved slots into the database
        pg_insert_resolved_slots(pg_conn_pool, schedule_id, slots)
        
        print(f"TEMP SCHEDULE RECEIVED ON TOPIC {message.topic}")
        print(f"Processed {len(slots)} temp schedule slots for schedule_id: {schedule_id}")

    
def schedule_received_mqtt(client: mqtt.Client, message: mqtt.MQTTMessage):
    import json
    from datetime import datetime
        
    jsonData = json.loads(message.payload)
    schedule_id = jsonData.get("schedule_id", "unknown")

    saved_schedule = get_schedule_slots_by_id(pg_conn_pool, schedule_id)
    if saved_schedule is not None:
        processed_info = pg_get_processed_schedule_info(
            pg_conn_pool, 
            schedule_id
        )
        print(f"🔄 Schedule {schedule_id} already processed at {processed_info.get('processed_at')}")
        return
        
    slots = parse_schedule_json(
            jsonData=jsonData,
            message=message,
            pg_conn_pool=pg_conn_pool,
            schedule_id=schedule_id
    )
        
    schedule_v3_with_immediate_check(
            list_resolved_slots=slots,
            scheduler_v2=scheduler_v2,
            mqtt_client=mqtt_client,
            pg_conn_pool=pg_conn_pool,
            isTemp=False,
            current_time=datetime.now(tz=DEVICE_TZ)
    )
        