from src.mqtt.mqtt_manager import SCHEDULE_SHUTDOWN_WARNING, SET_ROOM_STATUS, publish_v2
import paho.mqtt.client as mqtt


def mqtt_turn_on(
    room_id: str,
    mqtt_client: mqtt.Client
):
    publish_v2(
        client=mqtt_client,
        topic=f"{SET_ROOM_STATUS}/{room_id}",
        log=True,
        msg=1,
        retain=True
    )
    
def mqtt_turn_off(
    room_id: str,
    mqtt_client: mqtt.Client
): 
    publish_v2(
        client=mqtt_client,
        topic=f"{SET_ROOM_STATUS}/{room_id}",
        log=True,
        msg=0,
        retain=True
    )
    
def send_shutdown_warning(
    room_id: str,
    mqtt_client: mqtt.Client
):
    publish_v2(
        client=mqtt_client,
        topic=f"{SCHEDULE_SHUTDOWN_WARNING}/{room_id}",
        log=True,
        msg=room_id
    )

def send_schedule_ending(
    room_id: str,
    mqtt_client: mqtt.Client
):
    publish_v2(
        client=mqtt_client,
        topic="schedule_ending",
        msg=room_id,
        log=True,
        retain=False,
    )