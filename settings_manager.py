import json

KEY_OF_MINUTE_MARK_JSON = "minute_mark_to_warn"
KEY_OF_MINUTE_GAP_TO_IGNORE_TURN_OFF_JOB = "minute_gap_to_ignore_turn_off_job"


def load_settings(path_of_settings_json: str, setting_key: str) -> int:
    with open(path_of_settings_json, "r") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            data = {}
        if data[setting_key] is None:
            print("No settings were previously saved")
            return -1
        elif data[setting_key] is not None:
            print(f"Settings retrieved, {data[setting_key]}")
            return data[setting_key]


def set_the_minute_to_warn(path_of_setting_json, minute_to_warn, success):
    print(f"Set minute_to_warn to {minute_to_warn}\n")
    with open(path_of_setting_json, "r") as f:
        data = {}
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            data = {}
        data[KEY_OF_MINUTE_MARK_JSON] = minute_to_warn
        with open(path_of_setting_json, "w") as f_for_write:
            json.dump(data, f_for_write, indent=4)
            success(minute_to_warn)
            print(f"Saved.\n")


def set_minute_gap_to_ignore_turn_off_job(path_of_setting_json, gap: int):
    print(f"Set minute_gap to {gap}\n")
    with open(path_of_setting_json, "r") as f:
        data = {}
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            data = {}
        data[KEY_OF_MINUTE_GAP_TO_IGNORE_TURN_OFF_JOB] = gap
        with open(path_of_setting_json, "w") as f_for_write:
            json.dump(data, f_for_write, indent=4)
            print(f"Saved.\n")


