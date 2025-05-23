import sqlite3

DB_NAME = "system_settings.db"
TABLE_NAME = "settings"

# Keys used for settings
KEY_OF_MINUTE_MARK_JSON = "minute_mark_to_warn"
KEY_OF_MINUTE_GAP_TO_IGNORE_TURN_OFF_JOB = "minute_gap_to_ignore_turn_off_job"

LOG_KEY = "SQL"

conn = sqlite3.connect(DB_NAME, check_same_thread=False)


def create_settings_table():
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            key TEXT PRIMARY KEY,
            value INTEGER
        )
    ''')
    conn.commit()
    print(f"[{LOG_KEY}] Settings table created or exists.")


def set_setting(key: str, value: int):
    cursor = conn.cursor()
    cursor.execute(f'''
        INSERT INTO {TABLE_NAME} (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    ''', (key, value))
    conn.commit()
    conn.close()
    print(f"[{LOG_KEY}] '{key}' updated to {value}")


def get_setting(key: str) -> int:
    cursor = conn.cursor()
    cursor.execute(f'''
        SELECT value FROM {TABLE_NAME} WHERE key = ?
    ''', (key,))
    row = cursor.fetchone()
    conn.close()
    if row:
        print(f"[{LOG_KEY}] Retrieved setting '{key}': {row[0]}")
        return row[0]
    else:
        print(f"[WARNING] Setting '{key}' not found. Returning -1")
        return -1


# Optional: preload default values
def init_default_settings():
    create_settings_table()
    set_setting(KEY_OF_MINUTE_MARK_JSON, 5)
    set_setting(KEY_OF_MINUTE_GAP_TO_IGNORE_TURN_OFF_JOB, 3)
