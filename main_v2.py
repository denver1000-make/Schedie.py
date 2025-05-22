import os
from dotenv import load_dotenv
from sql.schedule_sql import create_tables as make_schedule_table
from sql.settings_sql import create_settings_table as make_settings_table, set_setting
from firestore.firestore_settings import listen_to_settings, init_firestore
import sys
            

if __name__ == "__main__":
    load_dotenv(dotenv_path=sys.argv[1])
    service_acc = os.getenv("SERVICE_ACCOUNT_PATH")
    firestore_db = init_firestore(service_account_path=service_acc)

    make_settings_table()
    make_schedule_table()

    def on_snapshot_for_settings(doc_snapshot, changes, read_time):
        for doc in doc_snapshot:
            print(f"[FIRESTORE] Settings updated: {doc.id}")
            settings = doc.to_dict()
            for key, value in settings.items():
                set_setting(key, value)


    # Start listening to settings changes
    listen_to_settings(firestore_db, on_snapshot=on_snapshot_for_settings)

    # Keep the program running
    print("[SYSTEM] Main loop running. Press Ctrl+C to exit.")
    try:
        import time

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[SYSTEM] Exiting gracefully.")
