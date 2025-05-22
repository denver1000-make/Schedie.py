import firebase_admin
from firebase_admin import firestore, credentials
import os

SCHEDULE_COLLECTION_PATH = "schedules"
SYSTEM_SETTINGS_PATH = "settings"
CLASS_CANCELLATION_REQUESTS = "classCancellationsRequest"


class FirestoreManager:
    def __init__(self):
        cred = credentials.Certificate(os.getenv("SERVICE_ACCOUNT_PATH"))
        app = firebase_admin.initialize_app(cred)
        self.firestore_db = firestore.client(app=app)

    def get_schedules_collection(self):
        return self.firestore_db.collection(SCHEDULE_COLLECTION_PATH).stream()

    def register_on_snapshot(self, on_snapshot):
        self.firestore_db.collection(CLASS_CANCELLATION_REQUESTS).on_snapshot(on_snapshot)

    def register_on_settings_changed(self, on_snapshot):
        self.firestore_db.collection(SYSTEM_SETTINGS_PATH).on_snapshot(on_snapshot)
