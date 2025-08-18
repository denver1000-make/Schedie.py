import firebase_admin
from firebase_admin import credentials, firestore

# Constants
SETTINGS_COLLECTION = "settings"
SETTINGS_DOCUMENT = "system"


def init_firestore(service_account_path: str):
    cred = credentials.Certificate(service_account_path)
    firebase_admin.initialize_app(cred)
    return firestore.client()


def upload_settings(db, settings: dict, collection: str = SETTINGS_COLLECTION, document: str = SETTINGS_DOCUMENT):
    doc_ref = db.collection(collection).document(document)
    doc_ref.set(settings)
    print(f"[INFO] Settings uploaded to {collection}/{document}:\n{settings}")


def fetch_settings(db, collection: str = SETTINGS_COLLECTION, document: str = SETTINGS_DOCUMENT) -> dict:
    doc_ref = db.collection(collection).document(document)
    doc = doc_ref.get()
    if doc.exists:
        settings = doc.to_dict()
        print(f"[INFO] Retrieved settings:\n{settings}")
        return settings
    else:
        print(f"[WARNING] No settings found in {collection}/{document}")
        return {}


def listen_to_settings(db, on_snapshot):
    doc_ref = db.collection(SETTINGS_COLLECTION).document(SETTINGS_DOCUMENT)
    doc_ref.on_snapshot(on_snapshot)
    print("[INFO] Listening for Firestore settings changes...")
