"""
Simple Scheduler Template - Runs a Python function every 5 seconds

This template demonstrates how to set up a basic scheduler using APScheduler
that executes a function at regular intervals. Perfect for testing, monitoring,
or any repetitive tasks.

Features:
- Runs a function every 5 seconds
- Easy to customize timing and function behavior
- Graceful shutdown handling
- Clean logging and documentation

Usage:
    python firestore_offline_test.py
"""

import time
import os
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from src.firestore.firestore_settings import init_firestore
from google.cloud.firestore_v1 import Client as FirestoreClient


def my_scheduled_function(firestore_db: FirestoreClient):
    """
    This is the function that will be executed every 5 seconds.
    
    This function will:
    - Query the Firestore "test" collection
    - Add a new document to the collection
    - Log the operation
    """
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] Scheduled function executed! 🚀")
    
    try:
        # Query the test collection to see current documents
        test_collection = firestore_db.collection("test")
        docs = test_collection.get()
        
        print(f"📊 Found {len(docs)} documents in 'test' collection")
        
        # Add a new document to the test collection
        new_doc_data = {
            "timestamp": current_time,
            "message": "Document added by scheduler",
            "execution_count": len(docs) + 1,
            "scheduler_type": "5_second_interval"
        }
        
        # Add the document
        doc_ref = test_collection.add(new_doc_data)
        print(f"✅ Added new document with ID: {doc_ref[1].id}")
        
        # Optional: Query a specific document or filter
        # Example: Get documents created in the last minute
        # recent_docs = test_collection.where("timestamp", ">=", some_timestamp).get()
        
    except Exception as e:
        print(f"❌ Error interacting with Firestore: {e}")
    

def load_config():
    """Load environment configuration from .env file."""
    env_file = os.getenv("ENV_FILE")
    if env_file is None:
        raise ValueError("ENV_FILE environment variable not found")
    load_dotenv(dotenv_path=env_file)


def get_service_acc_path() -> str:
    """Get the service account path from environment variables."""
    service_acc: str | None = os.getenv("SERVICE_ACCOUNT_PATH")
    if service_acc is None:
        raise ValueError("SERVICE_ACCOUNT_PATH not set")
    return service_acc


def setup_scheduler(firestore_db: FirestoreClient):
    """
    Initialize and configure the scheduler.
    
    Args:
        firestore_db: Initialized Firestore client
    
    Returns:
        BlockingScheduler: Configured scheduler instance
    """
    scheduler = BlockingScheduler()
    
    # Add the job to run every 5 seconds
    scheduler.add_job(
        func=my_scheduled_function,
        trigger=IntervalTrigger(seconds=5),
        args=[firestore_db],  # Pass firestore_db as argument
        id='my_periodic_job',
        name='Periodic Task Every 5 Seconds',
        replace_existing=True
    )
    
    print("✅ Scheduler configured to run every 5 seconds")
    return scheduler


def main():
    """
    Main function to start the scheduler.
    
    This function:
    1. Loads configuration
    2. Initializes Firestore connection
    3. Sets up the scheduler
    4. Starts the scheduler
    5. Handles graceful shutdown on Ctrl+C
    """
    print("🔄 Starting Scheduler Template")
    print("📋 Function will execute every 5 seconds")
    print("� Connecting to Firestore...")
    print("�🛑 Press Ctrl+C to stop")
    print("-" * 50)
    
    scheduler = None
    
    try:
        # Load configuration and initialize Firestore
        load_config()
        service_acc = get_service_acc_path()
        firestore_db = init_firestore(service_account_path=service_acc)
        print("✅ Firestore connection established")
        
        test_collection = firestore_db.collection("test")
        docs = test_collection.get()
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}] Scheduled function executed! 🚀")
        
        print(f"📊 Found {len(docs)} documents in 'test' collection")
        
        # Add a new document to the test collection
        new_doc_data = {
            "timestamp": current_time,
            "message": "Document added by scheduler",
            "execution_count": len(docs) + 1,
            "scheduler_type": "5_second_interval"
        }
        
        # Add the document
        doc_ref = test_collection.add(new_doc_data)
        print(f"✅ Added new document with ID: {doc_ref[1].id}")
        
        # Initialize scheduler with Firestore client
        # scheduler = setup_scheduler(firestore_db)
        
        # Start the scheduler (this will block the main thread)
        # scheduler.start()
        
    except KeyboardInterrupt:
        print("\n🛑 Shutdown signal received")
        if scheduler:
            scheduler.shutdown()
        print("✅ Scheduler stopped gracefully")
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        if scheduler:
            scheduler.shutdown()


if __name__ == "__main__":
    main()