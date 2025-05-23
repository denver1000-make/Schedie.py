import sqlite3


def get_db_connection(db_path: str) -> sqlite3.Connection:
    """
    Initialize and return a persistent SQLite connection.
    Suitable for threaded use with APScheduler.

    Parameters:
        db_path (str): Path to the SQLite database file.

    Returns:
        sqlite3.Connection: An open connection with `check_same_thread=False`.
    """
    conn = sqlite3.connect(db_path, check_same_thread=False)
    print(f"[DB] Connected to {db_path}")
    return conn
