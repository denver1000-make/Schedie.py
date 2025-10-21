
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from typing import Optional

engine: Optional[Engine] = None
SessionFactory: Optional[scoped_session] = None


def get_engine(user: str, password: str, host: str, port: int, database: str) -> Engine:
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(url, pool_size=20, max_overflow=0)
    return engine

def get_session_factory(engine) -> scoped_session:
    return scoped_session(sessionmaker(bind=engine))

def initialize_global_engine(user: str, password: str, host: str, port: int, database: str):
    """Initialize the global engine and session factory with environment variables"""
    global engine, SessionFactory
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    print(f"🔧 Initializing SQLAlchemy engine with URL: postgresql+psycopg2://{user}:***@{host}:{port}/{database}")
    engine = create_engine(url, pool_size=20, max_overflow=0)
    SessionFactory = scoped_session(sessionmaker(bind=engine))
    print(f"✅ Global engine and session factory created successfully")

def get_session() -> Session:
    """Get a session safely, ensuring SessionFactory is initialized"""
    if SessionFactory is None:
        raise RuntimeError("SessionFactory not initialized. Call initialize_global_engine() first.")
    return SessionFactory()
