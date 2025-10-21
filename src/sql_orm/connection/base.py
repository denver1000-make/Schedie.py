"""
Shared SQLAlchemy Base for all ORM models.
All ORM models should import Base from here to ensure they use the same declarative base.
"""

import sqlalchemy.orm as sa_orm

# Shared declarative base for all ORM models
Base = sa_orm.declarative_base()