from typing import Optional
import atexit

import sqlalchemy as sqla
import sqlalchemy.orm

_db: Optional[sqlalchemy.orm.Session] = None

@sqla.event.listens_for(sqla.Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.close()

def setup_db(db_path: str) -> sqla.orm.Session:
    global _db
    engine = sqlalchemy.create_engine(f"sqlite:///{db_path}") # , echo=True
    _db = sqlalchemy.orm.Session(engine)

    from . import schema
    schema.Model.metadata.create_all(engine)
    return _db

def get_db() -> sqlalchemy.orm.Session:
    if not _db:
        raise ValueError("Database session has not been created yet.")
    return _db

def close_db():
    if _db:
        _db.commit()

atexit.register(close_db)