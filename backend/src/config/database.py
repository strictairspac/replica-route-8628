from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .settings import DATABASE_URL
class Base(DeclarativeBase): pass
engine = create_engine(DATABASE_URL, pool_pre_ping=True, connect_args={'check_same_thread':False} if DATABASE_URL.startswith('sqlite') else {})
Session = sessionmaker(engine, expire_on_commit=False)
def db_session():
    with Session() as db: yield db
