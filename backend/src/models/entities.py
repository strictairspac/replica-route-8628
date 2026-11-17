from sqlalchemy import Column,Integer,String,JSON,DateTime,Text,ForeignKey,Boolean,UniqueConstraint
from datetime import datetime,timezone
from src.config.database import Base
class User(Base):
    __tablename__='users'
    id=Column(Integer,primary_key=True);email=Column(String(120),unique=True,nullable=False);password=Column(String(300),nullable=False);role=Column(String(20),nullable=False)
class Node(Base):
    __tablename__='nodes'
    id=Column(Integer,primary_key=True);name=Column(String(80),unique=True,nullable=False);role=Column(String(20),nullable=False);status=Column(String(20),nullable=False)
    lineage=Column(String(80),nullable=False);position=Column(Integer,nullable=False);delay_ms=Column(Integer,nullable=False);note=Column(String(300),default='')
class RequestSet(Base):
    __tablename__='request_sets'
    id=Column(Integer,primary_key=True);code=Column(String(80),unique=True,nullable=False);title=Column(String(160),nullable=False);events=Column(JSON,nullable=False);note=Column(String(300),default='')
class Trial(Base):
    __tablename__='trials'
    id=Column(Integer,primary_key=True);name=Column(String(120),nullable=False);request_set_id=Column(Integer,ForeignKey('request_sets.id'),nullable=False)
    max_delay_ms=Column(Integer,nullable=False);sticky_ms=Column(Integer,nullable=False);status=Column(String(20),nullable=False,default='queued');input_snapshot=Column(JSON,nullable=False);result=Column(JSON,nullable=True);error=Column(Text,nullable=True)
    created_at=Column(DateTime(timezone=True),default=lambda:datetime.utcnow())
    # Resumable replay: the cursor is the number of events whose decision and replay state are
    # committed together in one transaction. Replay_state is the checkpoint needed to continue.
    total_events=Column(Integer,nullable=False,default=0)
    cursor=Column(Integer,nullable=False,default=0)
    replay_state=Column(JSON,nullable=True)
    epoch=Column(Integer,nullable=False,default=0)
    pause_requested=Column(Boolean,nullable=False,default=False)
    pause_cursor=Column(Integer,nullable=True)
    lease_owner=Column(String(160),nullable=True)
    lease_token=Column(Integer,nullable=False,default=0)
    lease_expires_at=Column(DateTime(timezone=True),nullable=True)
    started_at=Column(DateTime(timezone=True),nullable=True)
    updated_at=Column(DateTime(timezone=True),default=lambda:datetime.utcnow())
    finished_at=Column(DateTime(timezone=True),nullable=True)
class TrialEvent(Base):
    """One committed request/event decision. Unique(trial_id, seq) makes retries idempotent."""
    __tablename__='trial_events'
    __table_args__=(UniqueConstraint('trial_id','seq',name='uq_trial_event_seq'),)
    id=Column(Integer,primary_key=True)
    trial_id=Column(Integer,ForeignKey('trials.id'),nullable=False,index=True)
    seq=Column(Integer,nullable=False)
    epoch=Column(Integer,nullable=False,default=0)
    payload=Column(JSON,nullable=False)
class Audit(Base):
    __tablename__='audit_logs'
    id=Column(Integer,primary_key=True);actor=Column(String(120),nullable=False);action=Column(String(160),nullable=False);created_at=Column(DateTime(timezone=True),default=lambda:datetime.utcnow())
