"""Idempotent in-place upgrade for the resumable-trial columns.

``create_all`` only builds missing tables, so an existing deployment keeps the old ``trials``
shape. This adds the checkpoint/lease columns when absent, creates ``trial_events``, and backfills
old rows so that trials stuck in ``running`` from a previous crash become resumable instead of
staying stuck forever.
"""
from sqlalchemy import inspect,text,select
from src.config.database import Base,engine,Session
from src.models.entities import Trial,TrialEvent

COLUMNS={
    'total_events':'INTEGER NOT NULL DEFAULT 0',
    'cursor':'INTEGER NOT NULL DEFAULT 0',
    'replay_state':'JSON NULL',
    'epoch':'INTEGER NOT NULL DEFAULT 0',
    'pause_requested':'BOOLEAN NOT NULL DEFAULT 0',
    'pause_cursor':'INTEGER NULL',
    'lease_owner':'VARCHAR(160) NULL',
    'lease_token':'INTEGER NOT NULL DEFAULT 0',
    'lease_expires_at':'DATETIME NULL',
    'started_at':'DATETIME NULL',
    'updated_at':'DATETIME NULL',
    'finished_at':'DATETIME NULL',
}


def migrate():
    Base.metadata.create_all(engine)
    inspector=inspect(engine)
    existing={c['name'] for c in inspector.get_columns('trials')}
    with engine.begin() as conn:
        for name,ddl in COLUMNS.items():
            if name not in existing:
                conn.execute(text(f'ALTER TABLE trials ADD COLUMN {name} {ddl}'))
    with Session() as db:
        for obj in db.scalars(select(Trial)):
            snapshot=obj.input_snapshot or {}
            events=(snapshot.get('request_set') or {}).get('events') or []
            changed=False
            if not obj.total_events:
                obj.total_events=len(events);changed=True
            if obj.status=='completed' and obj.result:
                if obj.cursor!=obj.total_events:obj.cursor=obj.total_events;changed=True
            elif obj.status in ('queued','running'):
                # Crashed before this feature existed: restart the replay from its committed cursor
                # (zero here) instead of leaving the trial stuck in "running".
                obj.status='queued';obj.lease_owner=None;obj.lease_expires_at=None
                obj.pause_requested=False;changed=True
            if changed:pass
        db.commit()
    return True
