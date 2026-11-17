"""Resumable, fenced trial execution.

Design: the trial row is the single source of truth for progress. ``cursor`` counts events whose
decision and replay state were committed together in one transaction; ``trial_events`` holds those
decisions under a unique ``(trial_id, seq)`` key.

Guarantees:

* **One executor.** Claiming is a conditional UPDATE (compare-and-swap) on the lease; every later
  mutation also CAS-checks ``lease_token`` + ``cursor``. A stale, duplicated or restarted executor
  cannot advance the cursor or overwrite a result.
* **Exactly-once results.** Event, cursor and replay state commit in one transaction. A retried
  event either finds the unique key taken (no second write) or replays the event from the committed
  cursor. Failure before the commit loses nothing because nothing was persisted.
* **Linearisable pause.** The pause endpoint is one locked UPDATE that sets ``status=paused``,
  records ``pause_cursor`` and bumps ``lease_token``. An in-flight decision either committed before
  that update (so it is a pre-pause request) or its CAS fails and it is discarded (so it is decided
  after resume). Requests are therefore never split across a pause boundary.
* **Crash recovery.** An executor that stops heart-beating loses its lease; another worker adopts
  the trial from the last committed cursor. Finished-but-unsaved work is simply recomputed by the
  deterministic engine, so no request is lost and no session/write position is shifted.
"""
import json,os,traceback,uuid
from dataclasses import dataclass
from datetime import datetime,timedelta,timezone
from sqlalchemy import select,update,func
from src.models.entities import Trial,TrialEvent
from src.config.settings import LEASE_SECONDS
from .routing import initial_state,apply_event,finalize_result

RUNNABLE=('queued','running')
# Every claim gets a unique fence: the process id alone would let two threads of one process
# accept each other's stale lease after an expiry.
PROCESS=os.getenv('WORKER_ID','worker')+':'+str(os.getpid())


@dataclass(frozen=True)
class Lease:
    owner:str
    token:int


def now():
    """Naive UTC so leases compare identically on SQLite and MySQL; serialisation re-adds tzinfo."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _aware(value):
    return value if value is None or value.tzinfo else value.replace(tzinfo=timezone.utc)


class DuplicateEventError(RuntimeError):
    """A request decision already exists for this sequence; refuse to compute or write it twice."""


def progress_fields(obj):
    total=int(obj.total_events or 0);cursor=int(obj.cursor or 0)
    return {'cursor':cursor,'total_events':total,'remaining':max(total-cursor,0),
            'percent':(100*cursor//total if total else 0),'epoch':int(obj.epoch or 0),
            'pause_cursor':obj.pause_cursor,
            'lease_expires_at':obj.lease_expires_at.isoformat() if obj.lease_expires_at else None}


def claim(db,ident):
    """Take the execution lease with a conditional UPDATE. Returns the fence or None.

    Only a runnable trial with no unexpired lease can be claimed. On MySQL the UPDATE locks the
    row; on any backend the WHERE clause is re-evaluated, so exactly one claimant increments it.
    """
    obj=db.get(Trial,ident)
    if not obj or obj.status not in RUNNABLE:return None
    # A lease is valid until lease_expires_at. After that any executor may adopt from the cursor;
    # the old executor's next CAS fails because the new claim bumped lease_token.
    owner=PROCESS+':'+uuid.uuid4().hex[:12]
    result=db.execute(update(Trial).where(
        Trial.id==ident,Trial.status.in_(RUNNABLE),
        (Trial.lease_owner.is_(None))|(Trial.lease_expires_at.is_(None))|(Trial.lease_expires_at<=now())
    ).values(lease_owner=owner,lease_token=Trial.lease_token+1,
             lease_expires_at=now()+timedelta(seconds=LEASE_SECONDS),status='running',
             started_at=func.coalesce(Trial.started_at,now()),updated_at=now()
    ).execution_options(synchronize_session=False))
    if result.rowcount!=1:
        db.rollback();return None
    db.commit()
    token=db.scalar(select(Trial.lease_token).where(Trial.id==ident,Trial.lease_owner==owner))
    return Lease(owner,token) if token is not None else None


def checkpoint(db,ident,lease,state,row,total):
    """Persist one decision, the new cursor and the replay state atomically under the fence.

    ``state['index']`` is the count of committed events after this one (1-based for the row), so
    the CAS requires the stored cursor to still be exactly one behind it.
    """
    seq=int(state['index'])
    if seq<1 or seq>total:return None
    previous=seq-1
    state={'nodes':state['nodes'],'sessions':state['sessions'],'last_time':state['last_time'],
           'lineage':state['lineage'],'primary':state['primary'],'index':seq}
    result=db.execute(update(Trial).where(
        Trial.id==ident,Trial.status=='running',Trial.lease_owner==lease.owner,
        Trial.lease_token==lease.token,Trial.cursor==previous
    ).values(cursor=seq,replay_state=state,updated_at=now(),
             lease_expires_at=now()+timedelta(seconds=LEASE_SECONDS)
    ).execution_options(synchronize_session=False))
    if result.rowcount!=1:
        db.rollback();return None
    try:
        db.add(TrialEvent(trial_id=ident,seq=seq,epoch=db.scalar(select(Trial.epoch).where(Trial.id==ident)) or 0,payload=row))
        db.commit()
    except Exception as exc:
        db.rollback()
        if _duplicate(db,ident,seq):raise DuplicateEventError('请求结果已存在，拒绝重复写入') from exc
        raise
    return seq


def _duplicate(db,ident,seq):
    db.rollback()
    return db.scalar(select(TrialEvent.id).where(TrialEvent.trial_id==ident,TrialEvent.seq==seq)) is not None


def finish(db,ident,lease):
    """Fold the committed event rows into the deterministic result and complete exactly once."""
    obj=db.get(Trial,ident)
    if not obj:return None
    total=int(obj.total_events or 0);cursor=int(obj.cursor or 0)
    if cursor<total or obj.lease_token!=lease.token or obj.lease_owner!=lease.owner:return None
    rows=[r.payload for r in db.scalars(select(TrialEvent).where(TrialEvent.trial_id==ident).order_by(TrialEvent.seq))]
    if len(rows)!=total:return None
    result=finalize_result(obj.replay_state,rows,obj.max_delay_ms,obj.sticky_ms)
    done=db.execute(update(Trial).where(
        Trial.id==ident,Trial.status=='running',Trial.lease_owner==lease.owner,Trial.lease_token==lease.token,Trial.cursor==total
    ).values(result=result,status='completed',error=None,lease_owner=None,lease_expires_at=None,
             finished_at=now(),updated_at=now()).execution_options(synchronize_session=False))
    if done.rowcount!=1:
        db.rollback();return None
    db.commit()
    return result


def fail(db,ident,lease,message):
    """Only the current fence owner may fail a trial; terminal and paused rows are never overwritten."""
    result=db.execute(update(Trial).where(
        Trial.id==ident,Trial.status=='running',Trial.lease_owner==lease.owner,Trial.lease_token==lease.token
    ).values(status='failed',error=str(message)[:2000],lease_owner=None,lease_expires_at=None,
             finished_at=now(),updated_at=now()).execution_options(synchronize_session=False))
    if result.rowcount!=1:db.rollback();return None
    db.commit();return True


def pause(db,ident):
    """Synchronously confirm a pause with one atomic conditional UPDATE.

    The same statement freezes ``pause_cursor`` to the committed cursor and bumps the fence token,
    so a checkpoint racing with it either committed first (and is therefore counted as a pre-pause
    request) or its CAS fails afterwards (and its request is decided only after resume). After this
    returns, no further decision can be committed.
    """
    obj=db.get(Trial,ident)
    if not obj:return None,'试验不存在'
    if obj.status in ('completed','failed'):return None,'已结束的试验不能暂停'
    if obj.status=='paused':return obj,None
    result=db.execute(update(Trial).where(Trial.id==ident,Trial.status.in_(('queued','running'))
    ).values(status='paused',pause_requested=True,pause_cursor=Trial.cursor,
             epoch=Trial.epoch+1,lease_token=Trial.lease_token+1,lease_owner=None,
             lease_expires_at=None,updated_at=now()).execution_options(synchronize_session=False))
    if result.rowcount!=1:
        db.rollback()
        current=db.get(Trial,ident)
        if current and current.status=='paused':return current,None
        return None,'已结束的试验不能暂停'
    db.commit()
    return db.get(Trial,ident,populate_existing=True),None


def resume(db,ident,requeue):
    """Resume a paused trial from its committed cursor. Returns (trial, None) or (None, reason).

    The status transition is a single conditional UPDATE, so two concurrent resumes cannot both
    enqueue the trial: only the one that actually moved paused->queued wins.
    """
    obj=db.get(Trial,ident)
    if not obj:return None,'试验不存在'
    result=db.execute(update(Trial).where(Trial.id==ident,Trial.status=='paused'
    ).values(status='queued',pause_requested=False,lease_owner=None,lease_expires_at=None,
             updated_at=now()).execution_options(synchronize_session=False))
    if result.rowcount!=1:
        db.rollback()
        current=db.get(Trial,ident)
        if current and current.status in ('completed','failed'):return None,'已结束的试验不能继续'
        return None,'只有已暂停的试验可以继续'
    db.commit()
    requeue(ident)
    return db.get(Trial,ident,populate_existing=True),None


def recover(db):
    """List runnable trials whose executor died or whose queue item was lost.

    Safe to call from many workers: adoption is fenced by ``claim``, so a duplicate queue item is a
    no-op. Returns the trial ids the caller should re-enqueue.
    """
    recovered=[]
    for obj in db.scalars(select(Trial).where(Trial.status.in_(RUNNABLE))):
        expiry=_aware(obj.lease_expires_at)
        # No owner, a lease without an expiry, or an expired lease all mean nobody is advancing it.
        orphan=(not obj.lease_owner) or expiry is None or expiry<=now()
        if orphan:recovered.append(obj.id)
    db.rollback()
    return recovered


def archive(ident,result):
    try:
        with open(f'/artifacts/route-{ident}.json','w',encoding='utf-8') as handle:
            handle.write(json.dumps(result,ensure_ascii=False,indent=2))
    except OSError:
        pass


def run_trial(ident):
    """Execute one trial to its next stopping point: completed, paused, failed, or leased elsewhere.

    Safe to call repeatedly. The first call advances from the committed cursor; later calls are
    no-ops once the row is terminal, paused, or fenced to another executor.
    """
    from src.config.database import Session
    with Session() as db:
        lease=claim(db,ident)
        if lease is None:return None
    try:
        while True:
            with Session() as db:
                obj=db.get(Trial,ident)
                if not obj:return None
                snapshot=obj.input_snapshot;total=int(obj.total_events or 0);cursor=int(obj.cursor or 0)
                state=obj.replay_state;max_delay=obj.max_delay_ms;sticky=obj.sticky_ms
            if cursor>=total:
                with Session() as db:result=finish(db,ident,lease)
                if result is not None:archive(ident,result);return 'completed'
                return 'paused' if _is_paused(ident) else None
            events=snapshot['request_set']['events']
            if not state:state=initial_state(snapshot['nodes'],events,max_delay,sticky)
            row=apply_event(state,events[cursor],max_delay,sticky)
            with Session() as db:advanced=checkpoint(db,ident,lease,state,row,total)
            if advanced is None:
                return 'paused' if _is_paused(ident) else None
    except DuplicateEventError:
        # Another executor persisted this exact request; stop rather than write it twice.
        return None
    except Exception as exc:  # noqa: BLE001 - any failure becomes a visible failed trial
        traceback.print_exc()
        from src.config.database import Session
        with Session() as db:fail(db,ident,lease,exc)
        return 'failed'


def _is_paused(ident):
    from src.config.database import Session
    with Session() as db:
        return db.scalar(select(Trial.status).where(Trial.id==ident))=='paused'
