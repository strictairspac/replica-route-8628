"""Real tests for resumable trial execution.

They run the production services against a file-backed SQLite database and a fake queue, so the
same SQL paths (conditional UPDATE fences, unique (trial_id, seq), one-transaction checkpoints)
are exercised without Docker. The routing engine itself is compared against the original one-shot
simulation to prove resumption produces byte-identical decisions and checksums.
"""
import json,os,threading,time
import pytest
from sqlalchemy import inspect,select

DB='/tmp/rtest.db'


@pytest.fixture()
def app(tmp_path,monkeypatch):
    path=str(tmp_path/'test.db')
    monkeypatch.setenv('DATABASE_URL','sqlite:///'+path)
    monkeypatch.setenv('SEED_DEMO','false')
    monkeypatch.setenv('REDIS_URL','redis://localhost:6399/0')
    monkeypatch.setenv('LEASE_SECONDS','2')
    monkeypatch.setenv('RECOVERY_INTERVAL_SECONDS','0.1')
    # Re-import every module against the isolated database.
    for name in [m for m in list(__import__('sys').modules) if m=='src' or m.startswith('src.') or m=='scripts' or m.startswith('scripts.')]:
        del __import__('sys').modules[name]
    from src.config.database import Base,engine,Session
    from src.config.migrate import migrate
    from src import worker
    migrate()
    from src.models.entities import User,Node,RequestSet
    from src.utils.security import password_hash
    with Session() as db:
        db.add(User(email='admin@qiyuan.test',password=password_hash('Demo2026!'),role='admin'))
        db.add(Node(name='harbor-primary',role='primary',status='healthy',lineage='qiyuan-log-01',position=105,delay_ms=0))
        db.add(Node(name='north-replica',role='replica',status='healthy',lineage='qiyuan-log-01',position=104,delay_ms=150))
        db.add(Node(name='south-replica',role='replica',status='healthy',lineage='qiyuan-log-01',position=105,delay_ms=20))
        db.add(RequestSet(code='QY-1117-A',title='写后读与节点故障',events=[
            {'at_ms':0,'kind':'write','session':'lab-A'},
            {'at_ms':100,'kind':'read','session':'lab-A'},
            {'at_ms':600,'kind':'replicate','node':'south-replica','position':106,'delay_ms':20},
            {'at_ms':700,'kind':'read','session':'lab-A'},
            {'at_ms':800,'kind':'node_down','node':'south-replica'},
            {'at_ms':900,'kind':'read','session':'lab-A'},
            {'at_ms':1000,'kind':'node_down','node':'harbor-primary'},
            {'at_ms':1100,'kind':'read','session':'lab-A'},
        ],note=''))
        db.commit()
    return {'path':path,'Session':Session,'engine':engine,'worker':worker}


def create_trial(Session,events_code='QY-1117-A',name='t'):
    from src.models.entities import RequestSet,Trial
    from src.utils.serialization import record
    from src.services.routing import simulate
    from sqlalchemy import select
    with Session() as db:
        req=db.scalar(select(RequestSet).where(RequestSet.code==events_code))
        from src.models.entities import Node
        nodes=[record(x) for x in db.scalars(select(Node).order_by(Node.id))]
        snapshot={'nodes':nodes,'request_set':record(req)}
        simulate(nodes,req.events,100,500)
        obj=Trial(name=name,request_set_id=req.id,max_delay_ms=100,sticky_ms=500,status='queued',
                  input_snapshot=snapshot,total_events=len(req.events))
        db.add(obj);db.commit();db.refresh(obj);return obj.id


def make_app(app):
    """A FastAPI app wired to the isolated DB with a no-op queue consumer."""
    from src.main import app as fastapi_app
    from src.routes import trials as routes
    from src.services import trials as service
    service.queue=type('Q',(),{'rpush':staticmethod(lambda *a:1),'blpop':staticmethod(lambda *a:None)})()
    return fastapi_app


def client(app):
    from fastapi.testclient import TestClient
    from src.main import app as fastapi_app
    with TestClient(fastapi_app) as c:
        token=c.post('/auth/login',json={'email':'admin@qiyuan.test','password':'Demo2026!'}).json()['token']
        c.headers.update({'Authorization':'Bearer '+token})
        return c


# ---------------------------------------------------------------- write-then-read

def test_write_then_read_is_preserved_and_checksum_matches_original(app):
    from src.services.execution import run_trial
    from src.services.routing import simulate
    from src.models.entities import Node,RequestSet,Trial
    from src.utils.serialization import record
    ident=create_trial(app['Session'])
    assert run_trial(ident)=='completed'
    with app['Session']() as db:
        obj=db.get(Trial,ident)
        snapshot=obj.input_snapshot
        expected=simulate(snapshot['nodes'],snapshot['request_set']['events'],100,500)
        assert obj.result['checksum']==expected['checksum']
        assert [t['decision'] for t in obj.result['trace']]==[t['decision'] for t in expected['trace']]
        assert obj.result['summary']['all_routed_reads_safe'] is True
        assert obj.cursor==obj.total_events==8
        # 100ms 写后读保持主库；700ms 南站；900ms 回主库；1100ms 拒绝
        assert obj.result['trace'][1]['node']=='harbor-primary'
        assert obj.result['trace'][3]['node']=='south-replica'
        assert obj.result['trace'][5]['node']=='harbor-primary'
        assert obj.result['trace'][7]['decision']=='rejected'


def test_retry_does_not_duplicate_results(app):
    from src.services.execution import run_trial,checkpoint,claim
    from src.models.entities import TrialEvent,Trial
    from sqlalchemy import select,func
    ident=create_trial(app['Session'])
    assert run_trial(ident)=='completed'
    assert run_trial(ident) is None          # terminal: second call is a no-op
    with app['Session']() as db:
        rows=db.scalars(select(TrialEvent).where(TrialEvent.trial_id==ident)).all()
        assert len(rows)==8
        assert len({r.seq for r in rows})==8
        # The unique key itself refuses a duplicate write.
        db.add(TrialEvent(trial_id=ident,seq=1,epoch=0,payload={}))
        with pytest.raises(Exception):db.commit()


def test_stale_token_cannot_advance(app):
    from src.services.execution import claim,checkpoint
    from src.services.routing import initial_state
    from src.models.entities import Trial
    from sqlalchemy import select
    ident=create_trial(app['Session'])
    with app['Session']() as db:lease=claim(db,ident)
    assert lease is not None
    with app['Session']() as db:
        obj=db.get(Trial,ident);events=obj.input_snapshot['request_set']['events']
        assert claim(db,ident) is None                     # lease already held
        state=initial_state(obj.input_snapshot['nodes'],events,100,500)
        state['index']=0
        # A checkpoint under a wrong fence token must be rejected.
        assert checkpoint(db,ident,lease.__class__(lease.owner,lease.token+99),state,{'index':1},8) is None
    with app['Session']() as db:assert db.get(Trial,ident).cursor==0


# ---------------------------------------------------------------- pause race

def test_pause_boundary_is_exact_and_resume_continues(app):
    from src.services.execution import run_trial,pause,resume
    from src.models.entities import Trial,TrialEvent
    from sqlalchemy import select
    ident=create_trial(app['Session'])
    with app['Session']() as db:
        obj,err=pause(db,ident)
        assert err is None and obj.status=='paused' and obj.pause_cursor==0
    # A paused trial is not claimable: no executor may advance it until resume.
    from src.services.execution import claim
    with app['Session']() as db:assert claim(db,ident) is None
    assert run_trial(ident) is None
    with app['Session']() as db:
        assert db.scalar(select(Trial.cursor).where(Trial.id==ident))==0
        assert db.scalars(select(TrialEvent).where(TrialEvent.trial_id==ident)).all()==[]
    queued=[]
    with app['Session']() as db:
        obj,err=resume(db,ident,queued.append)
        assert err is None and obj.status=='queued' and obj.epoch==1
    assert queued==[ident]
    assert run_trial(ident)=='completed'
    with app['Session']() as db:
        obj=db.get(Trial,ident)
        assert obj.status=='completed' and obj.cursor==8
        rows=db.scalars(select(TrialEvent).where(TrialEvent.trial_id==ident).order_by(TrialEvent.seq)).all()
        assert [r.seq for r in rows]==list(range(1,9))
        assert all(r.epoch==1 for r in rows)           # all decisions belong to the resumed run


def test_pause_race_leaves_no_split_request(app):
    """Pause while events are being applied: the boundary request is either fully before the pause
    or not committed at all, and the final result still equals the deterministic simulation."""
    from src.services.execution import run_trial,pause,resume
    from src.services.routing import simulate
    from src.models.entities import Trial,TrialEvent
    from sqlalchemy import select
    ident=create_trial(app['Session'])
    results=[]
    t=threading.Thread(target=lambda:results.append(run_trial(ident)))
    t.start()
    time.sleep(0.001)
    with app['Session']() as db:pause(db,ident)
    t.join(10)
    with app['Session']() as db:
        obj=db.get(Trial,ident)
        cursor=obj.cursor
        rows=[r.seq for r in db.scalars(select(TrialEvent).where(TrialEvent.trial_id==ident).order_by(TrialEvent.seq))]
        assert rows==list(range(1,cursor+1))          # contiguous, no gaps, no duplicates
        if obj.status=='paused':assert obj.pause_cursor==cursor
        elif obj.status=='completed':assert cursor==obj.total_events
    with app['Session']() as db:
        if db.get(Trial,ident).status=='paused':db.get(Trial,ident).pause_requested=False;db.get(Trial,ident).status='queued';db.commit()
    if results and results[0]=='paused':
        with app['Session']() as db:resume(db,ident,lambda i:None)
        assert run_trial(ident)=='completed'
    with app['Session']() as db:
        obj=db.get(Trial,ident);snap=obj.input_snapshot
        expected=simulate(snap['nodes'],snap['request_set']['events'],100,500)
        assert obj.result['checksum']==expected['checksum']


def test_pause_never_overwrites_terminal_state(app):
    from src.services.execution import run_trial,pause,resume
    from src.models.entities import Trial
    ident=create_trial(app['Session'])
    assert run_trial(ident)=='completed'
    with app['Session']() as db:
        obj,err=pause(db,ident)
        assert obj is None and '结束' in err
        obj,err=resume(db,ident,lambda i:None)
        assert obj is None and '结束' in err
    with app['Session']() as db:assert db.get(Trial,ident).status=='completed'


# ---------------------------------------------------------------- crash recovery

def test_crash_after_compute_before_commit_resumes_without_loss(app):
    from src.services import execution
    from src.services.execution import run_trial
    from src.services.routing import simulate
    from src.models.entities import Trial,TrialEvent
    from sqlalchemy import select
    ident=create_trial(app['Session'])
    real=execution.checkpoint
    calls={'n':0}
    def flaky(db,ident2,token,state,row,total):
        calls['n']+=1
        if calls['n']==5:
            db.rollback()                    # simulate the process dying before this commit
            return None
        return real(db,ident2,token,state,row,total)
    execution.checkpoint=flaky
    try:assert run_trial(ident) is None      # died mid-replay
    finally:execution.checkpoint=real
    with app['Session']() as db:
        obj=db.get(Trial,ident)
        assert obj.status=='running' and obj.cursor==4 and obj.result is None
        assert len(db.scalars(select(TrialEvent).where(TrialEvent.trial_id==ident)).all())==4
    # The lease of the dead executor is released by expiry; a new worker adopts the trial.
    with app['Session']() as db:
        obj=db.get(Trial,ident);obj.lease_expires_at=None;db.commit()
    assert run_trial(ident)=='completed'
    with app['Session']() as db:
        obj=db.get(Trial,ident);rows=db.scalars(select(TrialEvent).where(TrialEvent.trial_id==ident).order_by(TrialEvent.seq)).all()
        assert [r.seq for r in rows]==list(range(1,9))
        expected=simulate(obj.input_snapshot['nodes'],obj.input_snapshot['request_set']['events'],100,500)
        assert obj.result['checksum']==expected['checksum']
        assert obj.result['sessions']==expected['sessions']


def test_snapshot_is_immune_to_later_node_and_request_edits(app):
    from src.services.execution import run_trial,pause,resume
    from src.models.entities import Node,RequestSet,Trial
    from sqlalchemy import select
    ident=create_trial(app['Session'])
    with app['Session']() as db:pause(db,ident)
    with app['Session']() as db:
        db.scalar(select(Node).where(Node.name=='south-replica')).position=1
        db.scalar(select(Node).where(Node.name=='south-replica')).delay_ms=9999
        db.scalar(select(RequestSet).where(RequestSet.code=='QY-1117-A')).events=[{'at_ms':0,'kind':'node_down','node':'harbor-primary'}]
        db.commit()
    with app['Session']() as db:resume(db,ident,lambda i:None)
    assert run_trial(ident)=='completed'
    with app['Session']() as db:
        obj=db.get(Trial,ident)
        assert obj.result['trace'][3]['node']=='south-replica'   # frozen snapshot still used
        assert obj.result['summary']['requests']==5


def test_progress_is_visible_and_old_trials_stay_readable(app):
    from src.services.execution import run_trial,pause,resume
    from src.services import trials as service
    from src.models.entities import Trial
    ident=create_trial(app['Session'])
    assert run_trial(ident)=='completed'
    with app['Session']() as db:
        view=service.detail(db,ident)
        assert view['status']=='completed' and view['percent']==100 and view['total_events']==8
        assert view['result']['summary']['requests']==5
        assert service.listing(db)[0]['id']==ident
    # A paused trial exposes progress plus only committed evidence.
    ident2=create_trial(app['Session'],name='t2')
    with app['Session']() as db:pause(db,ident2)
    with app['Session']() as db:
        view=service.detail(db,ident2)
        assert view['status']=='paused' and view['percent']==0 and view['partial_trace']==[]


# ---------------------------------------------------------------- concurrency stress

def test_only_one_executor_advances_under_concurrent_takeover(app):
    """Several workers race to run the same trial; the cursor must move exactly once per event."""
    import concurrent.futures as cf
    from src.services.execution import run_trial
    from src.models.entities import Trial,TrialEvent
    from sqlalchemy import select
    ident=create_trial(app['Session'])
    with cf.ThreadPoolExecutor(max_workers=6) as pool:
        outcomes=list(pool.map(lambda _:run_trial(ident),range(6)))
    # At most one thread reached a terminal state; the others were fenced out or found it done.
    assert outcomes.count('completed')<=1
    with app['Session']() as db:
        obj=db.get(Trial,ident)
        rows=[r.seq for r in db.scalars(select(TrialEvent).where(TrialEvent.trial_id==ident).order_by(TrialEvent.seq))]
        if obj.status=='completed':
            assert rows==list(range(1,9))
        else:assert rows==list(range(1,obj.cursor+1))
    # A later recovery run always converges to a complete, non-duplicated result.
    with app['Session']() as db:
        obj=db.get(Trial,ident);obj.lease_expires_at=None;db.commit()
    assert run_trial(ident) in ('completed',None)
    with app['Session']() as db:
        obj=db.get(Trial,ident)
        if obj.status!='completed':
            obj.lease_expires_at=None;db.commit()
            assert run_trial(ident)=='completed'
    with app['Session']() as db:
        obj=db.get(Trial,ident)
        assert obj.status=='completed' and obj.result['trace'][3]['node']=='south-replica'
        rows=db.scalars(select(TrialEvent).where(TrialEvent.trial_id==ident)).all()
        assert len(rows)==8 and len({r.seq for r in rows})==8


def test_repeated_pause_resume_cycles_keep_one_decision_per_request(app):
    from src.services.execution import run_trial,pause,resume,claim
    from src.services.routing import simulate
    from src.models.entities import Trial,TrialEvent
    from sqlalchemy import select
    ident=create_trial(app['Session'])
    for _ in range(3):
        with app['Session']() as db:
            obj,err=pause(db,ident)
        if err:break
        with app['Session']() as db:resume(db,ident,lambda i:None)
        run_trial(ident)
    with app['Session']() as db:
        obj=db.get(Trial,ident)
        if obj.status!='completed':
            obj.lease_expires_at=None;db.commit()
            assert run_trial(ident)=='completed'
    with app['Session']() as db:
        obj=db.get(Trial,ident);snap=obj.input_snapshot
        expected=simulate(snap['nodes'],snap['request_set']['events'],100,500)
        rows=db.scalars(select(TrialEvent).where(TrialEvent.trial_id==ident).order_by(TrialEvent.seq)).all()
        assert len(rows)==8 and len({r.seq for r in rows})==8
        assert obj.result['checksum']==expected['checksum']
        assert all(a.payload==b for a,b in zip(rows,expected['trace']))


def test_crash_between_finish_and_commit_is_deterministic(app):
    """The trial crashes after the last event's decision is computed but before its commit, then
    the last event is recomputed on recovery. No request is lost and the session write position
    matches a clean run."""
    from src.services import execution
    from src.services.execution import run_trial
    from src.services.routing import simulate
    from src.models.entities import Trial,TrialEvent
    from sqlalchemy import select
    ident=create_trial(app['Session'])
    real=execution.checkpoint
    def dying(db,i,token,state,row,total):
        if int(state['index'])==total:
            db.rollback();return None      # computed the final decision, died before persisting it
        return real(db,i,token,state,row,total)
    execution.checkpoint=dying
    try:assert run_trial(ident) is None
    finally:execution.checkpoint=real
    with app['Session']() as db:
        obj=db.get(Trial,ident)
        assert obj.status=='running' and obj.cursor==7 and obj.result is None
        assert len(db.scalars(select(TrialEvent).where(TrialEvent.trial_id==ident)).all())==7
        obj.lease_expires_at=None;db.commit()
    assert run_trial(ident)=='completed'
    with app['Session']() as db:
        obj=db.get(Trial,ident);expected=simulate(obj.input_snapshot['nodes'],obj.input_snapshot['request_set']['events'],100,500)
        assert obj.cursor==8 and obj.result['checksum']==expected['checksum']
        assert obj.result['sessions']==expected['sessions']
        rows=db.scalars(select(TrialEvent).where(TrialEvent.trial_id==ident).order_by(TrialEvent.seq)).all()
        assert [r.seq for r in rows]==list(range(1,9))
        assert all(r.payload==e for r,e in zip(rows,expected['trace']))
