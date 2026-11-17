from sqlalchemy import select
from redis import Redis
from fastapi import HTTPException
from src.models.entities import Audit,Node,RequestSet,Trial,TrialEvent
from src.config.settings import REDIS_URL
from src.constants.enums import RUN_STATES
from src.utils.serialization import record
from .common import save
from .routing import simulate
from . import execution
queue=Redis.from_url(REDIS_URL,decode_responses=True)


def _view(obj):
    data=record(obj)
    # Internal checkpoint/lease bookkeeping is not part of the client contract.
    for key in ('replay_state','lease_owner','lease_token'):data.pop(key,None)
    data.update(execution.progress_fields(obj))
    return data


def listing(db):return [_view(x) for x in db.scalars(select(Trial).order_by(Trial.id.desc()))]


def detail(db,ident):
    obj=db.get(Trial,ident)
    if not obj:raise HTTPException(404,'试验不存在')
    data=_view(obj)
    if obj.status!='completed':
        # In-progress evidence: only committed decisions are exposed, so the page never shows a
        # request that was rolled back at a pause boundary.
        data['partial_trace']=[{**r.payload,'epoch':r.epoch} for r in db.scalars(select(TrialEvent).where(TrialEvent.trial_id==ident).order_by(TrialEvent.seq))]
    return data


def create(db,data,user):
    req=db.get(RequestSet,data.request_set_id)
    if not req:raise HTTPException(404,'请求集不存在')
    nodes=[record(x) for x in db.scalars(select(Node).order_by(Node.id))]
    try:simulate(nodes,req.events,data.max_delay_ms,data.sticky_ms)
    except ValueError as e:raise HTTPException(422,str(e))
    snapshot={'nodes':nodes,'request_set':record(req)}
    obj=Trial(**data.model_dump(),input_snapshot=snapshot,status=RUN_STATES[0],total_events=len(req.events),cursor=0,epoch=0)
    save(db,obj,user,'路由演练入队 '+data.name)
    try:queue.rpush('replicaroute:queue',str(obj.id))
    except Exception:
        obj.status='failed';obj.error='队列暂时不可用，请重新创建';db.commit();raise HTTPException(503,obj.error)
    return _view(obj)


def _enqueue(ident):
    try:queue.rpush('replicaroute:queue',str(ident))
    except Exception:pass


def pause(db,ident,user):
    obj,error=execution.pause(db,ident)
    if error:raise HTTPException(404 if '不存在' in error else 409,error)
    db.add(Audit(actor=user.email,action=f'暂停路由演练 #{ident}（暂停边界 {obj.pause_cursor} 事件）'))
    db.commit()
    return _view(obj)


def resume(db,ident,user):
    obj,error=execution.resume(db,ident,_enqueue)
    if error:raise HTTPException(404 if '不存在' in error else 409,error)
    db.add(Audit(actor=user.email,action=f'继续路由演练 #{ident}（从第 {obj.cursor} 个事件恢复）'))
    db.commit()
    return _view(obj)
