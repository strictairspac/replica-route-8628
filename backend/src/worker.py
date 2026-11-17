import os,time,traceback
from pathlib import Path
from src.config.database import Session
from src.config.settings import RECOVERY_INTERVAL_SECONDS,RECOVERY_COOLDOWN_SECONDS
from src.services.trials import queue,_enqueue
from src.services import execution

HEARTBEAT=Path(os.getenv('WORKER_HEARTBEAT','/tmp/worker-heartbeat'))
_last_seen={}


def recover():
    """Adopt trials whose executor disappeared: expired lease, or lost/duplicated queue items.

    Re-enqueueing is throttled per trial and harmless anyway: ``execution.claim`` is fenced, so
    extra queue items never let a second executor advance the same cursor twice.
    """
    try:
        with Session() as db:
            for ident in execution.recover(db):
                stamp=_last_seen.get(ident,0)
                if time.time()-stamp>=RECOVERY_COOLDOWN_SECONDS:
                    _enqueue(ident);_last_seen[ident]=time.time()
                    print('recovered trial',ident,flush=True)
    except Exception:
        traceback.print_exc()


def main():
    recover()
    while True:
        HEARTBEAT.touch()
        try:
            item=queue.blpop('replicaroute:queue',timeout=RECOVERY_INTERVAL_SECONDS)
        except Exception:
            # Redis unavailable: keep the heartbeat/recovery loop alive instead of spinning.
            traceback.print_exc();time.sleep(max(RECOVERY_INTERVAL_SECONDS,0.5));item=None
        if item:
            try:execution.run_trial(int(item[1]))
            except Exception:traceback.print_exc()
        recover()

if __name__=='__main__':main()
