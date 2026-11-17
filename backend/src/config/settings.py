import os
DATABASE_URL=os.getenv('DATABASE_URL','mysql+pymysql://replicaroute:replicaroute_demo_pw@localhost:17747/replicaroute')
REDIS_URL=os.getenv('REDIS_URL','redis://redis:6379/0')
JWT_SECRET=os.getenv('JWT_SECRET','local-replica-route-change-before-public-use')
# Execution lease. An executor that stops heart-beating (crash, restart) loses its fence after
# this many seconds and another executor may take over from the last committed checkpoint.
LEASE_SECONDS=float(os.getenv('LEASE_SECONDS','15'))
# Extra window between the pause request and the next decision, used by race tests.
PAUSE_GATE_SECONDS=float(os.getenv('PAUSE_GATE_SECONDS','0'))
# How often a worker scans for trials whose executor died (expired lease) and re-queues them.
RECOVERY_INTERVAL_SECONDS=float(os.getenv('RECOVERY_INTERVAL_SECONDS','2'))
RECOVERY_COOLDOWN_SECONDS=float(os.getenv('RECOVERY_COOLDOWN_SECONDS','5'))
