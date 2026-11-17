ROLES = ('admin', 'operator', 'viewer')
# queued/running are non-terminal. paused keeps the checkpoint and resumes from the cursor.
# paused_before / paused_after record whether the request in flight at pause confirmation was
# already committed (before) or deliberately rolled back and replayed after resume (after).
RUN_STATES = ('queued', 'running', 'paused', 'completed', 'failed')
ACTIVE_STATES = ('queued', 'running', 'paused')
TERMINAL_STATES = ('completed', 'failed')
