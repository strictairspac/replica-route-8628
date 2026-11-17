/** Progress bar + counter for a resumable trial. Source fields: cursor / total_events / percent. */
export function progress(t){
  const total=Number(t.total_events||0),cursor=Number(t.cursor||0);
  const percent=total?Math.min(100,Math.round(100*cursor/total)):0;
  return `<div class="progress" data-trial="${t.id}"><div class="progress-track"><span style="width:${percent}%"></span></div><small>已提交 ${cursor} / ${total} 事件 · ${percent}%${t.status==='paused'&&t.pause_cursor!=null?` · 暂停边界 ${t.pause_cursor}`:''}</small></div>`;
}
/** Pause/resume controls. Ended trials keep their evidence readable and expose no controls. */
export function trialActions(t,canWrite){
  if(!canWrite)return '';
  if(['queued','running'].includes(t.status))return `<button class="small act-pause" data-id="${t.id}">暂停</button>`;
  if(t.status==='paused')return `<button class="small act-resume" data-id="${t.id}">继续</button>`;
  return '';
}
