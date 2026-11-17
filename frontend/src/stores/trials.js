import {trialsApi} from '../api/trials.js';
export const trialsStore={
  items:[],
  async refresh(){this.items=await trialsApi.list();return this.items;},
  async pause(id){const updated=await trialsApi.pause(id);this.replace(updated);return updated;},
  async resume(id){const updated=await trialsApi.resume(id);this.replace(updated);return updated;},
  replace(updated){const at=this.items.findIndex(x=>x.id===updated.id);if(at>=0)this.items[at]={...this.items[at],...updated};else this.items.unshift(updated);},
  hasActive(){return this.items.some(x=>['queued','running','paused'].includes(x.status));},
};
