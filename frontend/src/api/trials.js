import {api} from './client.js';
export const trialsApi={
  list:()=>api('/trials'),
  detail:id=>api('/trials/'+id),
  create:data=>api('/trials','POST',data),
  pause:id=>api('/trials/'+id+'/pause','POST',{}),
  resume:id=>api('/trials/'+id+'/resume','POST',{}),
};
