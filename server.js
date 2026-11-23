import {WebSocketServer} from 'ws';
import {createClient} from '@supabase/supabase-js';
const sb=createClient(process.env.SUPABASE_URL,process.env.SUPABASE_SERVICE_KEY);
const wss=new WebSocketServer({port:+process.env.PORT||8080});
const C=new Map(),T=new Map(); // C: id->клиент, T: "от>кому" -> предложение обмена
const lvl=x=>Math.floor(Math.sqrt(x/50))+1;
const pub=p=>({nick:p.nick,xp:p.xp,level:lvl(p.xp),coins:p.coins,cores:p.cores,up:p.up});
const tx=(c,o)=>c.ws.readyState===1&&c.ws.send(JSON.stringify(o));
const save=c=>sb.from('players').update({xp:c.p.xp,coins:c.p.coins,cores:c.p.cores,up:c.p.up,updated_at:new Date()}).eq('id',c.p.id).then(({error})=>error&&console.error('save',error.message));
const push=c=>tx(c,{t:'pup',p:pub(c.p)});
const members=c=>c.party?[...C.values()].filter(o=>o.party===c.party):[c];
const pstate=l=>{const ms=members(l),nm={};ms.forEach(o=>nm[o.id]=o.p.nick);ms.forEach(o=>tx(o,{t:'party',leader:l.id,ids:ms.map(o=>o.id),nm}))};
const nope=o=>({t:'party',leader:null,ids:[],nm:{}});
function leave(c){const lid=c.party;if(!lid)return;
  if(lid===c.id){const ms=members(c);ms.forEach(o=>{o.party=null;tx(o,nope())})}
  else{c.party=null;tx(c,nope());const l=C.get(lid);l&&pstate(l)}}
async function login(c,{token,nick}){
  if(!/^[0-9a-f-]{36}$/.test(token||''))return tx(c,{t:'err',m:'Неверный ключ'});
  let {data}=await sb.from('players').select('*').eq('token',token).maybeSingle();
  if(!data){
    if(!/^[\wа-яА-ЯёЁ-]{3,16}$/.test(nick||''))return tx(c,{t:'err',m:'Ник: 3–16 символов, буквы/цифры/_/-'});
    const r=await sb.from('players').insert({token,nick}).select().single();
    if(r.error)return tx(c,{t:'err',m:r.error.code==='23505'?'Ник занят':'Ошибка базы данных'});
    data=r.data}
  const old=C.get(data.id);if(old&&old!==c)old.ws.close();
  c.p=data;c.id=data.id;C.set(c.id,c);tx(c,{t:'me',id:c.id,p:pub(data)})}
const int=(v,m=1e9)=>Math.max(0,Math.min(m,v|0));
wss.on('connection',ws=>{
  const c={ws,mode:'hub',x:0,z:18,yaw:0,party:null,raid:0,rw:0};
  ws.on('message',async raw=>{
    if(raw.length>20000)return;let m;try{m=JSON.parse(raw)}catch{return}
    if(m.t==='login')return login(c,m);
    if(!c.p)return;
    switch(m.t){
    case'pos':c.x=+m.x||0;c.z=+m.z||0;c.yaw=+m.yaw||0;break;
    case'hub':c.mode='hub';c.raid=0;break;
    case'inv':{const o=C.get(m.to);if(o&&o!==c&&!o.raid)tx(o,{t:'inv',from:c.id,nick:c.p.nick})}break;
    case'acc':{const l=C.get(m.from);if(!l||l.raid||c.raid)break;leave(c);l.party=l.id;c.party=l.id;pstate(l)}break;
    case'pl':leave(c);break;
    case'go':{if(c.party&&c.party!==c.id)break;const ms=members(c);ms.forEach(o=>{o.mode='raid';o.raid=1;tx(o,{t:'raid',host:c.id,n:ms.length})})}break;
    case'rl':if(c.raid)members(c).forEach(o=>o!==c&&tx(o,{t:'rl',from:c.id,d:m.d}));break;
    case'reward':{if(!c.raid||Date.now()-c.rw<15000)break;c.rw=Date.now();const b=int(m.b,2);
      c.p.xp+=50+50*b;c.p.coins+=30+30*b;c.p.cores+=1+b;save(c);push(c)}break;
    case'buy':{const k=m.k;if(!['dmg','hp','spd'].includes(k))break;const n=c.p.up[k]|0,pc=50*(n+1),pk=n+1;
      if(n>=10||c.p.coins<pc||c.p.cores<pk)break;c.p.coins-=pc;c.p.cores-=pk;c.p.up={...c.p.up,[k]:n+1};save(c);push(c)}break;
    case'tr':{const o=C.get(m.to);if(!o||o===c)break;const t={gc:int(m.gc),gk:int(m.gk),wc:int(m.wc),wk:int(m.wk)};
      if(t.gc>c.p.coins||t.gk>c.p.cores)break;T.set(c.id+'>'+o.id,t);tx(o,{t:'tr',from:c.id,nick:c.p.nick,...t});tx(c,{t:'msg',m:'Предложение отправлено'})}break;
    case'tok':{const f=C.get(m.from),k=m.from+'>'+c.id,t=T.get(k);if(!f||!t)break;T.delete(k);
      if(t.gc>f.p.coins||t.gk>f.p.cores||t.wc>c.p.coins||t.wk>c.p.cores){tx(c,{t:'msg',m:'Обмен не удался: не хватает ресурсов'});tx(f,{t:'msg',m:'Обмен не удался: не хватает ресурсов'});break}
      f.p.coins+=t.wc-t.gc;f.p.cores+=t.wk-t.gk;c.p.coins+=t.gc-t.wc;c.p.cores+=t.gk-t.wk;
      save(f);save(c);push(f);push(c);tx(f,{t:'msg',m:c.p.nick+' принял обмен'})}break;
    }});
  ws.on('close',()=>{if(C.get(c.id)!==c)return;leave(c);C.delete(c.id);for(const k of T.keys())if(k.includes(c.id))T.delete(k)});
});
setInterval(()=>{const l=[...C.values()].filter(o=>o.mode==='hub').map(o=>[o.id,o.p.nick,o.x,o.z,o.yaw,lvl(o.p.xp)]);
  C.forEach(o=>o.mode==='hub'&&tx(o,{t:'hub',l}))},100);
console.log('Nova Raid server on :'+(process.env.PORT||8080));
