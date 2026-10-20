// Zole server: vairāki turnīri ar parolēm. Palaid:  node server.js
// Atjauninājumi uz ierīcēm tiek sūtīti kā mazi "izmaiņu" ziņojumi (nevis viss turnīrs katru reizi).
const http=require('http'),fs=require('fs'),path=require('path'),os=require('os'),crypto=require('crypto'),zlib=require('zlib');
const PORT=process.env.PORT||3000,F=process.env.DATA_FILE||path.join(__dirname,'data.json');
let st={ts:{}};
try{const x=JSON.parse(fs.readFileSync(F,'utf8'));if(x&&x.ts)st=x}catch(e){}
const RU=process.env.UPSTASH_REDIS_REST_URL,RT=process.env.UPSTASH_REDIS_REST_TOKEN;
const rcmd=c=>fetch(RU,{method:'POST',headers:{Authorization:'Bearer '+RT,'Content-Type':'application/json'},body:JSON.stringify(c)}).then(r=>r.json());
async function loadRemote(){if(!RU)return;try{const r=await rcmd(['GET','zole2']);if(r.result){const x=JSON.parse(r.result);if(x&&x.ts)st=x}console.log('Loaded data from Upstash Redis')}catch(e){console.log('Redis load failed:',e.message)}}
const sha=s=>crypto.createHash('sha256').update(s).digest('hex'),rid=n=>crypto.randomBytes(n).toString('hex');
const clients={},fails={},stat={start:Date.now(),sse:0,page:0,api:0};
function send(tid,msg){const m='data: '+JSON.stringify(msg)+'\n\n',set=clients[tid]||[];stat.sse+=Buffer.byteLength(m)*(set.size||0);set.forEach(c=>c.write(m));if(msg.type==='gone'){set.forEach(c=>c.end());delete clients[tid]}}
// Saglabāšana diskā/Upstash tiek apvienota (ne biežāk kā reizi ~1,5 s)
let timer=null;
function flush(){timer=null;try{fs.writeFileSync(F,JSON.stringify(st))}catch(e){}
  return RU?rcmd(['SET','zole2',JSON.stringify(st)]).catch(e=>console.log('Redis save failed:',e.message)):Promise.resolve()}
function save(){const old=Date.now()-10*864e5;Object.keys(st.ts).forEach(k=>{if(st.ts[k].created<old){delete st.ts[k];send(k,{type:'gone'})}});if(!timer)timer=setTimeout(flush,1500)}
const quit=()=>{if(timer)clearTimeout(timer);const t=setTimeout(()=>process.exit(0),4000);flush().finally(()=>{clearTimeout(t);process.exit(0)})};
process.on('SIGTERM',quit);process.on('SIGINT',quit);
setInterval(()=>Object.values(clients).forEach(s=>s.forEach(c=>c.write(': ping\n\n'))),25000);
const json=(r,c,o)=>{const b=JSON.stringify(o||{});stat.api+=Buffer.byteLength(b);r.writeHead(c,{'Content-Type':'application/json'});r.end(b)};
let idx={m:0,raw:null,gz:null};
function page(q,r){const f=path.join(__dirname,'index.html'),m=fs.statSync(f).mtimeMs;if(idx.m!==m){idx.raw=fs.readFileSync(f);idx.gz=zlib.gzipSync(idx.raw);idx.m=m}
  const gz=/\bgzip\b/.test(q.headers['accept-encoding']||'');r.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Vary':'Accept-Encoding',...(gz?{'Content-Encoding':'gzip'}:{})});const pb=gz?idx.gz:idx.raw;stat.page+=pb.length;r.end(pb)}
const server=http.createServer((q,r)=>{
  const url=new URL(q.url,'http://x'),u=url.pathname;
  if(u==='/events'){
    const tid=url.searchParams.get('t'),t=st.ts[tid];
    if(!t||url.searchParams.get('k')!==t.tok){r.writeHead(403);return r.end()}
    r.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache',Connection:'keep-alive','X-Accel-Buffering':'no'});
    const f0='data: '+JSON.stringify({type:'full',cfg:t.cfg,rounds:t.rounds})+'\n\n';stat.sse+=Buffer.byteLength(f0);r.write(f0);(clients[tid]=clients[tid]||new Set()).add(r);q.on('close',()=>clients[tid]&&clients[tid].delete(r));return;
  }
  if(q.method==='GET'&&u==='/api/stats'){const mb=n=>Math.round(n/1048.576)/1000;return json(r,200,{uptimeMin:Math.round((Date.now()-stat.start)/6e4),memoryMB:Math.round(process.memoryUsage().rss/1048576),tournaments:Object.keys(st.ts).length,connections:Object.values(clients).reduce((a,x)=>a+x.size,0),sseMB:mb(stat.sse),pageMB:mb(stat.page),apiMB:mb(stat.api)})}
  if(q.method==='GET'&&u==='/api/list')return json(r,200,Object.entries(st.ts).map(([tid,t])=>({tid,name:t.cfg.name})));
  if(q.method==='GET'){try{return page(q,r)}catch(e){console.log('Cannot serve index.html:',e.message);r.writeHead(500,{'Content-Type':'text/plain; charset=utf-8'});return r.end('index.html nav atrasts serverī. Pārbaudi, vai fails index.html ir GitHub repozitorija saknē blakus server.js.')}}
  let body='';q.on('data',c=>{body+=c;if(body.length>1e6)q.destroy()});
  q.on('end',()=>{try{
    const b=body?JSON.parse(body):{},tk=q.headers['x-token'],hk=q.headers['x-host'];
    if(u==='/api/create'&&q.method==='POST'){
      const c=b.cfg;if(!c||!c.tables||!c.limit||!c.players||String(b.password||'').length<3||String(b.hostPassword||'').length<3||b.hostPassword===b.password)return json(r,400);
      const tid=rid(4),t={created:Date.now(),pw:sha(tid+':'+b.password),hpw:sha(tid+':h:'+b.hostPassword),tok:rid(16),hk:rid(16),cfg:{...c,tid},rounds:[]};
      st.ts[tid]=t;save();return json(r,200,{tid,token:t.tok,host:t.hk});
    }
    if(u==='/api/join'&&q.method==='POST'){
      const t=st.ts[b.tid];if(!t)return json(r,404);
      const ip=(q.headers['x-forwarded-for']||q.socket.remoteAddress||'').split(',')[0].trim(),k=ip+b.tid,f=fails[k]=fails[k]||{n:0,t:Date.now()};
      if(Date.now()-f.t>6e5){f.n=0;f.t=Date.now()}
      if(f.n>=8)return json(r,429);
      if(sha(b.tid+':'+b.password)===t.pw)return json(r,200,{token:t.tok});
      if(t.hpw&&sha(b.tid+':h:'+b.password)===t.hpw)return json(r,200,{token:t.tok,host:t.hk});
      f.n++;return json(r,403);
    }
    const m=u.match(/^\/api\/t\/([0-9a-f]+)(?:\/(cfg|round|order))?(?:\/([^/]+))?$/);
    const t=m&&st.ts[m[1]];if(!t)return json(r,404);
    if(tk!==t.tok)return json(r,403);
    const tid=m[1],a=m[2];
    if(!a&&q.method==='DELETE'){if(hk!==t.hk)return json(r,403);delete st.ts[tid];send(tid,{type:'gone'});save();return json(r,200)}
    if(a==='cfg'&&q.method==='POST'){if(hk!==t.hk)return json(r,403);if(!b.tables||!b.limit)return json(r,400);t.cfg={...b,tid};send(tid,{type:'cfg',cfg:t.cfg});save();return json(r,200)}
    if(a==='round'&&q.method==='POST'){if(!b.d)return json(r,400);b.id=rid(6);b.tid=tid;t.rounds.push(b);send(tid,{type:'add',round:b});save();return json(r,200)}
    if(a==='round'&&q.method==='DELETE'){t.rounds=t.rounds.filter(x=>x.id!==m[3]);send(tid,{type:'del',id:m[3]});save();return json(r,200)}
    if(a==='order'&&q.method==='POST'){
      const T=t.cfg.tables[b.table];if(!T||!Array.isArray(b.players)||b.players.slice().sort().join('|')!==T.players.slice().sort().join('|'))return json(r,400);
      T.players=b.players;send(tid,{type:'cfg',cfg:t.cfg});save();return json(r,200)}
    json(r,404);
  }catch(e){json(r,400)}});
});
if(!fs.existsSync(path.join(__dirname,'index.html')))console.log('WARNING: index.html not found next to server.js ('+__dirname+')');
loadRemote().then(()=>server.listen(PORT,'0.0.0.0',()=>{
  console.log('\nZole is running. Open one of these on your phones/tablets/TV (same Wi-Fi):\n');
  Object.values(os.networkInterfaces()).flat().filter(i=>i.family==='IPv4'&&!i.internal).forEach(i=>console.log('   http://'+i.address+':'+PORT));
  console.log('\n(on this computer: http://localhost:'+PORT+')\nStop with Ctrl+C. Data file: '+F+'\n');
}));
