// Zole tournament server: no installs needed, just Node.js. Run:  node server.js
const http=require('http'),fs=require('fs'),path=require('path'),os=require('os');
const PORT=process.env.PORT||3000,F=path.join(__dirname,'data.json');
let st={cfg:null,rounds:[]};
try{st=JSON.parse(fs.readFileSync(F,'utf8'))}catch(e){}
const clients=new Set();
function save(){fs.writeFileSync(F,JSON.stringify(st));const m='data: '+JSON.stringify(st)+'\n\n';clients.forEach(c=>c.write(m))}
http.createServer((q,r)=>{
  const u=q.url.split('?')[0];
  if(u==='/events'){
    r.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache',Connection:'keep-alive'});
    r.write('data: '+JSON.stringify(st)+'\n\n');clients.add(r);q.on('close',()=>clients.delete(r));return;
  }
  if(q.method==='GET'){r.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});return r.end(fs.readFileSync(path.join(__dirname,'index.html')))}
  let b='';q.on('data',c=>b+=c);
  q.on('end',()=>{try{
    if(u==='/api/cfg'&&q.method==='POST'){st.cfg=JSON.parse(b)}
    else if(u==='/api/rounds'&&q.method==='POST'){const o=JSON.parse(b);o.id=Date.now().toString(36)+Math.random().toString(36).slice(2,6);st.rounds.push(o)}
    else if(u.startsWith('/api/rounds/')&&q.method==='DELETE'){const id=u.split('/').pop();st.rounds=st.rounds.filter(x=>x.id!==id)}
    else{r.writeHead(404);return r.end()}
    save();r.writeHead(200);r.end('ok');
  }catch(e){r.writeHead(400);r.end()}});
}).listen(PORT,'0.0.0.0',()=>{
  console.log('\nZole is running. Open one of these on your phones/tablets/TV (same Wi-Fi):\n');
  Object.values(os.networkInterfaces()).flat().filter(i=>i.family==='IPv4'&&!i.internal).forEach(i=>console.log('   http://'+i.address+':'+PORT));
  console.log('\n(on this computer: http://localhost:'+PORT+')\nStop with Ctrl+C. Scores are saved in data.json\n');
});
