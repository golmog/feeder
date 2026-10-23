// 브라우저 의존성 없이 실제 상태 병합기와 요청 제어기를 검증한다.
const fs=require('fs'),vm=require('vm'),assert=require('assert'),path=require('path');
const root=path.resolve(__dirname,'..');
const elements=new Map(), requests=[], timers=[], events={};
function element(selector) {
  if(!elements.has(selector)) elements.set(selector,{textValue:'',attrs:{},length:1});
  const el=elements.get(selector);
  return new Proxy(el,{get(target,key){
    if(key==='text') return v=>{if(v===undefined)return target.textValue;target.textValue=v;return element(selector);};
    if(key==='attr') return (name,v)=>{if(v===undefined)return target.attrs[name];target.attrs[name]=v;return element(selector);};
    if(key==='html') return v=>{target.htmlValue=v;return element(selector);};
    if(key==='is') return ()=>false;
    if(key in target) return target[key];
    return ()=>element(selector);
  }});
}
const $=s=>element(s);
$.ajax=options=>{
 const callbacks={}; const r={options,callbacks,aborted:false};
 for(const key of ['done','fail','always'])r[key]=fn=>{callbacks[key]=fn;return r;};
 r.abort=()=>{r.aborted=true;if(callbacks.fail)callbacks.fail();if(callbacks.always)callbacks.always();};
 r.resolve=data=>{callbacks.done(data);if(callbacks.always)callbacks.always();};
 requests.push(r);return r;
};
const ctx={console,Map,Set,Array,Date,JSON,Number,String,Object,$,package_name:'feeder',sub:'download',
 setInterval:fn=>{timers.push(fn);return timers.length;},clearInterval:()=>{},render_queue_rows:()=>{},
 render_upload_queue_count:()=>{},format_bytes:v=>String(v),window:{},document:{}};
vm.createContext(ctx);vm.runInContext(fs.readFileSync(path.join(root,'static/feeder_dashboard.js'),'utf8'),ctx);
const S=ctx.FeederQueueState,now=Date.now()/1000;
const row=(id,status='downloading')=>({id,row_key:'db:'+id,row_version:'2026-10-23T01',status,current_engine_name:'cd2',engine_task_id:'same',infohash:'h'});
const db=(items,at=now)=>({success:true,items,observed_at:at,total:785,next_cursor:'next',counts:{pending:0}});
function runBaseline() {
let s=new S();assert(s.mergeDB(db([row(1)]),0,false));assert(s.mergeDB(db([row(2)]),0,true));
assert.strictEqual(s.dbItemsById.size,2);assert.strictEqual(s.counts.pending,0);
assert(!s.mergeDB(db([row(1,'pending')],now-1),0,false));
s.requestGeneration++;assert(!s.mergeDB(db([row(3)]),0,true));assert.strictEqual(s.dbItemsById.size,2);
const snapshot=(key,items,extra={})=>Object.assign({engine_key:key,engine_name:key,config_revision:'v1',match_verified:true,state:'ready',complete:true,items,last_success_at:now},extra);
const remote={task_id:'same',hash:'h',last_seen_at:now,download_speed:99,progress:25};
s.mergeEngines({success:true,observed_at:now,engines:[snapshot('cd2',[Object.assign({},remote,{db_id:1})]),snapshot('ad',[remote])]},1);
let view=s.view(now);assert.strictEqual(view.rows.find(r=>r.id===1).download_speed,99);
assert.strictEqual(view.orphanTotal,1);assert(view.rows.some(r=>r.row_key==='engine:ad:same'));
s.mergeEngines({success:true,observed_at:now+1,engines:[snapshot('cd2',[],{complete:false,state:'stale',error_type:'TimeoutError'}),snapshot('ad',[remote])]},1);
view=s.view(now+1);assert.strictEqual(view.rows.find(r=>r.id===1).download_speed,0);assert.strictEqual(view.orphanTotal,1);
s.mergeDB(db([row(1,'pending_upload')],now+2),1,false);
assert.strictEqual(s.view(now+2).rows.find(r=>r.id===1).status,'pending_upload');
s.mergeDB(Object.assign(db([],now+3),{removed_ids:[1]}),1,false);assert(!s.dbItemsById.has('1'));
s.mergeEngines({success:true,observed_at:now+3,engines:[snapshot('ad',[])]},1);assert.strictEqual(s.view(now+3).orphanTotal,0);
// 서로 다른 네임스페이스의 표시 ID, 부분 페이지 실패, 세대 변경 후 늦은 응답.
let renders=0;ctx.render_queue_rows=()=>renders++;
let d=new ctx.FeederDashboard();d.start();
assert(requests.some(r=>r.options.url.endsWith('/queue_bootstrap')),'SSE 없이도 DB 최초 조회');
assert(requests.some(r=>r.options.url.endsWith('/queue_engines')));
const initial=requests.find(r=>r.options.url.endsWith('/queue_bootstrap'));
initial.resolve(db([row(1)]));assert.strictEqual(d.state.dbItemsById.size,1);
assert.strictEqual(elements.get('#active_queue_tbody').attrs['aria-busy'],'false');
d.more();let page=requests.find(r=>r.options.url.endsWith('/queue_page'));page.callbacks.fail();page.callbacks.always();
assert.strictEqual(d.state.dbItemsById.size,1);assert.strictEqual(elements.get('#queue_more_btn').textValue,'다시 시도');
d.more();const oldPage=requests[requests.length-1];d.refresh();assert(oldPage.aborted);
oldPage.callbacks.done(db([row(99)]));assert(!d.state.dbItemsById.has('99'));
const latest=requests.filter(r=>r.options.url.endsWith('/queue_bootstrap')).slice(-1)[0];
latest.resolve(Object.assign(db([],now+10),{removed_ids:[1],total:0,next_cursor:null}));
assert.strictEqual(d.state.dbItemsById.size,0);
const count=renders;d.render();assert.strictEqual(renders,count,'변경 없는 데이터는 다시 렌더하지 않음');
const late=requests[requests.length-1];d.close();if(late.callbacks.done)late.callbacks.done({success:true,engines:[],observed_at:now});
assert(d.closed);assert(d.requests.size===0);
// 주 스크립트에서 SSE 미도착/단절 시 독립 HTTP 갱신을 확인한다.
const main=fs.readFileSync(path.join(root,'static/feeder_download.js'),'utf8');
ctx.window.addEventListener=(name,fn)=>events[name]=fn;ctx.apikey='';ctx.enable_sse=true;
ctx.EventSource=ctx.window.EventSource=function(){this.close=()=>{};this.addEventListener=()=>{};};
vm.runInContext(main,ctx);
const before=requests.length;ctx.init_download_activity();
assert(requests.length===before+1);const activity=requests[requests.length-1];assert(activity.options.data.snapshot==='1');
ctx.download_activity_loading=false;ctx.download_activity_source.onerror();ctx.download_activity_received=0;
timers[timers.length-1]();assert(requests.length===before+2,'SSE 단절 중에도 활동 HTTP 조회');
ctx.init_sse_listener();assert(ctx.sse_source);ctx.feeder_dashboard=d;events.pagehide();assert(ctx.queue_page_closed);
assert.strictEqual(ctx.queue_escape_html('<img src=x onerror="x">'), '&lt;img src=x onerror=&quot;x&quot;&gt;');
assert(main.includes('queue-item-buttons d-flex flex-wrap justify-content-end ml-auto'));
assert(main.includes('text-nowrap mr-2'));
assert(!main.includes('page_size: 500'));assert(!main.includes("sessionStorage.setItem('feeder_queue_cache'"));
assert(!main.includes('queue_loading_overlay'));
const template=fs.readFileSync(path.join(root,'templates/feeder_download_queue.html'),'utf8');
assert(template.includes('range(4)'));assert(template.includes('>— / —</h3>'));
console.log('dashboard checks passed');
// 실제 행 렌더러를 작은 DOM 대역으로 실행해 이스케이프와 행 재사용을 검증한다.
let domRows=[], builds=0, moves=0;
const tbody={length:1,0:{appendChild(fragment){moves++;domRows=fragment.rows.slice();}},
 html(){domRows=[];return this;},
 children(selector){
  const selected=selector.includes(':not')?[]:domRows.slice();
  return {each(fn){selected.forEach(row=>fn.call(row));return this;},remove(){return this;}};
 }};
function dom$(value){
 if(value==='#active_queue_tbody')return tbody;
 if(typeof value==='string'&&value.startsWith('<tr ')){
  builds++; const key=value.match(/data-queue-id="([^"]+)"/)[1];
  const row={key,innerHTML:value,getAttribute(name){return name==='data-queue-id'?key:null;},remove(){domRows=domRows.filter(r=>r!==row);}};
  return [row];
 }
 if(value&&typeof value==='object'&&value.key)return {attr:()=>value.key};
 return element(value);
}
ctx.$=dom$;ctx.document={activeElement:null,createDocumentFragment:()=>({rows:[],appendChild(row){this.rows.push(row);}})};
ctx.last_pipeline_control={state:'paused'};ctx.queue_page_closed=false;
let input=[Object.assign(row(1,'pending'),{title:'<img src=x onerror="evil">',file_name:'" data-secret="oops',local_path:'<script>x</script>'}),
 {id:'same',row_key:'engine:ad:same',status:'engine_unmanaged',task_id:'same',current_engine_name:'ad',title:'<b>x</b>',file_name:'a"b',raw_status:'pending'}];
ctx.render_queue_rows(input);assert.strictEqual(domRows.length,2);assert.strictEqual(builds,2);
assert(!domRows[0].innerHTML.includes('<img src=x'));assert(domRows[0].innerHTML.includes('&lt;img'));
assert(domRows[1].innerHTML.includes('data-file-name="a&quot;b"'));
const firstDom=domRows[0], oldBuilds=builds, oldMoves=moves;
ctx.render_queue_rows(input);assert.strictEqual(builds,oldBuilds);assert.strictEqual(moves,oldMoves);assert.strictEqual(domRows[0],firstDom);
input=[Object.assign({},input[0],{title:'changed'}),input[1]];
ctx.render_queue_rows(input);assert.strictEqual(builds,oldBuilds+1);assert.strictEqual(domRows[0],firstDom);assert.strictEqual(moves,oldMoves);
assert(domRows[0].innerHTML.includes('data-action="execute_single"'));
assert(!domRows[0].innerHTML.includes(' disabled'));
ctx.last_pipeline_control={state:'running'};ctx.render_queue_rows(input);
assert(domRows[0].innerHTML.includes(' disabled'));
console.log('renderer checks passed');

}

function runStaleSpeedChecks() {
  const state=new S(), at=1000;
  const task=(id,engine,tid)=>Object.assign(row(id),{current_engine_name:engine,engine_task_id:tid,infohash:'hash-'+tid});
  const item=(tid,id,speed,time,stale=false)=>({task_id:tid,hash:'hash-'+tid,db_id:id,last_seen_at:time,
    filename:tid,download_speed:speed,progress:id===1?75:50,stale});
  const snap=(engine,items,verified=true,extra={})=>Object.assign({engine_key:engine,engine_name:engine,
    config_revision:'v1',state:'ready',complete:true,match_verified:verified,items},extra);
  const merge=(items,time,verified=true,extra={},generation=0)=>state.mergeEngines({success:true,observed_at:time,
    engines:[snap('ad',items,verified,extra),snap('cd2',[item('a',3,100+time-at,time)])]},generation);
  state.mergeDB(db([task(1,'ad','a'),task(2,'ad','b'),task(3,'cd2','a')],at),0,false);
  const originalA=item('a',1,88,at), originalB=item('b',2,40,at);
  merge([originalA,originalB],at);
  const previousA=state.engineSnapshots.get('ad').items.get('a');
  assert.strictEqual(state.view(at).rows.find(r=>r.id===1).download_speed,88);
  // DB 매칭 실패로 보존된 A만 지연 표시하고, 이번에 받은 B와 다른 엔진은 즉시 갱신한다.
  merge([item('b',2,55,at+1)],at+1,false);
  let view=state.view(at+1), a=view.rows.find(r=>r.id===1), b=view.rows.find(r=>r.id===2);
  assert.strictEqual(a.engine_stale,true,'이번 원격 목록에 없는 A는 stale');
  assert.strictEqual(a.download_speed,0,'보존된 A의 과거 속도는 현재 속도가 아님');
  assert.strictEqual(a.progress,75);assert.strictEqual(a.status,'downloading');
  assert.strictEqual(b.download_speed,55);assert.strictEqual(b.engine_stale,false);
  assert.strictEqual(view.rows.find(r=>r.id===3).download_speed,101);
  assert.strictEqual(originalA.stale,false);assert.strictEqual(previousA.stale,false,'이전 스냅샷 객체 불변');
  assert.notStrictEqual(state.engineSnapshots.get('ad').items.get('a'),previousA);
  assert.strictEqual(state.engineSnapshots.get('ad').items.get('a').last_seen_at,at);
  // 빈 목록/매칭 실패도 행을 없애거나 이전 속도를 복구하지 않는다.
  merge([],at+2,false);view=state.view(at+2);
  for(const id of [1,2]){const r=view.rows.find(r=>r.id===id);assert.strictEqual(r.engine_stale,true);assert.strictEqual(r.download_speed,0);}
  assert.strictEqual(state.engineSnapshots.get('ad').items.size,2);
  // 서버가 보낸 stale 신호와 오류를 유지하고, 정상 부분 응답에서는 실제 새 항목만 갱신한다.
  merge([item('b',2,999,at+3,true)],at+3,false,{complete:false,state:'error',error_type:'TimeoutError'});
  assert.strictEqual(state.view(at+3).rows.find(r=>r.id===2).download_speed,0);
  merge([item('b',2,999,at+4,true)],at+4,true,{complete:false});
  assert.strictEqual(state.view(at+4).rows.find(r=>r.id===2).download_speed,0);
  merge([item('b',2,66,at+5)],at+5,true,{complete:false});view=state.view(at+5);
  assert.strictEqual(view.rows.find(r=>r.id===1).download_speed,0);
  assert.strictEqual(view.rows.find(r=>r.id===2).download_speed,66);
  // 정상 완전 목록으로 복구하면 누락된 원격 A는 제거하고 B의 stale을 해제한다.
  merge([item('b',2,77,at+6)],at+6);view=state.view(at+6);
  assert.strictEqual(state.engineSnapshots.get('ad').items.has('a'),false);
  assert.strictEqual(view.rows.find(r=>r.id===2).download_speed,77);
  assert.strictEqual(view.rows.find(r=>r.id===2).engine_stale,false);
  assert.strictEqual(view.rows.find(r=>r.id===3).download_speed,106);
  assert(view.rows.some(r=>r.id===1&&r.status==='downloading'),'DB 행은 엔진 누락으로 삭제하지 않음');
  merge([item('a',1,999,at+5)],at+5);
  assert.strictEqual(state.engineSnapshots.get('ad').items.has('a'),false,'오래된 응답 무시');
  state.requestGeneration=1;assert.strictEqual(merge([originalA],at+7),false,'이전 요청 세대 무시');
  merge([item('b',2,80,at+7)],at+7,false,{config_revision:'v2'},1);
  assert.strictEqual(state.engineSnapshots.get('ad').items.get('b').db_id,undefined,'설정 변경 시 이전 DB 매칭을 계승하지 않음');
  // 미확인 원격 항목의 보존 기간은 마지막 실제 수신 시각부터 300초다.
  const ttl=new S();
  ttl.mergeEngines({success:true,observed_at:at,engines:[snap('ad',[item('old',null,88,at)])]},0);
  ttl.mergeEngines({success:true,observed_at:at+10,engines:[snap('ad',[],false)]},0);
  assert.strictEqual(ttl.view(at+10).rows[0].status,'engine_unverified');
  assert.strictEqual(ttl.view(at+10).rows[0].engine_stale,true);
  ttl.mergeEngines({success:true,observed_at:at+301,engines:[snap('ad',[],false)]},0);
  assert.strictEqual(ttl.engineSnapshots.get('ad').items.size,0);assert.strictEqual(ttl.view(at+301).rows.length,0);
  const strict=new S();strict.mergeDB(db([task(1,'ad','a')],at),0,false);
  strict.mergeEngines({success:true,observed_at:at,engines:[snap('ad',[Object.assign(item('other',1,999,at),{hash:'hash-a'})])]},0);
  assert.strictEqual(strict.view(at).rows[0].download_speed,undefined,'서로 다른 명시적 task ID는 같은 hash로 보강하지 않음');
  console.log('stale speed checks passed');
}

function runMatchChecks() {
  runStaleSpeedChecks();
  const state=new S(), at=Date.now()/1000;
  const item=(id,extra={})=>Object.assign({task_id:id,hash:id,last_seen_at:at,filename:id,download_speed:30},extra);
  const snap=(items,verified,extra={})=>Object.assign({engine_key:'ad',engine_name:'ad',config_revision:'v1',state:'ready',complete:true,items,match_verified:verified},extra);
  const merge=(items,verified,t,extra)=>state.mergeEngines({success:true,observed_at:at+t,engines:[snap(items,verified,extra)]},0);
  merge([item('old')],true,0);
  assert.strictEqual(state.view(at).rows[0].status,'engine_unmanaged');
  merge([item('new')],false,1);
  let view=state.view(at+1);
  assert.strictEqual(view.rows.length,2,'이전 검증 행 보존');
  assert.strictEqual(view.orphanTotal,0);assert.strictEqual(view.unverifiedTotal,2);
  assert(view.rows.every(r=>r.status==='engine_unverified'));
  // 실제 행 렌더러에서 미확인 항목에 조작 버튼이 생기지 않는지 검사한다.
  const main=fs.readFileSync(path.join(root,'static/feeder_download.js'),'utf8');
  ctx.$=$;ctx.window.addEventListener=()=>{};ctx.document={};
  vm.runInContext(main,ctx);
  let rendered=[],stored=[];
  const table={length:1,0:{appendChild(f){stored=f.rows;}},children(selector){
    return {each(fn){if(!selector.includes(':not'))stored.forEach(r=>fn.call(r));},remove(){}};
  }};
  ctx.document={activeElement:null,createDocumentFragment:()=>({rows:[],appendChild(r){this.rows.push(r);}})};
  ctx.$=value=>{
    if(value==='#active_queue_tbody')return table;
    if(typeof value==='string'&&value.startsWith('<tr ')){
      rendered.push(value);const key=value.match(/data-queue-id="([^"]+)"/)[1];
      return [{key,innerHTML:value,remove(){},getAttribute(){return key;}}];
    }
    if(value&&value.key)return {attr:()=>value.key};
    return element(value);
  };
  ctx.render_queue_rows(view.rows);
  assert(rendered.every(html=>!html.includes('adopt_orphan_btn')&&!html.includes('block_orphan_btn')&&!html.includes('queue_action_btn')));
  assert(rendered.every(html=>html.includes('DB 연결 확인')));
  merge([item('new')],true,2);
  view=state.view(at+2);assert.strictEqual(view.rows.length,1);assert.strictEqual(view.rows[0].status,'engine_unmanaged');
  rendered=[];ctx.render_queue_rows(view.rows);assert(rendered[0].includes('adopt_orphan_btn'));assert(rendered[0].includes('block_orphan_btn'));
  // 부분 복구 응답에 포함되지 않은 미확인 항목을 검증된 고아로 승격하지 않는다.
  merge([item('unknown')],false,3);
  merge([item('new')],true,4,{complete:false});
  assert.strictEqual(state.view(at+4).rows.find(r=>r.task_id==='unknown').status,'engine_unverified');
  console.log('F02 match checks passed');
}

function runUploadChecks() {
  let clock=100000,shown=[];
  class FakeDate extends Date {static now(){return clock;}}
  ctx.Date=FakeDate;ctx.$=$;ctx.document={hidden:false};ctx.render_queue_rows=()=>{};
  ctx.render_upload_queue_count=counts=>shown.push(counts);
  const uploads=()=>requests.filter(r=>r.options.url.endsWith('/queue_upload_counts'));
  const last=()=>uploads().slice(-1)[0];
  const advance=(controller,ms)=>{clock+=ms;controller.poll();};
  const d=new ctx.FeederDashboard();d.start();const first=last();
  first.resolve({success:true,state:'loading',counts:null});
  let n=uploads().length;advance(d,4000);assert.strictEqual(uploads().length,n);
  advance(d,1000);assert.strictEqual(uploads().length,n+1);
  last().resolve({success:true,state:'ready',counts:{upload_current:0,upload_total:0}});
  assert.strictEqual(shown[0].upload_current,0);assert.strictEqual(d.uploadPollDelay,15000);
  n=uploads().length;advance(d,14999);assert.strictEqual(uploads().length,n);
  advance(d,1);assert.strictEqual(uploads().length,n+1);
  last().resolve({success:true,state:'stale',counts:null});
  assert.strictEqual(shown.length,1,'이전 정상 0/0 값 보존');
  advance(d,d.uploadPollDelay);last().callbacks.fail();last().callbacks.always();
  assert(d.uploadPollDelay>=30000);assert(elements.get('#queue_upload_state').textValue.includes('갱신 지연'));
  n=uploads().length;advance(d,5000);assert.strictEqual(uploads().length,n);
  d.close();
  // 초기 계산 지연도 30초 뒤에는 점진적으로 확인 간격을 늘린다.
  const slow=new ctx.FeederDashboard();slow.start();last().resolve({success:true,state:'loading',counts:null});
  for(let i=0;i<6;i++){advance(slow,5000);last().resolve({success:true,state:'loading',counts:null});}
  assert.strictEqual(slow.uploadPollDelay,15000);
  n=uploads().length;advance(slow,5000);assert.strictEqual(uploads().length,n);
  advance(slow,10000);last().resolve({success:true,state:'ready',counts:{upload_current:50,upload_total:113}});
  assert.strictEqual(shown.slice(-1)[0].upload_total,113);assert.strictEqual(slow.uploadPollDelay,15000);slow.close();
  // 수치 없는 오류의 재확인은 점진적으로 줄이며 비활성 페이지에서는 중단한다.
  const errors=new ctx.FeederDashboard();errors.start();
  let previous=0;
  for(let i=0;i<5;i++){
    if(i)advance(errors,errors.uploadPollDelay);
    last().callbacks.fail();last().callbacks.always();
    assert(errors.uploadPollDelay>=previous&&errors.uploadPollDelay<=60000);previous=errors.uploadPollDelay;
  }
  assert.strictEqual(errors.uploadPollDelay,60000);
  ctx.document.hidden=true;n=uploads().length;advance(errors,120000);assert.strictEqual(uploads().length,n);
  ctx.document.hidden=false;errors.close();
  // 새로고침 전 요청과 페이지 종료 후 응답은 카드를 덮지 않는다.
  const generation=new ctx.FeederDashboard();generation.start();const old=last();generation.refresh();
  assert(old.aborted);last().resolve({success:true,state:'ready',counts:{upload_current:0,upload_total:0}});
  n=shown.length;old.callbacks.done({success:true,state:'ready',counts:{upload_current:999,upload_total:999}});
  assert.strictEqual(shown.length,n);assert.strictEqual(shown.slice(-1)[0].upload_total,0);
  advance(generation,15000);const late=last();generation.close();
  late.callbacks.done({success:true,state:'ready',counts:{upload_current:888,upload_total:888}});
  assert.strictEqual(shown.length,n);
  console.log('F03 upload polling checks passed');
}

function scrollEnvironment(config={}) {
  const values=new Map(), pending=[], observers=[], intervals=new Set(), pageEvents={};
  let top=500;
  const sentinel={getBoundingClientRect:()=>({top,bottom:top+1})};
  function query(selector) {
    if(!values.has(selector))values.set(selector,{length:1,textValue:'',props:{},attrs:{},visible:false,handlers:new Map()});
    const el=values.get(selector);
    return new Proxy(el,{get(target,key){
      if(key==='text')return value=>{if(value===undefined)return target.textValue;target.textValue=value;return query(selector);};
      if(key==='prop'||key==='attr')return (name,value)=>{const store=key==='prop'?target.props:target.attrs;if(value===undefined)return store[name];store[name]=value;return query(selector);};
      if(key==='toggle')return value=>{target.visible=!!value;return query(selector);};
      if(key==='on')return (name,fn)=>{target.handlers.set(name,(target.handlers.get(name)||[]).concat(fn));return query(selector);};
      if(key==='off')return name=>{String(selector).split(',').forEach(part=>{const v=values.get(part.trim());if(v)v.handlers.delete(name);});return query(selector);};
      if(key==='is')return ()=>false;
      if(key in target)return target[key];
      return ()=>query(selector);
    }});
  }
  query.ajax=options=>{
    const hooks={},request={options,hooks,settled:false,aborted:false};
    for(const name of ['done','fail','always'])request[name]=fn=>{hooks[name]=fn;return request;};
    request.resolve=value=>{request.settled=true;hooks.done(value);hooks.always();};
    request.reject=()=>{request.settled=true;hooks.fail();hooks.always();};
    request.abort=()=>{request.aborted=true;hooks.fail();hooks.always();};
    pending.push(request);return request;
  };
  class Observer {
    constructor(callback,options){if(config.noInit)throw Error('observer unavailable');this.callback=callback;this.options=options;observers.push(this);}
    observe(target){if(config.noObserve)throw Error('observe unavailable');this.target=target;}
    disconnect(){this.disconnected=true;}
    emit(inside){this.callback([{target:sentinel,isIntersecting:inside}]);}
  }
  let timerId=0;
  const sandbox={console,Map,Set,Array,Date,JSON,Number,String,Object,$:query,package_name:'feeder',sub:'download',
    setInterval:()=>{intervals.add(++timerId);return timerId;},clearInterval:id=>intervals.delete(id),
    format_bytes:n=>n+' B',render_upload_queue_count:()=>{},render_queue_rows:rows=>{sandbox.lastRows=rows;if(config.pushAfter40&&rows.length>40)top=2000;},
    document:{hidden:false,getElementById:id=>id==='queue_load_sentinel'?sentinel:null},
    window:{innerHeight:800,IntersectionObserver:config.unsupported?undefined:Observer,addEventListener:(name,fn)=>pageEvents[name]=fn}};
  vm.createContext(sandbox);vm.runInContext(fs.readFileSync(path.join(root,'static/feeder_dashboard.js'),'utf8'),sandbox);
  const controller=new sandbox.FeederDashboard();
  const calls=kind=>pending.filter(r=>r.options.url.endsWith('/'+kind));
  const active=kind=>calls(kind).find(r=>!r.settled&&!r.aborted);
  const click=id=>{const handlers=values.get(id).handlers.get('click.feederQueue')||[];handlers.forEach(fn=>fn());};
  return {sandbox,controller,values,pending,observers,intervals,pageEvents,query,calls,active,click,setTop:value=>top=value};
}
function runLoadingLabelChecks() {
  const e=scrollEnvironment(), c=e.sandbox;
  vm.runInContext(fs.readFileSync(path.join(root,'static/feeder_download.js'),'utf8'),c);
  const rendered=[];
  const table={length:1,0:{appendChild(){}},children(){return {each(){},remove(){}};}};
  c.document.activeElement=null;c.document.createDocumentFragment=()=>({appendChild(){}});
  c.$=value=>{
    if(value==='#active_queue_tbody')return table;
    if(typeof value==='string'&&value.startsWith('<tr ')){rendered.push(value);return [{innerHTML:value}];}
    return e.query(value);
  };
  c.render_queue_rows([
    Object.assign(row(1),{title:'A',progress:75,engine_stale:true,download_speed:0}),
    Object.assign(row(2),{title:'B',engine_stale:false,download_speed:55}),
    Object.assign(row(3),{title:'C',engine_stale:false,download_speed:0}),
    {id:'u',row_key:'engine:ad:u',title:'D',status:'engine_unverified',engine_stale:true,current_engine_name:'ad'},
    Object.assign(row(5,'uploading'),{title:'E',engine_stale:true})]);
  const html=id=>rendered.find(text=>text.includes('data-queue-id="db:'+id+'"'));
  assert.strictEqual((html(1).match(/로딩 중/g)||[]).length,1);
  assert(!html(1).includes('갱신 지연'));assert(!html(1).includes('88 B/s'));
  assert(html(2).includes('55 B/s'));assert(!html(2).includes('로딩 중'));
  assert(html(3).includes('0 B/s'));assert(!html(3).includes('로딩 중'));
  const unknown=rendered.find(text=>text.includes('engine:ad:u'));
  assert(unknown.includes('DB 연결 확인'));assert(!unknown.includes('adopt_orphan_btn')&&!unknown.includes('block_orphan_btn')&&!unknown.includes('queue_action_btn'));
  assert(html(5).includes('로딩 중')&&!html(5).includes('이전 수신 정보 · 실시간 속도 갱신 지연'),'다운로드 외 상태도 간단한 로딩 문구');
  assert(html(1).includes('queue-item-buttons d-flex flex-wrap justify-content-end ml-auto'));
  console.log('U01 loading label checks passed');
}
function runScrollChecks() {
  runLoadingLabelChecks();
  const page=(start,count,total,cursor)=>({success:true,observed_at:now,items:Array.from({length:count},(_,i)=>row(start+i,'pending')),
    counts:{pending:total},total,next_cursor:cursor,removed_ids:[]});
  const e=scrollEnvironment(), d=e.controller;d.start();
  assert.strictEqual(e.observers.length,0,'첫 DB 응답 전 관찰 금지');
  e.active('queue_bootstrap').resolve(page(1,40,785,'c40'));
  assert.strictEqual(d.state.dbItemsById.size,40);let observer=e.observers[0];
  assert.strictEqual(observer.options.root,null);assert.strictEqual(observer.options.rootMargin,'300px 0px');
  assert.strictEqual(e.values.get('#queue_more_btn').visible,false);
  observer.emit(true);const first=e.active('queue_page');
  for(let i=0;i<4;i++)observer.emit(true);
  d.bootstrap();d.render();
  assert.strictEqual(e.calls('queue_page').length,1);assert.strictEqual(e.calls('queue_bootstrap').length,1);
  assert.strictEqual(d.pageState,'loading');assert.strictEqual(e.values.get('#queue_more_btn').props.disabled,true);
  assert.strictEqual(e.values.get('#queue_load_status').textValue,'로딩 중');
  first.resolve(page(41,40,785,'c80'));
  assert.strictEqual(d.state.dbItemsById.size,80);assert.strictEqual(e.calls('queue_page').length,2);
  e.active('queue_page').resolve(page(81,40,785,'c120'));
  assert.strictEqual(d.state.dbItemsById.size,120);assert.strictEqual(e.calls('queue_page').length,2,'같은 가시 구간 두 묶음 제한');
  assert.strictEqual(e.values.get('#queue_more_btn').visible,true);
  // 재진입으로 전체 785건까지 누락·중복 없이 접근한다.
  let guard=0;
  while(d.state.nextCursor&&guard++<30){
    if(!e.active('queue_page')){observer.emit(false);observer.emit(true);}
    const req=e.active('queue_page');assert(req);
    const offset=Number(req.options.data.cursor.slice(1)), count=Math.min(40,785-offset);
    req.resolve(page(offset+1,count,785,offset+count<785?'c'+(offset+count):null));
  }
  assert.strictEqual(d.state.dbItemsById.size,785);assert.strictEqual(d.state.nextCursor,null);
  assert(observer.disconnected);const allCalls=e.calls('queue_page').length;observer.emit(true);assert.strictEqual(e.calls('queue_page').length,allCalls);
  d.close();
  // 추가 행이 sentinel을 화면 밖으로 밀면 다음 실제 교차까지 요청하지 않는다.
  const moved=scrollEnvironment({pushAfter40:true});moved.controller.start();moved.active('queue_bootstrap').resolve(page(1,40,120,'c40'));
  const moving=moved.observers[0];moving.emit(true);moved.active('queue_page').resolve(page(41,40,120,'c80'));
  assert.strictEqual(moved.calls('queue_page').length,1);
  moving.emit(false);moved.setTop(500);moving.emit(true);assert.strictEqual(moved.calls('queue_page').length,2);moved.controller.close();
  // 오류는 자동 재시도를 멈추며 렌더/다른 요청 완료가 오류 버튼을 덮지 않는다.
  const failed=scrollEnvironment(), f=failed.controller;f.start();failed.active('queue_bootstrap').resolve(page(1,40,120,'c40'));
  let fo=failed.observers[0];fo.emit(true);failed.active('queue_page').reject();
  f.render();for(let i=0;i<3;i++)fo.emit(true);
  assert.strictEqual(f.state.dbItemsById.size,40);assert.strictEqual(failed.calls('queue_page').length,1);
  assert.strictEqual(failed.values.get('#queue_more_btn').textValue,'다시 시도');assert(failed.values.get('#queue_more_btn').visible);
  failed.click('#queue_more_btn');failed.setTop(2000);failed.active('queue_page').resolve(page(41,40,120,'c80'));
  assert.strictEqual(f.pageState,'idle');assert.strictEqual(f.state.dbItemsById.size,80);
  fo.emit(false);failed.setTop(500);fo.emit(true);const expired=failed.active('queue_page');expired.resolve({success:false,error:'snapshot_expired'});
  assert(fo.disconnected);assert.strictEqual(failed.calls('queue_bootstrap').length,2);
  failed.active('queue_bootstrap').resolve(page(1,40,120,'c80'));const recovered=failed.observers.slice(-1)[0];
  recovered.emit(true);assert.strictEqual(failed.calls('queue_page').length,3,'만료 재동기화 후 자동 루프 차단');
  assert.strictEqual(f.pageState,'error');expired.hooks.done(page(9999,1,120,null));assert(!f.state.dbItemsById.has('9999'));
  failed.click('#queue_more_btn');failed.active('queue_page').resolve(page(81,40,120,null));assert.strictEqual(f.state.dbItemsById.size,120);f.close();
  // 미지원/생성 실패/관찰 실패에서도 수동 클릭으로 마지막 페이지까지 접근한다.
  for(const config of [{unsupported:true},{noInit:true},{noObserve:true}]){
    const fallback=scrollEnvironment(config), c=fallback.controller;c.start();fallback.active('queue_bootstrap').resolve(page(1,40,81,'c40'));
    assert(fallback.values.get('#queue_more_btn').visible);fallback.click('#queue_more_btn');fallback.active('queue_page').resolve(page(41,40,81,'c80'));
    fallback.click('#queue_more_btn');fallback.active('queue_page').resolve(page(81,1,81,null));assert.strictEqual(c.state.dbItemsById.size,81);c.close();
  }
  const empty=scrollEnvironment();empty.controller.start();empty.active('queue_bootstrap').resolve(page(1,0,0,null));
  assert.strictEqual(empty.observers.length,0);assert.strictEqual(empty.calls('queue_page').length,0);empty.controller.close();
  // 원격 행은 클라이언트 보관분만 40개씩 펼치며 미확인 상태를 유지한다.
  const remote=scrollEnvironment(), r=remote.controller;r.start();remote.active('queue_bootstrap').resolve(page(1,0,0,null));
  remote.active('queue_engines').resolve({success:true,observed_at:now,engines:[{engine_key:'ad',engine_name:'ad',config_revision:'v1',state:'ready',complete:true,match_verified:false,
    items:Array.from({length:180},(_,i)=>({task_id:'u'+i,hash:'h'+i,filename:'u'+i,last_seen_at:now}))}]});
  const engineCalls=remote.calls('queue_engines').length, ro=remote.observers[0];ro.emit(true);
  assert.strictEqual(r.state.orphanLimit,120);assert.strictEqual(r.state.view(now).rows.length,120);assert(remote.values.get('#queue_engine_more_btn').visible);
  assert(r.state.view(now).rows.every(row=>row.status==='engine_unverified'));
  remote.click('#queue_engine_more_btn');assert.strictEqual(r.state.orphanLimit,160);ro.emit(false);ro.emit(true);
  assert.strictEqual(r.state.view(now).rows.length,180);assert.strictEqual(remote.calls('queue_engines').length,engineCalls);
  assert.strictEqual(remote.calls('queue_page').length,0);r.close();
  // refresh/재초기화/pagehide가 관찰자·진행 중 요청·이벤트를 정리한다.
  const life=scrollEnvironment(), l=life.controller;l.start();life.active('queue_bootstrap').resolve(page(1,40,100,'c40'));
  const oldObserver=life.observers[0];oldObserver.emit(true);const late=life.active('queue_page');l.refresh();
  assert(late.aborted&&oldObserver.disconnected);oldObserver.emit(true);late.hooks.done(page(999,1,100,null));assert(!l.state.dbItemsById.has('999'));
  life.active('queue_bootstrap').resolve(page(1,40,100,'c40'));l.start();
  assert.strictEqual(life.intervals.size,1);assert.strictEqual(life.values.get('#queue_more_btn').handlers.get('click.feederQueue').length,1);
  life.active('queue_bootstrap').resolve(page(1,40,100,'c40'));
  vm.runInContext(fs.readFileSync(path.join(root,'static/feeder_download.js'),'utf8'),life.sandbox);
  life.sandbox.feeder_dashboard=l;life.sandbox.apikey='';
  life.sandbox.EventSource=life.sandbox.window.EventSource=function(){this.close=()=>{};};
  life.sandbox.init_sse_listener();life.sandbox.sse_source.onopen();
  l.pageState='error';life.sandbox.sse_source.onopen();life.active('queue_bootstrap').resolve(page(1,40,100,'c40'));
  assert.strictEqual(l.pageState,'error','SSE 재연결도 페이지 오류의 수동 재시도 대기를 유지');
  life.pageEvents.pagehide();assert(l.closed);assert.strictEqual(life.intervals.size,0);
  assert(life.observers.every(o=>o.disconnected));assert.strictEqual(life.values.get('#queue_more_btn').handlers.has('click.feederQueue'),false);
  const before=life.calls('queue_page').length;life.observers.forEach(o=>o.emit(true));assert.strictEqual(life.calls('queue_page').length,before);
  console.log('U02 scroll loading checks passed');
}

const selected=process.argv[2];
if(selected==='--match')runMatchChecks();
else if(selected==='--upload')runUploadChecks();
else if(selected==='--scroll')runScrollChecks();
else {runBaseline();runMatchChecks();runUploadChecks();runScrollChecks();}
