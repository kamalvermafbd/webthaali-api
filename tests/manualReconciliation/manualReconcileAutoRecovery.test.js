"use strict";
// Isolated state-machine tests: no .env, worker, connector, Redis or real HTTP.
const { test } = require("node:test");
const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const { randomUUID } = require("node:crypto");
const fs = require("node:fs"), path = require("node:path"), vm = require("node:vm");
const ROOT = path.resolve(__dirname, "../..");
function database(tables) {
    return { from(table) {
        let filters = [], mutation, single = false, limit = Infinity, order;
        const q = {
            select() { return q; }, update(v) { mutation = v; return q; },
            eq(k,v) { filters.push(r => r[k] === v); return q; },
            in(k,v) { filters.push(r => v.includes(r[k])); return q; },
            is(k,v) { filters.push(r => r[k] === v); return q; },
            not(k,op,v) { filters.push(r => r[k] !== v); return q; },
            lt(k,v) { filters.push(r => r[k] < v); return q; },
            gt(k,v) { filters.push(r => r[k] > v); return q; },
            order(k) { order = k; return q; }, limit(v) { limit = v; return q; },
            maybeSingle() { single = true; return q; },
            then(ok,bad) {
                let rows = (tables[table] || []).filter(r => filters.every(f => f(r)));
                if (order) rows = [...rows].sort((a,b) => String(a[order]).localeCompare(String(b[order])));
                rows = rows.slice(0,limit);
                if (mutation) rows.forEach(r => Object.assign(r,mutation));
                return Promise.resolve({data: single ? rows[0] || null : rows,error:null}).then(ok,bad);
            }
        }; return q;
    }};
}
function fixture(options = {}) {
    const id=randomUUID(), token=randomUUID(), tables={
        manual_reconcile_status:[{id:2,company_code:"C0053",tally_owner:"USER",operation_token:token,status:"RUNNING",updated_at:"2020-01-01"}],
        manual_reconcile_attempts:[{attempt_id:id,reservation_id:2,company_code:"C0053",tally_owner:"USER",operation_token:token}],
        manual_reconcile_failure_dispatches:[],manual_reconcile_failure_recovery_evidence:[]
    };
    const db=database(tables), leases=new Set(), rpc=[], sent=[], timers=[], reports=[];
    let successes=0;
    db.rpc=async(name,args)=>{
        rpc.push(name);
        if(name.startsWith("claim_manual_reconcile_auto")){if(leases.has(id))return{data:false};leases.add(id);return{data:true};}
        if(name.startsWith("release_manual_reconcile_auto")){leases.delete(id);return{data:true};}
        if(name.startsWith("renew_manual_reconcile_auto"))return{data:true};
        if(name==="claim_manual_reconcile_failure_dispatch_v1"){
            if(tables.manual_reconcile_failure_dispatches.length)return{data:false};
            tables.manual_reconcile_failure_dispatches.push({attempt_id:id,dispatch_token:args.p_dispatch_token,dispatch_state:"CLAIMED",execution_result:null,execution_finished_at:null});return{data:true};
        }throw Error(name);
    };
    const context={module:{exports:{}},Buffer,URL,Date,
        process:{env:{MANUAL_RECONCILE_AUTO_FAILURE_DISPATCH_ENABLED:options.disabled?"false":"true",MANUAL_RECONCILE_INTERNAL_RECOVERY_TOKEN:"synthetic-token-0000000000000000000000",MANUAL_RECONCILE_RECOVERY_INTERNAL_URL:"http://127.0.0.1:3001/reconcileRecovery"}},
        setInterval(){return{unref(){}};},clearInterval(){},clearTimeout(){},
        setTimeout(callback,ms){const t={callback,ms};timers.push(t);return t;},
        require(name){
            if(name==="http")return{request(url,config,cb){
                const req=new EventEmitter();req.destroy=()=>{};
                req.end=body=>{
                    sent.push(JSON.parse(body));
                    if(options.timeout){timers.find(t=>t.ms===15000).callback();return;}
                    const dispatch=tables.manual_reconcile_failure_dispatches.find(r=>r.attempt_id===id);
                    if(dispatch?.dispatch_state==="DISPATCH_STARTED")dispatch.dispatch_state="OUTCOME_UNCERTAIN";
                    const res=new EventEmitter();res.destroy=()=>{};res.statusCode=options.recovered?200:409;cb(res);
                    res.emit("data",JSON.stringify({attempt_id:id,success:!!options.recovered,recovered:!!options.recovered,recovery_state:options.reason||"ORIGINAL_COORDINATOR_DURABLE_RECEIPT_REQUIRED"}));res.emit("end");
                };return req;
            }};
            if(name==="./inspectManualReconcileRecovery")return{inspectManualReconcileRecovery:async()=>({state:options.state||"EXECUTION_UNCERTAIN",reservationOwned:true})};
            if(name==="./recoverManualReconciliation")return{recoverManualReconciliation:async()=>{successes++;return{recovered:true,reason:"CLOSED"};}};
            return require(name);
        }
    };
    vm.runInNewContext(fs.readFileSync(path.join(ROOT,"services/manualReconciliation/manualReconcileAutoRecovery.js"),"utf8"),context);
    const watchdog=context.module.exports.createManualReconcileAutoRecovery({supabase:db,batchSize:options.batchSize||25,onResult:r=>reports.push(r)});watchdog.start();
    return{id,tables,db,leases,rpc,sent,reports,watchdog,successes:()=>successes};
}
async function run(h){await h.watchdog.runOnce();h.watchdog.stop();}
test("normal verified completion closes without failure dispatch",async()=>{const h=fixture({state:"FINALIZATION_RECORDED"});await run(h);assert.equal(h.successes(),1);assert.equal(h.leases.size,0);assert.equal(h.sent.length,0);});
test("stale RUNNING creates one durable dispatch and does not replay uncertain execution",async()=>{const h=fixture();await h.watchdog.runOnce();await run(h);assert.equal(h.sent.length,1);assert.equal(h.tables.manual_reconcile_failure_dispatches.length,1);});
test("disabled gate is actionable and creates no permanent claim",async()=>{const h=fixture({disabled:true});await run(h);assert.equal(h.reports[0].reason,"FAILURE_DISPATCH_DISABLED");assert.ok(h.reports[0].action);assert.equal(h.tables.manual_reconcile_failure_dispatches.length,0);});
test("missing exit receipt error reaches watchdog unchanged",async()=>{const h=fixture();await run(h);assert.ok(h.reports.some(r=>r.reason==="ORIGINAL_COORDINATOR_DURABLE_RECEIPT_REQUIRED"&&!r.recovered));});
test("absolute API timeout retains uncertain lease",async()=>{const h=fixture({timeout:true});await run(h);assert.equal(h.leases.size,1);assert.ok(h.reports.some(r=>r.reason==="FAILURE_DISPATCH_OUTCOME_UNCERTAIN"));});
for(const state of ["CLAIMED","DISPATCH_STARTED"]){test(`crash at ${state} reuses original token despite retained lease`,async()=>{const h=fixture(),token=randomUUID();h.leases.add(h.id);h.tables.manual_reconcile_failure_dispatches.push({attempt_id:h.id,dispatch_token:token,dispatch_state:state});await run(h);assert.equal(h.sent[0].dispatch_token,token);assert.equal(h.rpc.length,0);assert.equal(h.tables.manual_reconcile_failure_dispatches.length,1);});}
test("existing uncertain RUNNING dispatch requires operator verification",async()=>{const h=fixture();h.tables.manual_reconcile_failure_dispatches.push({attempt_id:h.id,dispatch_token:randomUUID(),dispatch_state:"OUTCOME_UNCERTAIN"});await run(h);assert.equal(h.sent.length,0);assert.equal(h.reports[0].reason,"FAILURE_EXISTING_DISPATCH_REQUIRES_VERIFICATION");assert.ok(h.reports[0].action);});
for(const state of ["OUTCOME_UNCERTAIN","RESUME_CLAIMED"]){for(const result of [null,"ERROR","INCOMPLETE","RECOVERED"]){test(`SQL closed ${state}/${result} delegates atomic admission to API`,async()=>{const h=fixture({recovered:true});Object.assign(h.tables.manual_reconcile_status[0],{status:"FAILED",operation_token:null,closed_at:"2026-11-20",last_terminal_attempt_id:h.id});h.tables.manual_reconcile_failure_dispatches.push({attempt_id:h.id,dispatch_token:randomUUID(),dispatch_state:state,execution_result:result});h.tables.manual_reconcile_failure_recovery_evidence.push({attempt_id:h.id,recovery_state:"SETTLED",remote_execution_fenced:true,redis_cleanup_confirmed:true,redis_stage_receipt:{},redis_stage_confirmed_at:"2026-11-20",terminal_closed_at:"2026-11-20",redis_unblock_confirmed_at:result==="RECOVERED"?"2026-11-20":null});await run(h);assert.equal(h.sent.length,1);});}}
test("duplicate runOnce shares one scan",async()=>{const h=fixture();const a=h.watchdog.runOnce(),b=h.watchdog.runOnce();assert.equal(a,b);await a;h.watchdog.stop();assert.equal(h.sent.length,1);});
test("continuation cursor moves past old RUNNING dispatches",async()=>{const h=fixture({batchSize:1});h.tables.manual_reconcile_status=[];h.tables.manual_reconcile_failure_dispatches.push({attempt_id:"00000000-0000-4000-8000-000000000001",dispatch_token:randomUUID(),dispatch_state:"OUTCOME_UNCERTAIN"},{attempt_id:h.id,dispatch_token:randomUUID(),dispatch_state:"RESUME_CLAIMED"});Object.assign(h.tables.manual_reconcile_attempts[0],{reservation_id:3});h.tables.manual_reconcile_status.push({id:3,company_code:"C0053",tally_owner:"USER",status:"FAILED",operation_token:null,closed_at:"2026-11-20",last_terminal_attempt_id:h.id});h.tables.manual_reconcile_failure_recovery_evidence.push({attempt_id:h.id,recovery_state:"SETTLED",remote_execution_fenced:true,redis_cleanup_confirmed:true,redis_stage_receipt:{},redis_stage_confirmed_at:"2026-11-20",terminal_closed_at:"2026-11-20"});await h.watchdog.runOnce();await run(h);assert.equal(h.sent.length,1);});
