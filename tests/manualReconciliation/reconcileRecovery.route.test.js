"use strict";
// Runs only the extracted route with in-memory SQL CAS doubles.
const { test } = require("node:test");
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const fs = require("node:fs"),path=require("node:path"),vm=require("node:vm");
const source=fs.readFileSync(path.resolve(__dirname,"../../secondServer.js"),"utf8");
const start=source.indexOf('app.post("/reconcileRecovery",');
const end=source.indexOf("\n});",start)+5;
assert.ok(start>=0&&end>start);
function harness(options={}){
    const attempt=randomUUID(),token=randomUUID(),secret="synthetic-test-secret-0000000000000000";
    const dispatch={attempt_id:attempt,dispatch_token:token,dispatch_state:options.state||"DISPATCH_STARTED",execution_finished_at:null,execution_result:null,resume_claim_token:null,resume_claimed_at:options.stale?"2020-01-01":new Date().toISOString()};
    let handler,calls=0,resumeCalls=0;
    const supabase={
        from(table){assert.equal(table,"manual_reconcile_failure_dispatches");let filters=[],change;
            const q={update(v){change=v;return q;},eq(k,v){filters.push(()=>dispatch[k]===v);return q;},is(k,v){filters.push(()=>dispatch[k]===v);return q;},select(){return q;},then(ok,bad){const matches=filters.every(f=>f());if(matches)Object.assign(dispatch,change);return Promise.resolve({data:matches?[{attempt_id:attempt}]:[],error:null}).then(ok,bad);}};return q;
        },
        async rpc(name,args){assert.equal(name,"claim_manual_reconcile_failure_resume_v1");resumeCalls++;
            // This models the required contract, not proof of deployed SQL.
            const admitted=options.sqlClosed===true&&(
                dispatch.dispatch_state==="OUTCOME_UNCERTAIN"||
                (dispatch.dispatch_state==="RESUME_CLAIMED"&&options.stale));
            if(admitted)Object.assign(dispatch,{dispatch_state:"RESUME_CLAIMED",resume_claim_token:args.p_resume_claim_token,execution_finished_at:null,execution_result:null});
            return{data:admitted,error:null};
        }
    };
    const ctx={app:{post(route,fn){handler=fn;}},supabase,Buffer,Date,require,console:{error(){}},process:{env:{MANUAL_RECONCILE_RECOVERY_ENABLED:"true",MANUAL_RECONCILE_FAILURE_RECOVERY_ENABLED:"true",MANUAL_RECONCILE_INTERNAL_RECOVERY_TOKEN:secret}},
        async recoverFailedManualReconciliation(){calls++;if(options.throwRecovery)throw Error("injected crash");return options.blocked?{recovered:false,reason:options.blocked}:{recovered:true,reason:"FAILURE_SETTLED_REDIS_UNBLOCK_SQL_CONFIRMED"};},
        async recoverManualReconciliation(){throw Error("unexpected success path");}
    };
    vm.runInNewContext(source.slice(start,end),ctx);
    async function invoke(){let status=200,body;const res={status(s){status=s;return res;},json(b){body=b;return res;}};await handler({socket:{remoteAddress:"127.0.0.1"},body:{attempt_id:attempt,recovery_type:"FAILURE",dispatch_token:token},get(){return secret;}},res);return{status,body};}
    return{dispatch,invoke,calls:()=>calls,resumeCalls:()=>resumeCalls};
}
test("duplicate initial watchdog dispatch cannot execute recovery twice",async()=>{const h=harness();const results=await Promise.all([h.invoke(),h.invoke()]);assert.equal(h.calls(),1);assert.ok(results.some(r=>r.status===200));assert.ok(results.some(r=>r.status===409));assert.equal(h.dispatch.dispatch_state,"RECOVERY_CONFIRMED");});
test("missing durable exit receipt stays incomplete with actionable response",async()=>{const h=harness({blocked:"ORIGINAL_COORDINATOR_DURABLE_RECEIPT_REQUIRED"});const r=await h.invoke();assert.equal(r.status,409);assert.match(r.body.operator_action,/original coordinator/);assert.equal(h.dispatch.execution_result,"INCOMPLETE");assert.equal(h.dispatch.dispatch_state,"OUTCOME_UNCERTAIN");});
test("crashed initial API has no RUNNING replay admission",async()=>{const h=harness({state:"OUTCOME_UNCERTAIN"});const r=await h.invoke();assert.equal(r.status,409);assert.equal(h.calls(),0);});
test("stale RESUME_CLAIMED cannot resume without SQL closure",async()=>{const h=harness({state:"RESUME_CLAIMED",stale:true});assert.equal((await h.invoke()).status,409);assert.equal(h.calls(),0);});
test("SQL-closed stale resume uses token CAS then confirms",async()=>{const h=harness({state:"RESUME_CLAIMED",stale:true,sqlClosed:true});assert.equal((await h.invoke()).status,200);assert.equal(h.resumeCalls(),1);assert.equal(h.dispatch.dispatch_state,"RECOVERY_CONFIRMED");});
test("Redis cleanup failure after SQL closure returns resumable incomplete outcome",async()=>{const h=harness({state:"OUTCOME_UNCERTAIN",sqlClosed:true,blocked:"SQL_CLOSED_REDIS_UNBLOCK_UNCERTAIN"});assert.equal((await h.invoke()).status,409);assert.equal(h.dispatch.execution_result,"INCOMPLETE");assert.equal(h.dispatch.dispatch_state,"OUTCOME_UNCERTAIN");assert.equal(h.dispatch.resume_claim_token,null);});
test("SQL-closed error releases only owned resume claim",async()=>{const h=harness({state:"OUTCOME_UNCERTAIN",sqlClosed:true,throwRecovery:true});assert.equal((await h.invoke()).status,503);assert.equal(h.dispatch.execution_result,"ERROR");assert.equal(h.dispatch.dispatch_state,"OUTCOME_UNCERTAIN");});

test("SQL-closed cleanup can retry successfully after Redis becomes available", async () => {
    const options = { state: "OUTCOME_UNCERTAIN", sqlClosed: true,
        blocked: "SQL_CLOSED_REDIS_UNBLOCK_UNCERTAIN" };
    const h = harness(options);
    assert.equal((await h.invoke()).status, 409);
    assert.equal(h.dispatch.execution_result, "INCOMPLETE");
    delete options.blocked;
    assert.equal((await h.invoke()).status, 200);
    assert.equal(h.dispatch.execution_result, "RECOVERED");
    assert.equal(h.dispatch.dispatch_state, "RECOVERY_CONFIRMED");
    assert.equal(h.calls(), 2);
});
