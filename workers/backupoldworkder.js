require("dotenv").config();

const { createClient } = require("@supabase/supabase-js");

const crypto = require("crypto");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

const {
    runJob
} = require("../jobs/jobRunner");

/*
const WORKER_NAME = process.env.WORKER_NAME;

if (!WORKER_NAME) {
    throw new Error("WORKER_NAME missing in environment");
}
*/
async function pickJob(workerConfig) {

    const startOfDay = new Date();
    startOfDay.setHours(0, 0, 0, 0);

    const startOfTomorrow = new Date(startOfDay);
    startOfTomorrow.setDate(startOfTomorrow.getDate() + 1);

    /*
     * =========================================================
     * SERVER CATEGORY
     * =========================================================
     */

    const serverCategory =
        workerConfig.server_category;

    if (!serverCategory) {

        console.error(
            "Server category missing for server:",
            workerConfig.server_id
        );

        return null;
    }

    /*
     * =========================================================
     * SPECIAL WORKER SERVER
     * =========================================================
     */

    if (serverCategory === "SPECIAL_WORKER") {

        const { data: configs, error: configError } =
            await supabase
                .from("sync_worker_config")
                .select(
                    "company_code, tally_owner"
                )
                .eq("is_active", true)
                .eq(
                    "sync_preference",
                    "SPECIAL_WORKER"
                )
                .eq(
                    "worker_id",
                    workerConfig.id
                )
                .eq(
                    "server_id",
                    workerConfig.server_id
                );

        if (configError) {

            console.error(
                "Special worker config error:",
                configError
            );

            return null;
        }

        if (!configs || configs.length === 0) {
            return null;
        }

        const conditions =
            configs.map(config =>
                `and(company_code.eq.${config.company_code},tally_owner.eq.${config.tally_owner})`
            );

        const { data, error } =
            await supabase
                .from("sync_batches")
                .select("*")
                .eq(
                    "batch_status",
                    "PENDING"
                )
                .eq(
                    "worker_status",
                    "PENDING"
                )
                .eq(
                    "worker_type",
                    workerConfig.worker_type
                )
                .gte(
                    "created_at",
                    startOfDay.toISOString()
                )
                .lt(
                    "created_at",
                    startOfTomorrow.toISOString()
                )
                .or(
                    conditions.join(",")
                )
               .order("id", {
                    ascending: true
                })
                .limit(1)
                .maybeSingle();

        if (error) {

            console.error(
                "Special worker job pickup error:",
                error
            );

            return null;
        }

        return data;
    }

    /*
     * =========================================================
     * SPECIAL SERVER
     * =========================================================
     */

    if (serverCategory === "SPECIAL_SERVER") {

        const { data: configs, error: configError } =
            await supabase
                .from("sync_worker_config")
                .select(
                    "company_code, tally_owner"
                )
                .eq("is_active", true)
                .eq(
                    "sync_preference",
                    "SPECIAL_SERVER"
                )
                .eq(
                    "server_id",
                    workerConfig.server_id
                );

        if (configError) {

            console.error(
                "Special server config error:",
                configError
            );

            return null;
        }

        if (!configs || configs.length === 0) {
            return null;
        }

        const conditions =
            configs.map(config =>
                `and(company_code.eq.${config.company_code},tally_owner.eq.${config.tally_owner})`
            );

        const { data, error } =
            await supabase
                .from("sync_batches")
                .select("*")
                .eq(
                    "batch_status",
                    "PENDING"
                )
                .eq(
                    "worker_status",
                    "PENDING"
                )
                .eq(
                    "worker_type",
                    workerConfig.worker_type
                )
                .gte(
                    "created_at",
                    startOfDay.toISOString()
                )
                .lt(
                    "created_at",
                    startOfTomorrow.toISOString()
                )
                .or(
                    conditions.join(",")
                )
                .order("id", {
                    ascending: true
                })
                .limit(1)
                .maybeSingle();

        if (error) {

            console.error(
                "Special server job pickup error:",
                error
            );

            return null;
        }

        return data;
    }

    /*
     * =========================================================
     * GENERAL SERVER
     * =========================================================
     */

    if (serverCategory === "GENERAL") {

        const { data: configs, error: configError } =
            await supabase
                .from("sync_worker_config")
                .select(
                    "company_code, tally_owner"
                )
                .eq("is_active", true)
                .eq(
                    "sync_preference",
                    "GENERAL"
                );

        if (configError) {

            console.error(
                "General worker config error:",
                configError
            );

            return null;
        }

        if (!configs || configs.length === 0) {
            return null;
        }

        const conditions =
            configs.map(config =>
                `and(company_code.eq.${config.company_code},tally_owner.eq.${config.tally_owner})`
            );

        const { data, error } =
            await supabase
                .from("sync_batches")
                .select("*")
                .eq(
                    "batch_status",
                    "PENDING"
                )
                .eq(
                    "worker_status",
                    "PENDING"
                )
                .eq(
                    "worker_type",
                    workerConfig.worker_type
                )
                .gte(
                    "created_at",
                    startOfDay.toISOString()
                )
                .lt(
                    "created_at",
                    startOfTomorrow.toISOString()
                )
                .or(
                    conditions.join(",")
                )
                .order("id", {
                        ascending: true
                    })
                .limit(1)
                .maybeSingle();

        if (error) {

            console.error(
                "General job pickup error:",
                error
            );

            return null;
        }

        return data;
    }

    /*
     * =========================================================
     * UNKNOWN SERVER CATEGORY
     * =========================================================
     */

    console.error(
        "Unknown server category:",
        serverCategory
    );

    return null;
}

async function lockJob(job, workerConfig) {

    const { data, error } = await supabase
        .from("sync_batches")
        .update({

            batch_status:
                "PROCESSING",

            worker_status:
                "RUNNING",

            worker_id:
                workerConfig.worker_name,

            locked_at:
                new Date(),

            started_at:
                new Date(),

            heartbeat_at:
                new Date()

        })
        .eq("id", job.id)
        .eq("worker_status", "PENDING")
        .select()
        .single();

    if (error) {

        console.error(
            "Lock failed:",
            error
        );

        return null;
    }

    return data;
}

async function startHeartbeat(
    jobId,
    workerConfig
) {

    console.log(
        "HEARTBEAT STARTED:",
        jobId
    );

    const heartbeat = setInterval(async () => {

        console.log(
            "HEARTBEAT TICK:",
            new Date().toISOString()
        );

        const now = new Date();

        const { data, error } = await supabase
            .from("sync_batches")
            .update({
                heartbeat_at: now,
                last_activity_at: now
            })
            .eq("id", jobId)
            .eq("worker_status", "RUNNING")
            .select("id, heartbeat_at, last_activity_at");

        if (error) {

            console.error(
                "Heartbeat update error:",
                error
            );

            return;
        }

        console.log(
            "HEARTBEAT UPDATED:",
            data
        );

                await supabase
            .from("workers")
            .update({
                runtime_heartbeat_at: now
            })
            .eq(
                "runtime_id",
                workerConfig.runtime_id
            );


    }, 30 * 1000);

    return heartbeat;
}

async function recoverStaleJobs(workerConfig) {

    const staleBefore = new Date(
    Date.now() -
        workerConfig.heartbeat_timeout_seconds * 1000
    ).toISOString();


    const { data, error } = await supabase
        .from("sync_batches")
        .update({
            batch_status: "PENDING",
            worker_status: "PENDING",
            worker_id: null,
            locked_at: null,
            started_at: null,
            heartbeat_at: null,
            last_activity_at: null
        })
        .eq("batch_status", "PROCESSING")
        .eq("worker_status", "RUNNING")
        .lt("heartbeat_at", staleBefore)
        .select();

    if (error) {

        console.error(
            "Stale job recovery error:",
            error
        );

        return;
    }

    if (data && data.length > 0) {

        console.log(
            "STALE JOBS RECOVERED:",
            data.length
        );
    }
}

/*
async function loadWorkerConfig() {

    const { data, error } = await supabase
    .from("workers")
    .select(`
        *,
        worker_servers (
            http_url,
            server_category
        )
    `)
    .eq("worker_name", WORKER_NAME)
    .eq("is_active", true)
    .single();


    if (error || !data) {
        throw new Error(
            "Active worker configuration not found: "
            + WORKER_NAME
        );
    }

    if (!data.worker_servers?.http_url) {
        throw new Error(
            "Worker server HTTP URL not found for: "
            + WORKER_NAME
        );
    }

    return data;
}
*/

async function loadWorkerConfig() {

    const runtimeId =
        crypto.randomUUID();

    const { data, error } =
        await supabase.rpc(
            "claim_worker_runtime",
            {
                p_runtime_id:
                    runtimeId
            }
        );

    if (error) {

        throw new Error(
            "Worker runtime claim failed: "
            + error.message
        );
    }

    if (!data || data.length === 0) {

        throw new Error(
            "No available worker slot configured"
        );
    }

    const workerConfig = data[0];

    workerConfig.runtime_id =
        runtimeId;

    return workerConfig;
}

async function workerLoop(){

    const workerConfig =
        await loadWorkerConfig();

    console.log(
        "Worker Started:",
        workerConfig.worker_name
    );

    console.log(
        "Worker Type:",
        workerConfig.worker_type
    );

    console.log(
        "Queue:",
        workerConfig.queue_name
    );

    console.log(
        "Server ID:",
        workerConfig.server_id
    );


    while(true){

    await recoverStaleJobs(workerConfig);

    const job =
         await pickJob(workerConfig);

   
        if(!job){

            console.log(
                "No pending jobs..."
            );

            await sleep(5000);

            continue;
        }



        const lockedJob =
                await lockJob(job, workerConfig);



        if(!lockedJob){

            continue;
        }



    console.log(
    "JOB LOCKED:",
    lockedJob.batch_id
);


const heartbeat =
    await startHeartbeat(
        lockedJob.id,
        workerConfig
    );

try {

    await runJob(
        lockedJob,
        workerConfig.http_url
    );

} catch (error) {

    console.error(
        "JOB EXECUTION FAILED:",
        lockedJob.batch_id,
        error
    );

} finally {

    clearInterval(heartbeat);

}

await sleep(5000);

    }

}



function sleep(ms){

    return new Promise(
        resolve => setTimeout(resolve, ms)
    );

}



workerLoop();