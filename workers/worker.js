require("dotenv").config();

const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

const {
    runJob
} = require("../jobs/jobRunner");

const WORKER_ID =
    "worker-" + Math.random().toString(36).substring(2, 10);


async function pickJob() {

    const { data, error } = await supabase
        .from("sync_batches")
        .select("*")
        .eq("batch_status", "PENDING")
        .eq("worker_status", "PENDING")
        .order("priority", {
            ascending: true
        })
        .order("created_at", {
            ascending: true
        })
        .limit(1)
        .maybeSingle();


    if (error) {

        console.error(
            "Job pickup error:",
            error
        );

        return null;
    }


    return data;

}



async function lockJob(job) {


    const { data, error } = await supabase
        .from("sync_batches")
        .update({

            batch_status:
                "PROCESSING",

            worker_status:
                "RUNNING",

            worker_id:
                WORKER_ID,

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



    if(error){

        console.error(
            "Lock failed:",
            error
        );

        return null;
    }


    return data;

}



async function workerLoop(){


    console.log(
        "Worker Started:",
        WORKER_ID
    );


    while(true){


        const job =
            await pickJob();


        if(!job){

            console.log(
                "No pending jobs..."
            );

            await sleep(5000);

            continue;
        }



        const lockedJob =
            await lockJob(job);



        if(!lockedJob){

            continue;
        }



    console.log(
    "JOB LOCKED:",
    lockedJob.batch_id
);


await runJob(lockedJob);


await sleep(5000);

    }

}



function sleep(ms){

    return new Promise(
        resolve => setTimeout(resolve, ms)
    );

}



workerLoop();