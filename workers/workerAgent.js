// workers/workerAgent.js

const { execFile } = require("child_process");

require("dotenv").config();

const os = require("os");
const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

async function loadAgent() {

    const hostname = os.hostname();

    const { data: agents, error } = await supabase
        .from("worker_agents")
        .select(`
            id,
            agent_name,
            machine_name,
            server_id,
            is_active
        `)
        .eq("is_active", true);

    if (error) {
        throw error;
    }

    if (!agents || agents.length === 0) {
        throw new Error(
            "No active worker agent configured"
        );
    }

    /*
     * Agent identity is resolved from the DB.
     * agent_name is used only as a DB registration identifier.
     */
    const agent = agents.find(
    item => item.machine_name === hostname
);
    if (!agent) {

        throw new Error(
            `No worker agent registered for machine ${hostname}`
        );
    }

    return agent;
}

async function loadWorkers(serverId) {

    const { data, error } = await supabase
        .from("workers")
        .select(`
            id,
            worker_name,
            worker_type,
            queue_name,
            server_id,
            is_active,
            concurrency,
            priority,
            heartbeat_timeout_seconds,
            pm2_process_name,
            script_path
        `)
        .eq("server_id", serverId);

    if (error) {
        throw error;
    }

    return data || [];
}

function runPM2(args) {
    return new Promise((resolve, reject) => {

        execFile("pm2", args, (error, stdout, stderr) => {

            if (error) {
                reject(error);
                return;
            }

            resolve(stdout);
        });

    });
}

async function getPM2Process(processName) {

    try {

        const output = await runPM2([
            "jlist"
        ]);

        const processes = JSON.parse(output);

        return processes.find(
            process => process.name === processName
        );

    } catch (error) {

        console.error(
            "PM2 status check failed:",
            error.message
        );

        return null;
    }
}

async function reconcileWorker(worker) {

    const pm2Process = await getPM2Process(
        worker.pm2_process_name
    );

    const isRunning =
        pm2Process &&
        ["online", "launching"].includes(
            pm2Process.pm2_env?.status
        );

    if (worker.is_active && !isRunning) {

        console.log(
            "DRY RUN: WOULD START",
            worker.pm2_process_name,
            "WORKER ID:",
            worker.id
        );

        return;
    }

    if (!worker.is_active && isRunning) {

        console.log(
            "DRY RUN: WOULD STOP",
            worker.pm2_process_name,
            "WORKER ID:",
            worker.id
        );

        return;
    }

    console.log(
        "DRY RUN: NO ACTION",
        worker.pm2_process_name
    );
}

async function reconcileWorkers() {

    const agent = await loadAgent();

    const workers = await loadWorkers(
        agent.server_id
    );

    console.log(
        "\nWorker Agent:",
        new Date().toISOString()
    );

    console.log(
        "Agent:",
        agent.agent_name,
        "Server ID:",
        agent.server_id
    );

    for (const worker of workers) {

        console.log({
            id: worker.id,
            worker_name: worker.worker_name,
            worker_type: worker.worker_type,
            queue_name: worker.queue_name,
            pm2_process_name: worker.pm2_process_name,
            script_path: worker.script_path,
            is_active: worker.is_active,
            concurrency: worker.concurrency,
            priority: worker.priority
        });

        await reconcileWorker(worker);

    }
}

async function start() {

    console.log(
        "Worker Agent Starting..."
    );

    await reconcileWorkers();

    setInterval(
        async () => {

            try {

                await reconcileWorkers();

            } catch (error) {

                console.error(
                    "Worker Agent Reconcile Error:",
                    error.message
                );

            }

        },
        30000
    );
}

start().catch(error => {

    console.error(
        "Worker Agent Fatal Error:",
        error
    );

    process.exit(1);
});