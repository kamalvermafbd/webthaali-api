require("dotenv").config();

const { createClient } =
    require("@supabase/supabase-js");


const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

async function resolveWorkerForBatch(batch) {

    if (!batch) {

        throw new Error(
            "Batch is required"
        );
    }

    const {
        company_code,
        tally_owner,
        worker_type
    } = batch;

    if (
        !company_code ||
        !tally_owner
    ) {

        throw new Error(
            "Batch company_code or tally_owner missing"
        );
    }

    /*
     * ---------------------------------------------------------
     * 1. LOAD SYNC PREFERENCE
     * ---------------------------------------------------------
     */

    const { data: configs, error: configError } =
        await supabase
            .from("sync_worker_config")
            .select(`
                id,
                company_code,
                tally_owner,
                sync_preference,
                worker_id,
                server_id,
                is_active
            `)
            .eq(
                "company_code",
                company_code
            )
            .eq(
                "tally_owner",
                tally_owner
            )
            .eq(
                "is_active",
                true
            );

    if (configError) {

        throw new Error(
            "Worker config lookup failed: "
            + configError.message
        );
    }

    if (
        !configs ||
        configs.length === 0
    ) {

        throw new Error(
            `No active worker config found for ${company_code}/${tally_owner}`
        );
    }

    /*
     * ---------------------------------------------------------
     * 2. TRY CONFIGURED WORKER FIRST
     *    Used by SPECIAL_WORKER style mappings.
     * ---------------------------------------------------------
     */

    for (const config of configs) {

        if (!config.worker_id) {
            continue;
        }

        const { data: worker, error: workerError } =
            await supabase
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
                    heartbeat_timeout_seconds
                `)
                .eq(
                    "id",
                    config.worker_id
                )
                .eq(
                    "is_active",
                    true
                )
                .maybeSingle();

        if (workerError) {

            throw new Error(
                "Worker lookup failed: "
                + workerError.message
            );
        }

        if (!worker) {
            continue;
        }

        /*
         * If batch has a worker type,
         * respect it.
         */

        if (
            worker_type &&
            worker.worker_type !== worker_type
        ) {
            continue;
        }

        const serverId =
            config.server_id ||
            worker.server_id;

        if (!serverId) {
            continue;
        }

        const { data: server, error: serverError } =
            await supabase
                .from("worker_servers")
                .select(`
                    id,
                    server_name,
                    http_url,
                    is_active,
                    server_category,
                    machine_name
                `)
                .eq(
                    "id",
                    serverId
                )
                .eq(
                    "is_active",
                    true
                )
                .maybeSingle();

        if (serverError) {

            throw new Error(
                "Worker server lookup failed: "
                + serverError.message
            );
        }

        if (!server) {
            continue;
        }

        return {

            id:
                worker.id,

            worker_name:
                worker.worker_name,

            worker_type:
                worker.worker_type,

            queue_name:
                worker.queue_name,

            server_id:
                server.id,

            server_category:
                server.server_category,

            http_url:
                server.http_url,

            sync_preference:
                config.sync_preference,

            company_code:
                config.company_code,

            tally_owner:
                config.tally_owner,

            concurrency:
                worker.concurrency,

            priority:
                worker.priority,

            heartbeat_timeout_seconds:
                worker.heartbeat_timeout_seconds
        };
    }

    /*
     * ---------------------------------------------------------
     * 3. GENERAL ROUTING
     *
     * GENERAL does not have worker_id.
     * Select active worker by batch worker_type.
     * ---------------------------------------------------------
     */

    const generalConfig =
        configs.find(
            config =>
                config.sync_preference ===
                "GENERAL"
        );

    if (generalConfig) {

        if (!worker_type) {

            throw new Error(
                `worker_type missing for GENERAL batch ${batch.batch_id}`
            );
        }

        const { data: workers, error: workersError } =
            await supabase
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
                    runtime_id,
                    runtime_heartbeat_at
                `)
                .eq(
                    "worker_type",
                    worker_type
                )
                .eq(
                    "is_active",
                    true
                )
                .not(
                    "runtime_id",
                    "is",
                    null
                )
                .order(
                    "priority",
                    {
                        ascending: true
                    }
                )
                .order(
                    "id",
                    {
                        ascending: true
                    }
                );

        if (workersError) {

            throw new Error(
                "General worker lookup failed: "
                + workersError.message
            );
        }

        if (
            !workers ||
            workers.length === 0
        ) {

            throw new Error(
                `No active ${worker_type} worker found`
            );
        }

        /*
         * For GENERAL workers we use the first
         * eligible worker according to priority.
         */

        for (const worker of workers) {

            const { data: server, error: serverError } =
                await supabase
                    .from("worker_servers")
                    .select(`
                        id,
                        server_name,
                        http_url,
                        is_active,
                        server_category,
                        machine_name
                    `)
                    .eq(
                        "id",
                        worker.server_id
                    )
                    .eq(
                        "is_active",
                        true
                    )
                    .maybeSingle();

            if (serverError) {

                throw new Error(
                    "General server lookup failed: "
                    + serverError.message
                );
            }

            if (!server) {
                continue;
            }

            if (
                server.server_category !==
                "GENERAL"
            ) {
                continue;
            }

            return {

                id:
                    worker.id,

                worker_name:
                    worker.worker_name,

                worker_type:
                    worker.worker_type,

                queue_name:
                    worker.queue_name,

                server_id:
                    server.id,

                server_category:
                    server.server_category,

                http_url:
                    server.http_url,

                sync_preference:
                    generalConfig.sync_preference,

                company_code:
                    company_code,

                tally_owner:
                    tally_owner,

                concurrency:
                    worker.concurrency,

                priority:
                    worker.priority,

                heartbeat_timeout_seconds:
                    worker.heartbeat_timeout_seconds
            };
        }
    }

    throw new Error(
        `No eligible worker found for batch ${batch.batch_id}`
    );
}

module.exports = {
    resolveWorkerForBatch
};