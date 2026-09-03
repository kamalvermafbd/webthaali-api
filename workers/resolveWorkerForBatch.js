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
/* 030926
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
                    heartbeat_timeout_seconds,
                    runtime_id,
                    runtime_heartbeat_at
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

        if (
    !worker.runtime_id ||
    !worker.runtime_heartbeat_at
) {
    continue;
}

const heartbeatTimeout =
    worker.heartbeat_timeout_seconds || 120;

const heartbeatAge =
    Date.now() -
    new Date(worker.runtime_heartbeat_at).getTime();

if (
    heartbeatAge >
    heartbeatTimeout * 1000
) {
    continue;
}

        

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

        const { count: runningCount, error: batchError } =
    await supabase
        .from("sync_batches")
        .select(
            "id",
            {
                count: "exact",
                head: true
            }
        )
        .eq(
            "worker_id",
            worker.worker_name
        )
        .in(
            "worker_status",
            [
                "PENDING",
                "RUNNING"
            ]
        );

if (batchError) {
    throw new Error(
        "Worker batch load lookup failed: "
        + batchError.message
    );
}

const currentLoad =
    runningCount || 0;

const concurrency =
    worker.concurrency || 1;

if (currentLoad >= concurrency) {
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
*/

/*
 * ---------------------------------------------------------
 * 2. ROUTING BY SYNC PREFERENCE
 * ---------------------------------------------------------
 */

const configuredConfig =
    configs.find(
        config =>
            config.sync_preference === "SPECIAL_WORKER"
    );


/*
 * ---------------------------------------------------------
 * 2A. SPECIAL WORKER
 *
 * DB decides exact worker.
 * No fallback to another worker.
 * ---------------------------------------------------------
 */

if (configuredConfig) {

    if (
        !configuredConfig.worker_id ||
        !configuredConfig.server_id
    ) {

        throw new Error(
            `Invalid SPECIAL_WORKER configuration for ${company_code}/${tally_owner}`
        );
    }

    const {
        data: worker,
        error: workerError
    } = await supabase
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
            "id",
            configuredConfig.worker_id
        )
        .eq(
            "is_active",
            true
        )
        .maybeSingle();

    if (workerError) {

        throw new Error(
            "Worker lookup failed: " +
            workerError.message
        );
    }

    if (!worker) {

        throw new Error(
            `Configured worker ${configuredConfig.worker_id} is not active`
        );
    }

    if (
        worker_type &&
        worker.worker_type !== worker_type
    ) {

        throw new Error(
            `Configured worker type mismatch for ${company_code}/${tally_owner}`
        );
    }

    if (
        worker.server_id !==
        configuredConfig.server_id
    ) {

        throw new Error(
            `Configured worker/server mismatch for ${company_code}/${tally_owner}`
        );
    }

    const {
        data: server,
        error: serverError
    } = await supabase
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
            configuredConfig.server_id
        )
        .eq(
            "is_active",
            true
        )
        .maybeSingle();

    if (serverError) {

        throw new Error(
            "Worker server lookup failed: " +
            serverError.message
        );
    }

    if (!server) {

        throw new Error(
            `Configured server ${configuredConfig.server_id} is not active`
        );
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
            configuredConfig.sync_preference,

        company_code:
            configuredConfig.company_code,

        tally_owner:
            configuredConfig.tally_owner,

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
 * 2B. SPECIAL SERVER
 *
 * DB decides exact server.
 * Any eligible worker on that server.
 * No cross-server fallback.
 * ---------------------------------------------------------
 */

const specialServerConfig =
    configs.find(
        config =>
            config.sync_preference === "SPECIAL_SERVER"
    );

if (specialServerConfig) {

    if (!specialServerConfig.server_id) {

        throw new Error(
            `Invalid SPECIAL_SERVER configuration for ${company_code}/${tally_owner}`
        );
    }

    if (!worker_type) {

        throw new Error(
            `worker_type missing for GENERAL batch ${batch.batch_id}`
        );
    }

    const {
        data: server,
        error: serverError
    } = await supabase
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
            specialServerConfig.server_id
        )
        .eq(
            "is_active",
            true
        )
        .maybeSingle();

    if (serverError) {

        throw new Error(
            "Worker server lookup failed: " +
            serverError.message
        );
    }

    if (!server) {

        throw new Error(
            `Configured server ${specialServerConfig.server_id} is not active`
        );
    }

    const {
        data: workers,
        error: workersError
    } = await supabase
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
            "server_id",
            specialServerConfig.server_id
        )
        .eq(
            "worker_type",
            worker_type
        )
        .eq(
            "is_active",
            true
        );

    if (workersError) {

        throw new Error(
            "Special server worker lookup failed: " +
            workersError.message
        );
    }

    if (
        !workers ||
        workers.length === 0
    ) {

        throw new Error(
            `No active ${worker_type} worker found on configured server ${specialServerConfig.server_id}`
        );
    }

    const eligibleWorkers = [];

    for (const worker of workers) {

        if (
            !worker.runtime_id ||
            !worker.runtime_heartbeat_at
        ) {
            continue;
        }

        const heartbeatTimeout =
            worker.heartbeat_timeout_seconds || 120;

        const heartbeatAge =
            Date.now() -
            new Date(
                worker.runtime_heartbeat_at
            ).getTime();

        if (
            heartbeatAge >
            heartbeatTimeout * 1000
        ) {
            continue;
        }

        const {
            count: runningCount,
            error: batchError
        } = await supabase
            .from("sync_batches")
            .select(
                "id",
                {
                    count: "exact",
                    head: true
                }
            )
            .eq(
                "worker_id",
                worker.worker_name
            )
            .in(
                "worker_status",
                [
                    "PENDING",
                    "RUNNING"
                ]
            );

        if (batchError) {

            throw new Error(
                "Worker batch load lookup failed: " +
                batchError.message
            );
        }

        const currentLoad =
            runningCount || 0;

        const concurrency =
            worker.concurrency || 1;

        if (
            currentLoad >= concurrency
        ) {
            continue;
        }

        eligibleWorkers.push({
            worker,
            currentLoad,
            availableSlots:
                concurrency -
                currentLoad
        });
    }

    if (
        eligibleWorkers.length === 0
    ) {

        throw new Error(
            `No available ${worker_type} worker on configured server ${specialServerConfig.server_id}`
        );
    }

    eligibleWorkers.sort(
        (a, b) => {

            if (
                a.currentLoad !==
                b.currentLoad
            ) {
                return (
                    a.currentLoad -
                    b.currentLoad
                );
            }

            if (
                a.availableSlots !==
                b.availableSlots
            ) {
                return (
                    b.availableSlots -
                    a.availableSlots
                );
            }

            if (
                a.worker.priority !==
                b.worker.priority
            ) {
                return (
                    a.worker.priority -
                    b.worker.priority
                );
            }

            return (
                a.worker.id -
                b.worker.id
            );
        }
    );

    const selected =
        eligibleWorkers[0];

    const worker =
        selected.worker;

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
            specialServerConfig.sync_preference,

        company_code:
            specialServerConfig.company_code,

        tally_owner:
            specialServerConfig.tally_owner,

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
                `worker_type missing for SPECIAL_SERVER batch ${batch.batch_id}`
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
/*
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

        */



                /*
         * ---------------------------------------------------------
         * SELECT GENERAL WORKER BY CURRENT LOAD
         *
         * Each worker is DB-configured.
         * No worker name/id is hardcoded here.
         *
         * Prefer worker having:
         * 1. available concurrency
         * 2. lower current load
         * 3. lower priority
         * 4. lower id
         * ---------------------------------------------------------
         */

        const eligibleWorkers = [];

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

            if (
                !worker.runtime_id ||
                !worker.runtime_heartbeat_at
            ) {
                continue;
            }

            const heartbeatTimeout =
                worker.heartbeat_timeout_seconds || 120;

            const heartbeatAge =
                Date.now() -
                new Date(worker.runtime_heartbeat_at).getTime();

            if (
                heartbeatAge >
                heartbeatTimeout * 1000
            ) {
                continue;
            }

            /*
             * Count batches currently assigned/running
             * to this worker.
             *
             * sync_batches.worker_id stores worker_name.
             */

            const { count: runningCount, error: batchError } =
                await supabase
                    .from("sync_batches")
                    .select(
                        "id",
                        {
                            count: "exact",
                            head: true
                        }
                    )
                    .eq(
                        "worker_id",
                        worker.worker_name
                    )
                    .in(
                        "worker_status",
                        [
                            "PENDING",
                            "RUNNING"
                        ]
                    );

            if (batchError) {

                throw new Error(
                    "Worker batch load lookup failed: "
                    + batchError.message
                );
            }

            const currentLoad =
                runningCount || 0;

            const concurrency =
                worker.concurrency || 1;

            /*
             * Worker has no available execution slot.
             */

            if (
                currentLoad >= concurrency
            ) {
                continue;
            }

            eligibleWorkers.push({

                worker,

                server,

                currentLoad,

                availableSlots:
                    concurrency -
                    currentLoad
            });
        }

        if (
            eligibleWorkers.length === 0
        ) {

            throw new Error(
                `No available ${worker_type} worker found for batch ${batch.batch_id}`
            );
        }

        /*
         * Prefer:
         * lowest current load
         * then higher available capacity
         * then priority
         * then worker id
         */

        eligibleWorkers.sort(
            (a, b) => {

                if (
                    a.currentLoad !==
                    b.currentLoad
                ) {
                    return (
                        a.currentLoad -
                        b.currentLoad
                    );
                }

                if (
                    a.availableSlots !==
                    b.availableSlots
                ) {
                    return (
                        b.availableSlots -
                        a.availableSlots
                    );
                }

                if (
                    a.worker.priority !==
                    b.worker.priority
                ) {
                    return (
                        a.worker.priority -
                        b.worker.priority
                    );
                }

                return (
                    a.worker.id -
                    b.worker.id
                );
            }
        );

        const selected =
            eligibleWorkers[0];

        const worker =
            selected.worker;

        const server =
            selected.server;

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

    throw new Error(
        `No eligible worker found for batch ${batch.batch_id}`
    );
}

module.exports = {
    resolveWorkerForBatch
};