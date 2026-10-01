require("dotenv").config();

const crypto = require("crypto");
const { createClient } = require("@supabase/supabase-js");

const { Resend } =
    require("resend");

const resend =
    new Resend(
        process.env.RESEND_API_KEY
    );

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

const RUN_INTERVAL_MS =
    30 * 60 * 1000; // 30 minutes

let worker = null;
let runtimeId = null;
let runtimeHeartbeat = null;
let schedulerRunning = false;


// =========================================================
// LOAD WORKER
// =========================================================

async function loadWorker() {

    const workerId =
        process.argv[2];

    if (!workerId) {
        throw new Error(
            "Auto Email Worker ID required"
        );
    }

    const {
        data,
        error
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
            heartbeat_timeout_seconds
        `)
        .eq(
            "id",
            workerId
        )
        .eq(
            "is_active",
            true
        )
        .single();

    if (error || !data) {
        throw new Error(
            "Active auto email worker not found: " +
            (error?.message || workerId)
        );
    }

    return data;
}


// =========================================================
// CLAIM RUNTIME
// =========================================================

async function claimRuntime() {

    runtimeId =
        crypto.randomUUID();

    const {
        data,
        error
    } = await supabase.rpc(
        "claim_worker_runtime",
        {
            p_worker_id:
                worker.id,

            p_runtime_id:
                runtimeId
        }
    );

    if (
        error ||
        !data ||
        data.length === 0
    ) {

        throw new Error(
            `Worker runtime already active or claim failed: ${worker.worker_name}`
        );
    }

    console.log(
        "AUTO EMAIL WORKER RUNTIME CLAIMED:",
        runtimeId
    );
}


// =========================================================
// HEARTBEAT
// =========================================================

function startRuntimeHeartbeat() {

    runtimeHeartbeat =
        setInterval(
            async () => {

                try {

                    const now =
                        new Date();

                    const {
                        error
                    } = await supabase
                        .from("workers")
                        .update({
                            runtime_heartbeat_at:
                                now,

                            updated_at:
                                now
                        })
                        .eq(
                            "id",
                            worker.id
                        )
                        .eq(
                            "runtime_id",
                            runtimeId
                        )
                        .eq(
                            "is_active",
                            true
                        );

                    if (error) {

                        console.error(
                            "AUTO EMAIL WORKER HEARTBEAT ERROR:",
                            error.message
                        );

                        return;
                    }

                    console.log(
                        "AUTO EMAIL WORKER HEARTBEAT:",
                        now.toISOString()
                    );

                } catch (error) {

                    console.error(
                        "AUTO EMAIL WORKER HEARTBEAT EXCEPTION:",
                        error.message
                    );
                }

            },
            30 * 1000
        );
}

// =========================================================
// GMAIL REGISTRATION NOTIFICATION
// =========================================================

async function notifyMissingGmail(company) {

    const companyEmail =
        String(company.email || "").trim();

    if (!companyEmail) {

        console.log(
            "COMPANY EMAIL MISSING:",
            company.company_code
        );

        return;
    }

    const lastNotified =
        company.gmail_auto_scheduler_last_notified_at
            ? new Date(
                company.gmail_auto_scheduler_last_notified_at
            )
            : null;

    const oneDayAgo =
        Date.now() -
        (24 * 60 * 60 * 1000);

    if (
        lastNotified &&
        !Number.isNaN(lastNotified.getTime()) &&
        lastNotified.getTime() >= oneDayAgo
    ) {

        console.log(
            "GMAIL NOTIFICATION ALREADY SENT TODAY:",
            company.company_code
        );

        return;
    }

    const subject =
        "Auto Email Scheduler - Email Registration Required";

    const message = `
Dear ${company.businessname || "Customer"},

Auto Email Scheduler is not active for your company.

Please register/connect your email ID with the Auto Email Scheduler from Company Settings to enable automatic customer email reminders.

Regards,
Billey
`.trim();

    try {

     const result =
    await resend.emails.send({

        from:
            "Billey <noreply@billey.in>",

        to:
            companyEmail,

        subject,

        text:
            message

    });

if (result.error) {

    throw new Error(
        result.error.message ||
        "Resend email failed"
    );
}

        const {
            error: updateError
        } = await supabase
            .from("company")
            .update({
                gmail_auto_scheduler_last_notified_at:
                    new Date().toISOString()
            })
            .eq(
                "company_code",
                company.company_code
            );

        if (updateError) {

            console.error(
                "GMAIL NOTIFICATION TIMESTAMP UPDATE ERROR:",
                company.company_code,
                updateError.message
            );

            return;
        }

        console.log(
            "GMAIL REGISTRATION NOTIFICATION SENT:",
            company.company_code,
            companyEmail
        );

    } catch (error) {

        console.error(
            "GMAIL REGISTRATION NOTIFICATION FAILED:",
            company.company_code,
            error.message
        );

    }
}

// =========================================================
// AUTO EMAIL PROCESSOR
// =========================================================

async function runAutoEmailProcessor() {

    console.log(
        "=============================================="
    );

    console.log(
        "AUTO EMAIL PROCESSOR STARTED:",
        new Date().toISOString()
    );

    console.log(
        "=============================================="
    );


   // =========================================================
// LOAD ACTIVE COMPANIES
// =========================================================

const {
    data: companies,
    error: companyError
} = await supabase
    .from("company")
    .select(`
        company_code,
        businessname,
        email,
        is_active,
        gmail_email,
        gmail_refresh_token,
        gmail_connected,
        gmail_auto_scheduler_last_notified_at
    `)
    .eq(
        "is_active",
        true
    )
    .order(
        "company_code",
        {
            ascending: true
        }
    );


if (companyError) {

    throw new Error(
        "ACTIVE COMPANY FETCH FAILED: " +
        companyError.message
    );
}


console.log(
    "ACTIVE COMPANIES FOUND:",
    companies?.length || 0
);


for (
    const company
    of companies || []
) {

    console.log(
        "AUTO EMAIL COMPANY CHECK:",
        company.company_code,
        company.businessname
    );


    const gmailRegistered =
        company.gmail_connected === true &&
        Boolean(
            String(
                company.gmail_email || ""
            ).trim()
        ) &&
        Boolean(
            String(
                company.gmail_refresh_token || ""
            ).trim()
        );


    if (!gmailRegistered) {

    console.log(
        "GMAIL NOT REGISTERED:",
        company.company_code
    );

    await notifyMissingGmail(company);

    continue;
}


    console.log(
        "GMAIL REGISTERED:",
        company.company_code,
        company.gmail_email
    );
}
}


// =========================================================
// SCHEDULER
// =========================================================

async function runScheduler() {

    if (schedulerRunning) {

        console.log(
            "AUTO EMAIL SCHEDULER ALREADY RUNNING — SKIP"
        );

        return;
    }

    schedulerRunning = true;

    try {

        await runAutoEmailProcessor();

    } catch (error) {

        console.error(
            "AUTO EMAIL PROCESSOR ERROR:",
            error
        );

    } finally {

        schedulerRunning = false;
    }
}


// =========================================================
// CLEANUP
// =========================================================

async function cleanupRuntime() {

    console.log(
        "AUTO EMAIL WORKER CLEANUP STARTED"
    );

    if (runtimeHeartbeat) {

        clearInterval(
            runtimeHeartbeat
        );

        runtimeHeartbeat = null;
    }

    if (!worker || !runtimeId) {
        return;
    }

    try {

        const {
            error
        } = await supabase
            .from("workers")
            .update({
                runtime_id: null,
                runtime_started_at: null,
                runtime_heartbeat_at: null,
                updated_at: new Date()
            })
            .eq(
                "id",
                worker.id
            )
            .eq(
                "runtime_id",
                runtimeId
            );

        if (error) {

            console.error(
                "AUTO EMAIL WORKER CLEANUP ERROR:",
                error.message
            );

        } else {

            console.log(
                "AUTO EMAIL WORKER RUNTIME RELEASED"
            );
        }

    } catch (error) {

        console.error(
            "AUTO EMAIL WORKER CLEANUP EXCEPTION:",
            error.message
        );
    }
}


// =========================================================
// START
// =========================================================

async function startWorker() {

    worker =
        await loadWorker();

    console.log(
        "=============================================="
    );

    console.log(
        "AUTO EMAIL WORKER STARTING"
    );

    console.log(
        "WORKER NAME:",
        worker.worker_name
    );

    console.log(
        "WORKER TYPE:",
        worker.worker_type
    );

    console.log(
        "SERVER ID:",
        worker.server_id
    );

    console.log(
        "=============================================="
    );


    await claimRuntime();

    startRuntimeHeartbeat();


    // Run immediately after startup
    await runScheduler();


    // Then every 30 minutes
    setInterval(
        async () => {

            await runScheduler();

        },
        RUN_INTERVAL_MS
    );

}


// =========================================================
// PROCESS SIGNALS
// =========================================================

process.once(
    "SIGINT",
    async () => {

        await cleanupRuntime();

        process.exit(0);
    }
);


process.once(
    "SIGTERM",
    async () => {

        await cleanupRuntime();

        process.exit(0);
    }
);


// =========================================================
// BOOT
// =========================================================

startWorker()
    .catch(
        async error => {

            console.error(
                "AUTO EMAIL WORKER START FAILED:",
                error
            );

            await cleanupRuntime();

            process.exit(1);
        }
    );