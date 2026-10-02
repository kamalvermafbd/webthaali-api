require("dotenv").config();

const crypto = require("crypto");
const { createClient } = require("@supabase/supabase-js");

const { Resend } =
    require("resend");

const {
    sendGmailEmail
} = require("../utils/gmailSender");

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
        gmail_auto_scheduler_last_notified_at,
        license_from,
        license_till
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

    const today = new Date().toISOString().slice(0, 10);

const subscriptionActive =
    company.license_from &&
    company.license_till &&
    today >= String(company.license_from).slice(0, 10) &&
    today <= String(company.license_till).slice(0, 10);

if (!subscriptionActive) {
    console.log(
        "AUTO EMAIL SKIP - SUBSCRIPTION INACTIVE:",
        company.company_code
    );
    continue;
}

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


        // =========================================================
    // LOAD AUTO EMAIL SETTINGS
    // =========================================================

    const {
        data: settingsRows,
        error: settingsError
    } = await supabase
        .from("fund_flow_auto_email_settings")
        .select(`
            company_code,
            tally_owner,
            ledger_guid,
            auto_email_enabled,
            before_due_enabled,
            before_due_days,
            on_due_date_enabled,
            email_time
        `)
        .eq(
            "company_code",
            company.company_code
        );

    if (settingsError) {

        console.error(
            "AUTO EMAIL SETTINGS FETCH FAILED:",
            company.company_code,
            settingsError.message
        );

        continue;
    }

    console.log(
        "AUTO EMAIL SETTINGS FOUND:",
        company.company_code,
        settingsRows?.length || 0
    );

        // =========================================================
    // PROCESS ENABLED AUTO EMAIL SETTINGS
    // =========================================================

        for (const settings of settingsRows || []) {

        if (settings.auto_email_enabled !== true) {
            continue;
        }

        console.log(
            "AUTO EMAIL ENABLED:",
            company.company_code,
            settings.tally_owner,
            settings.ledger_guid
        );

            // =====================================================
        // LOAD FUND FLOW FOR THIS LEDGER
        // =====================================================

        const asOfDate =
            new Date().toISOString().slice(0, 10);

        const {
            data: fundFlowRows,
            error: fundFlowError
        } = await supabase.rpc(
            "get_fund_flow_core",
            {
                p_company_code: company.company_code,
                p_tally_owner: settings.tally_owner,
                p_as_of_date: asOfDate
            }
        );

        if (fundFlowError) {

            console.error(
                "FUND FLOW FETCH FAILED:",
                company.company_code,
                settings.ledger_guid,
                fundFlowError.message
            );

            continue;
        }

        const ledgerRows =
            (fundFlowRows || []).filter(
                row =>
                    row.ledger_guid === settings.ledger_guid &&
                    row.party_type === "CUSTOMER"
            );

        console.log(
            "FUND FLOW ROWS FOUND:",
            company.company_code,
            settings.ledger_guid,
            ledgerRows.length
        );

        if (!ledgerRows.length) {
            continue;
        }

                // =====================================================
        // FIND DUE REMINDER INVOICES
        // =====================================================

        const today =
            new Date().toISOString().slice(0, 10);

        const eligibleRows = [];

        for (const row of ledgerRows) {

            if (!row.due_date || !row.bill_name) {
                continue;
            }

            const dueDate =
                String(row.due_date).slice(0, 10);

            const dueDateMs =
                new Date(`${dueDate}T00:00:00`).getTime();

            const todayMs =
                new Date(`${today}T00:00:00`).getTime();

            const daysUntilDue =
                Math.round(
                    (dueDateMs - todayMs) /
                    (24 * 60 * 60 * 1000)
                );

            let emailType = null;

            if (
                settings.before_due_enabled === true &&
                daysUntilDue === Number(settings.before_due_days)
            ) {
                emailType = "BEFORE_DUE";
            }

            if (
                settings.on_due_date_enabled === true &&
                daysUntilDue === 0
            ) {
                emailType = "ON_DUE_DATE";
            }

            if (!emailType) {
                continue;
            }

            eligibleRows.push({
                ...row,
                email_type: emailType
            });
        }

        console.log(
            "AUTO EMAIL ELIGIBLE ROWS:",
            company.company_code,
            settings.ledger_guid,
            eligibleRows.length
        );

        if (!eligibleRows.length) {
            continue;
        }

                // =====================================================
        // CHECK AUTO EMAIL LOG
        // =====================================================

        const rowsToSend = [];

        for (const row of eligibleRows) {

            const { data: existingLog, error: logCheckError } =
                await supabase
                    .from("fund_flow_auto_email_log")
                    .select("id, status")
                    .eq("company_code", company.company_code)
                    .eq("tally_owner", settings.tally_owner)
                    .eq("ledger_guid", settings.ledger_guid)
                    .eq("bill_name", row.bill_name)
                    .eq("due_date", row.due_date)
                    .eq("email_type", row.email_type)
                    .maybeSingle();

            if (logCheckError) {

                console.error(
                    "AUTO EMAIL LOG CHECK FAILED:",
                    company.company_code,
                    row.bill_name,
                    logCheckError.message
                );

                continue;
            }

            if (
                existingLog &&
                (
                    existingLog.status === "SENT" ||
                    existingLog.status === "PROCESSING"
                )
            ) {
                console.log(
                    "AUTO EMAIL ALREADY PROCESSED:",
                    company.company_code,
                    row.bill_name,
                    row.email_type
                );

                continue;
            }

            rowsToSend.push(row);
        }

        console.log(
            "AUTO EMAIL ROWS READY TO SEND:",
            company.company_code,
            settings.ledger_guid,
            rowsToSend.length
        );

        if (!rowsToSend.length) {
            continue;
        }

                // =====================================================
        // CHECK EMAIL TIME
        // =====================================================

        const currentTime =
            new Date().toTimeString().slice(0, 5);

        const configuredEmailTime =
            String(settings.email_time || "09:00:00")
                .slice(0, 5);

        if (currentTime < configuredEmailTime) {

            console.log(
                "AUTO EMAIL WAITING FOR EMAIL TIME:",
                company.company_code,
                settings.ledger_guid,
                "CURRENT:",
                currentTime,
                "CONFIGURED:",
                configuredEmailTime
            );

            continue;
        }

                // =====================================================
        // GET CUSTOMER EMAIL
        // =====================================================

        const { data: debtorData, error: debtorError } =
            await supabase.rpc(
                "get_debtor_email_summary",
                {
                    p_company_code: company.company_code,
                    p_tally_owner: settings.tally_owner,
                    p_opening_date: asOfDate,
                    p_as_of_date: asOfDate
                }
            );

        if (debtorError) {

            console.error(
                "DEBTOR EMAIL SUMMARY FETCH FAILED:",
                company.company_code,
                settings.ledger_guid,
                debtorError.message
            );

            continue;
        }

        const debtor =
            (debtorData || []).find(
                row =>
                    row.ledger_guid ===
                    settings.ledger_guid
            );

        if (!debtor) {

            console.log(
                "DEBTOR NOT FOUND:",
                company.company_code,
                settings.ledger_guid
            );

            continue;
        }

        const recipientEmail =
            String(debtor.email || "").trim();

        if (!recipientEmail) {

            console.log(
                "DEBTOR EMAIL MISSING:",
                company.company_code,
                settings.ledger_guid
            );

            continue;
        }

        console.log(
            "AUTO EMAIL RECIPIENT:",
            company.company_code,
            recipientEmail
        );

            // =====================================================
// BUILD AUTO EMAIL MESSAGE
// =====================================================

const totalOutstanding =
    Number(debtor.total_outstanding || 0);

const totalOverdue =
    Number(debtor.overdue_amount || 0);

const fallingDueAmount =
    Math.max(
        totalOutstanding - totalOverdue,
        0
    );

const customerRows =
    ledgerRows.filter(
        row =>
            row.party_type === "CUSTOMER" &&
            Number(row.net_cash_required || 0) > 0
    );

const overdueInvoiceRows =
    customerRows
        .filter(
            row => row.status === "OVERDUE"
        )
        .sort(
            (a, b) =>
                String(a.due_date || "")
                    .localeCompare(
                        String(b.due_date || "")
                    )
        );

const fallingDueRows =
    customerRows
        .filter(
            row => row.status === "UPCOMING"
        )
        .sort(
            (a, b) =>
                String(a.due_date || "")
                    .localeCompare(
                        String(b.due_date || "")
                    )
        );

const UPCOMING_ALERT_DAYS = 7;

function formatEmailDate(dateValue) {

    if (!dateValue) {
        return "-";
    }

    const d =
        new Date(
            `${String(dateValue).slice(0, 10)}T00:00:00`
        );

    if (Number.isNaN(d.getTime())) {
        return String(dateValue);
    }

    return d.toLocaleDateString(
        "en-IN",
        {
            day: "2-digit",
            month: "short",
            year: "numeric"
        }
    );
}

function getDaysUntilDue(dueDate) {

    if (!dueDate) {
        return 0;
    }

    const dueDateMs =
        new Date(
            `${String(dueDate).slice(0, 10)}T00:00:00`
        ).getTime();

    const todayMs =
        new Date(
            `${today}T00:00:00`
        ).getTime();

    return Math.max(
        Math.floor(
            (dueDateMs - todayMs) /
            (24 * 60 * 60 * 1000)
        ),
        0
    );
}

const nearDueRows =
    fallingDueRows.filter(
        row =>
            getDaysUntilDue(row.due_date) <=
            UPCOMING_ALERT_DAYS
    );

let messageIntro = "";


// =====================================================
// TRIGGER-SPECIFIC MESSAGE
// =====================================================

// BEFORE DUE
if (
    rowsToSend.some(
        row => row.email_type === "BEFORE_DUE"
    )
) {

    const beforeDueTriggerRows =
        rowsToSend.filter(
            row =>
                row.email_type === "BEFORE_DUE"
        );

    if (
        beforeDueTriggerRows.length === 1
    ) {

        const row =
            beforeDueTriggerRows[0];

        const days =
            getDaysUntilDue(
                row.due_date
            );

        const dueDate =
            formatEmailDate(
                row.due_date
            );

        const amount =
            Number(
                row.net_cash_required || 0
            );

        messageIntro =
            `This is a reminder that Invoice ${row.bill_name} for ₹${amount.toFixed(2)} is due in ${days} day${days === 1 ? "" : "s"} on ${dueDate}. We request you to kindly arrange the payment by the due date.`;

    } else {

        const triggerAmount =
            beforeDueTriggerRows.reduce(
                (sum, row) =>
                    sum +
                    Number(
                        row.net_cash_required || 0
                    ),
                0
            );

        messageIntro =
            `This is a reminder that ₹${triggerAmount.toFixed(2)} is due within the next ${Number(settings.before_due_days)} day${Number(settings.before_due_days) === 1 ? "" : "s"}. We request you to kindly arrange the payment by the respective due date.`;
    }


// ON DUE DATE
} else if (
    rowsToSend.some(
        row => row.email_type === "ON_DUE_DATE"
    )
) {

    const dueTodayRows =
        rowsToSend.filter(
            row =>
                row.email_type === "ON_DUE_DATE"
        );

    if (
        dueTodayRows.length === 1
    ) {

        const row =
            dueTodayRows[0];

        const amount =
            Number(
                row.net_cash_required || 0
            );

        messageIntro =
            `This is a reminder that Invoice ${row.bill_name} for ₹${amount.toFixed(2)} is due today. We request you to kindly arrange the payment today.`;

    } else {

        const dueTodayAmount =
            dueTodayRows.reduce(
                (sum, row) =>
                    sum +
                    Number(
                        row.net_cash_required || 0
                    ),
                0
            );

        messageIntro =
            `This is a reminder that invoices totaling ₹${dueTodayAmount.toFixed(2)} are due today. We request you to kindly arrange the payment today.`;
    }
}


// =====================================================
// EMAIL BODY
// =====================================================

let message =
    `Dear ${debtor.ledger_name || "Customer"},\n\n` +
    `This is a reminder regarding your account with ${company.businessname || "Billey"}.\n\n` +
    `${messageIntro}\n\n`;


// =====================================================
// OUTSTANDING STATUS
// =====================================================

message +=
    `OUTSTANDING STATUS\n` +
    `------------------\n` +
    `Total Outstanding : ₹${totalOutstanding.toFixed(2)}\n` +
    `Total Overdue     : ₹${totalOverdue.toFixed(2)}\n` +
    `Total Falling Due : ₹${fallingDueAmount.toFixed(2)}\n\n`;


// =====================================================
// OVERDUE DETAILS
// =====================================================

if (
    overdueInvoiceRows.length > 0
) {

    message +=
        `OVERDUE DETAILS\n` +
        `---------------\n`;

    for (
        const row
        of overdueInvoiceRows
    ) {

        const dueDate =
            formatEmailDate(
                row.due_date
            );

        const invoiceDate =
            formatEmailDate(
                row.voucher_date
            );

        const amount =
            Number(
                row.net_cash_required || 0
            );

        const delayDays =
            Math.max(
                Math.floor(
                    (
                        new Date(
                            `${today}T00:00:00`
                        ).getTime() -
                        new Date(
                            `${String(row.due_date).slice(0, 10)}T00:00:00`
                        ).getTime()
                    ) /
                    (24 * 60 * 60 * 1000)
                ),
                0
            );

        message +=
            `Invoice: ${row.bill_name}\n` +
            `Invoice Date: ${invoiceDate}\n` +
            `Due Date: ${dueDate}\n` +
            `Outstanding: ₹${amount.toFixed(2)}\n` +
            `Delay: ${delayDays} day${delayDays === 1 ? "" : "s"}\n\n`;
    }
}


// =====================================================
// FALLING DUE / UPCOMING
// =====================================================

if (
    fallingDueRows.length > 0
) {

    message +=
        `FALLING DUE / UPCOMING\n` +
        `----------------------\n`;

    for (
        const row
        of fallingDueRows
    ) {

        const dueDate =
            formatEmailDate(
                row.due_date
            );

        const invoiceDate =
            formatEmailDate(
                row.voucher_date
            );

        const amount =
            Number(
                row.net_cash_required || 0
            );

        const dueIn =
            getDaysUntilDue(
                row.due_date
            );

        message +=
            `Invoice: ${row.bill_name}\n` +
            `Invoice Date: ${invoiceDate}\n` +
            `Due Date: ${dueDate}\n` +
            `Outstanding: ₹${amount.toFixed(2)}\n` +
            `Due In: ${dueIn} day${dueIn === 1 ? "" : "s"}\n\n`;
    }
}


message +=
    `Regards,\n` +
    `${company.businessname || "Billey"}`;

                    // =====================================================
        // CREATE PROCESSING LOGS
        // =====================================================

        const processingLogIds = [];

        for (const row of rowsToSend) {

            const { data: logRow, error: logInsertError } =
                await supabase
                    .from("fund_flow_auto_email_log")
                    .insert({
                        company_code: company.company_code,
                        tally_owner: settings.tally_owner,
                        ledger_guid: settings.ledger_guid,
                        bill_name: row.bill_name,
                        due_date: row.due_date,
                        email_type: row.email_type,
                        recipient_email: recipientEmail,
                        status: "PROCESSING"
                    })
                    .select("id")
                    .single();

            if (logInsertError) {

                console.error(
                    "AUTO EMAIL PROCESSING LOG FAILED:",
                    company.company_code,
                    row.bill_name,
                    row.email_type,
                    logInsertError.message
                );

                continue;
            }

            processingLogIds.push(logRow.id);
        }

        console.log(
            "AUTO EMAIL PROCESSING LOGS CREATED:",
            company.company_code,
            processingLogIds.length
        );

        if (!processingLogIds.length) {
            continue;
        }

                // =====================================================
        // SEND AUTO EMAIL THROUGH GMAIL
        // =====================================================

        try {

            const emailSubject =
                rowsToSend.length === 1
                    ? `Outstanding Payment Reminder - ${debtor.ledger_name || "Customer"}`
                    : `Outstanding Payment Reminder - ${debtor.ledger_name || "Customer"}`;

            const gmailResult =
                await sendGmailEmail({
                    gmail_email:
                        company.gmail_email,

                    gmail_refresh_token:
                        company.gmail_refresh_token,

                    to:
                        recipientEmail,

                    subject:
                        emailSubject,

                    message
                });

            console.log(
                "AUTO EMAIL SENT:",
                company.company_code,
                recipientEmail,
                gmailResult.message_id
            );

            // =================================================
            // MARK LOGS AS SENT
            // =================================================

            const { error: sentUpdateError } =
                await supabase
                    .from("fund_flow_auto_email_log")
                    .update({
                        status: "SENT",
                        sent_at: new Date().toISOString(),
                        error_message: null
                    })
                    .in(
                        "id",
                        processingLogIds
                    );

            if (sentUpdateError) {

                console.error(
                    "AUTO EMAIL SENT LOG UPDATE FAILED:",
                    company.company_code,
                    sentUpdateError.message
                );

            }

        } catch (emailError) {

            console.error(
                "AUTO EMAIL SEND FAILED:",
                company.company_code,
                recipientEmail,
                emailError.message
            );

            // ===============================================
            // MARK LOGS AS FAILED
            // ===============================================

            const { error: failedUpdateError } =
                await supabase
                    .from("fund_flow_auto_email_log")
                    .update({
                        status: "FAILED",
                        error_message:
                            String(emailError.message || "Email send failed")
                    })
                    .in(
                        "id",
                        processingLogIds
                    );

            if (failedUpdateError) {

                console.error(
                    "AUTO EMAIL FAILED LOG UPDATE ERROR:",
                    company.company_code,
                    failedUpdateError.message
                );
            }
        }

    }
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