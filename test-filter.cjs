require("dotenv").config();

const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

(async () => {
    const { data, error } = await supabase
        .from("sync_batches")
        .select("id, batch_id, error_message")
        .or(
            "error_message.is.null,error_message.not.like.CONNECTOR\\_OUTCOME\\_UNKNOWN:%"
        )
        .limit(100);

    if (error) {
        console.error("FILTER ERROR:", error.message);
        process.exitCode = 1;
        return;
    }

    const uncertain = data.filter(row =>
        String(row.error_message || "")
            .startsWith("CONNECTOR_OUTCOME_UNKNOWN:")
    );

    console.log("ROWS CHECKED:", data.length);
    console.log("UNCERTAIN ROWS RETURNED:", uncertain.length);
    console.log(
        uncertain.length === 0
            ? "FILTER RESULT: PASSED"
            : "FILTER RESULT: FAILED"
    );

    if (uncertain.length > 0) {
        process.exitCode = 1;
    }
})().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
