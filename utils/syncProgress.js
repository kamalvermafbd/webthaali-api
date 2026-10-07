async function updateSyncProgress({
    supabase,
    batchId,
    stage,
    progress,
    action = null
}) {
    if (!supabase) {
        console.warn("⚠️ SYNC PROGRESS: supabase missing");
        return;
    }

    if (!batchId) {
        console.warn("⚠️ SYNC PROGRESS: batchId missing");
        return;
    }

    try {
        const updateData = {
            sync_progress: {
                stage,
                progress,
                updated_at: new Date().toISOString()
            },
            current_stage: stage
        };

        if (action) {
            updateData.current_action = action;
        }

        const { error } = await supabase
            .from("sync_batches")
            .update(updateData)
            .eq("batch_id", batchId);

        if (error) {
            console.error(
                "❌ SYNC PROGRESS UPDATE ERROR:",
                error.message
            );
            return;
        }

        console.log(
            "📊 SYNC PROGRESS:",
            {
                batchId,
                stage,
                progress
            }
        );

    } catch (err) {
        console.error(
            "❌ SYNC PROGRESS HELPER ERROR:",
            err.message
        );
    }
}

module.exports = {
    updateSyncProgress
};