const BatchStatusManager =
    require("../sync-engine/BatchStatusManager");

class StageManager {

    // ----------------------------------
    // Update Stage
    // ----------------------------------

    async update({

        batch_id,

        module,

        entity = null,

        stage,

        operation = null

    }) {

        return BatchStatusManager.updateModule({

            batch_id,

            module,

            entity,

            action: stage,

            operation

        });

    }

    // ----------------------------------
    // Mark Entity Completed
    // ----------------------------------

    async complete({

        batch_id,

        module,

        entity

    }) {

        return BatchStatusManager.updateModule({

            batch_id,

            module,

            entity,

            action: "COMPLETED"

        });

    }

}

module.exports =
    new StageManager();