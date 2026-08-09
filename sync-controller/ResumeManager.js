const EntityPipeline =
    require("./EntityPipeline");

class ResumeManager {

    // ----------------------------------
    // Resolve Sync Mode
    // ----------------------------------

    async resolve({

          runningBatch

    }) {

       
        if (!runningBatch) {

            return {

                mode: "NEW",

                batch: null,

                 startEntity:

                EntityPipeline.first()

            };

        }

        return {

            mode: "RESUME",

            batch: runningBatch,

            startEntity:

                this.getNextEntity(

                    runningBatch

                )

        };

    }

    // ----------------------------------
    // Resolve Next Entity
    // ----------------------------------

    getNextEntity(batch) {

        if (!batch) {

            return EntityPipeline.first();

        }

        if (

            !batch.current_entity ||

            batch.current_action !== "COMPLETED"

        ) {

            return (

                batch.current_entity ||

                EntityPipeline.first()

            );

        }

        return (

            EntityPipeline.next(

                batch.current_entity

            ) ||

            null

        );

    }

}

module.exports =
    new ResumeManager();