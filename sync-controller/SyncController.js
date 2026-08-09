const EntityPipeline =
    require("./EntityPipeline");

const ResumeManager =
    require("./ResumeManager");

class SyncController {

    // ----------------------------------
    // Resolve Sync Direction
    // ----------------------------------

    async start({

        runningBatch

    }) {

        const resume =

            await ResumeManager.resolve({

                runningBatch

            });

        const remainingEntities =

            resume.mode === "NEW"

                ? EntityPipeline.all()

                : EntityPipeline.from(

                    resume.startEntity

                );

        return {

            success: true,

            mode: resume.mode,

            startEntity: resume.startEntity,

            remainingEntities

        };

    }

}

module.exports =
    new SyncController();