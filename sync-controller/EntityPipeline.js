const {

    ENTITY_SEQUENCE

} = require("../sync-engine/constants");

class EntityPipeline {

    // ----------------------------------
    // First Entity
    // ----------------------------------

    first() {

        return ENTITY_SEQUENCE[0] || null;

    }

    // ----------------------------------
    // Next Entity
    // ----------------------------------

    next(currentEntity) {

        if (!currentEntity) {

            return this.first();

        }

        const index =

            ENTITY_SEQUENCE.indexOf(

                currentEntity

            );

        if (index === -1) {

            throw new Error(

                `Unknown entity : ${currentEntity}`

            );

        }

        return ENTITY_SEQUENCE[index + 1] || null;

    }

    // ----------------------------------
    // Previous Entity
    // ----------------------------------

    previous(currentEntity) {

        const index =

            ENTITY_SEQUENCE.indexOf(

                currentEntity

            );

        if (index === -1) {

            throw new Error(

                `Unknown entity : ${currentEntity}`

            );

        }

        return ENTITY_SEQUENCE[index - 1] || null;

    }

    // ----------------------------------
    // Last Entity
    // ----------------------------------

    last() {

        return ENTITY_SEQUENCE[

            ENTITY_SEQUENCE.length - 1

        ] || null;

    }

    // ----------------------------------
    // Is Last Entity
    // ----------------------------------

    isLast(currentEntity) {

        return (

            currentEntity ===

            this.last()

        );

    }

    // ----------------------------------
    // Is Valid Entity
    // ----------------------------------

    has(currentEntity) {

        return ENTITY_SEQUENCE.includes(

            currentEntity

        );

    }

    // ----------------------------------
    // Get All Entities
    // ----------------------------------

    all() {

        return [

            ...ENTITY_SEQUENCE

        ];

    }


        // ----------------------------------
    // Get Remaining Entities
    // ----------------------------------

    from(startEntity) {

        if (!startEntity) {

            return this.all();

        }

        const index =

            ENTITY_SEQUENCE.indexOf(

                startEntity

            );

        if (index === -1) {

            throw new Error(

                `Unknown entity : ${startEntity}`

            );

        }

        return ENTITY_SEQUENCE.slice(

            index

        );

    }
    

}

module.exports =
    new EntityPipeline();
    