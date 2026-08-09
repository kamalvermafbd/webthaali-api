const VoucherIntegrityService =
    require("../services/VoucherIntegrityService");

const {

    VALIDATION_ACTION

} = require("./constants");

/*
const VALIDATION_ACTION = {

    INSERT: "INSERT",

    UPDATE: "UPDATE",

    FORCE_UPDATE: "FORCE_UPDATE",

    SKIP: "SKIP"

};
*/

class VoucherValidationService {

    async validateNewVoucher(args) {

        const {

            parsedVoucher,

            existingVoucherMap

        } = args;

        const guid =
            parsedVoucher.header?.guid?.trim();

     const existingVoucher =
        existingVoucherMap.get(guid);

        if (existingVoucher === undefined) {

            return this.buildValidationResult({
                action:
                    VALIDATION_ACTION.INSERT
            });

        }

        if (existingVoucher.is_deleted) {

            return this.buildValidationResult({
                action:
                    VALIDATION_ACTION.UPDATE
            });

        }

        return this.validateAlterId({

            ...args,

            existingAlterId:
                existingVoucher.alterid

        });


    }

    async validateAlterId({

        company_code,

        tally_owner,

        parsedVoucher,

        runId,

        existingAlterId

    }) {

      

        const incomingAlterId =
            Number(parsedVoucher.header?.alterid);

        if (existingAlterId !== incomingAlterId) {

            return this.buildValidationResult({

                action:
                    VALIDATION_ACTION.UPDATE

            });

        }

        return this.runIntegrityValidation({

            company_code,

            tally_owner,

            parsedVoucher,

            runId

        });

    }

    async runIntegrityValidation(args) {

        return VoucherIntegrityService
            .validateVoucher(args);

    }

    buildValidationResult({

        action,

        valid = true,

        requiresRepair = false,

        reasons = []

    }) {

        return {

            action,

            valid,

            requiresRepair,

            reasons

        };

    }

}

module.exports =
    new VoucherValidationService();