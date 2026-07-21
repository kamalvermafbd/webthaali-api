const saveGroups =
    require("./sync/saveGroups");

const saveUnits =
    require("./sync/saveUnits");

const saveLedgers =
    require("./sync/saveLedgers");

const saveStocks =
    require("./sync/saveStocks");

const saveGodowns =
    require("./sync/saveGodowns");

const saveCostCentres =
    require("./sync/saveCostCentres");

const saveVoucherTypes =
    require("./sync/saveVoucherTypes");


async function saveMasters({

    company_code,

    tally_owner,

    groups = [],

    units = [],

    ledgers = [],

    stocks = [],

    godowns = [],

    costCentres = [],

    voucherTypes = []

}) {

    const summary = {};

    summary.groups =
        await saveGroups({

            company_code,

            tally_owner,

            data: groups

        });

    summary.units =
        await saveUnits({

            company_code,

            tally_owner,

            data: units

        });

    summary.ledgers =
        await saveLedgers({

            company_code,

            tally_owner,

            data: ledgers

        });

    summary.stocks =
        await saveStocks({

            company_code,

            tally_owner,

            data: stocks

        });

    summary.godowns =
        await saveGodowns({

            company_code,

            tally_owner,

            data: godowns

        });

    summary.costCentres =
        await saveCostCentres({

            company_code,

            tally_owner,

            data: costCentres

        });

    summary.voucherTypes =
        await saveVoucherTypes({

            company_code,

            tally_owner,

            data: voucherTypes

        });

    return summary;

}

module.exports = saveMasters;