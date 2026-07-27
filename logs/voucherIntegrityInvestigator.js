const fs = require("fs");

class VoucherIntegrityInvestigator {

    investigate({

        parsedVoucher,

        dbData

    }) {

        fs.appendFileSync(
            "./logs/voucher-integrity-investigation.jsonl",
            JSON.stringify({

                voucher_number:
                    parsedVoucher?.header?.voucherNumber,

                voucher_guid:
                    parsedVoucher?.header?.guid,

                db_inventory_count:
                    (dbData.inventory || []).length,

                parsed_inventory_count:
                    (parsedVoucher.inventory || [])
                        .filter(
                            x =>
                                x.inventoryNode ===
                                "ALLINVENTORYENTRIES.LIST"
                        ).length,

                db_stock_count:
                    (dbData.stockVouchers || []).length,

                parsed_stock_count:
                    (parsedVoucher.inventory || [])
                        .filter(
                            x =>
                                x.inventoryNode !==
                                "ALLINVENTORYENTRIES.LIST"
                        ).length,

                db_inventory:
                    (dbData.inventory || []).map(item => ({

                        stock:
                            item.stock_item,

                        stock_guid:
                            item.stock_guid,

                        batch_id:
                            item.batch_id,

                        key: [
                            item.stock_guid,
                            "ALLINVENTORYENTRIES.LIST",
                            item.batch_id || ""
                        ].join("|")

                    })),

                parsed_inventory:
                    (parsedVoucher.inventory || [])
                        .filter(
                            x =>
                                x.inventoryNode ===
                                "ALLINVENTORYENTRIES.LIST"
                        )
                        .map(item => ({

                            stock:
                                item.stockItem,

                            stock_guid:
                                item.stockGuid,

                            batch_id:
                                item.batchId,

                            key: [
                                item.stockGuid,
                                "ALLINVENTORYENTRIES.LIST",
                                item.batchId || ""
                            ].join("|")

                        }))

            }) + "\n"
        );

    }

}

module.exports = new VoucherIntegrityInvestigator();