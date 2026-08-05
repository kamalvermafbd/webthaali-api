function filterRowsByVoucherGuids({

    voucherGuids,

    rows = []

}) {

    if (!voucherGuids?.length) {

        return [];

    }

    const guidSet = new Set(voucherGuids);

    return rows.filter(

        row =>

            guidSet.has(

                row.voucher_guid

            )

    );

}

module.exports = {

    filterRowsByVoucherGuids

};