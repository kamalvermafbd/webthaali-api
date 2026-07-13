
function buildTaxLedgerEntries(items) {

  const taxLedgerMap = new Map();

  for (const item of items) {

    const cgstAmount =
      Number(item.cgst || 0);

    const sgstAmount =
      Number(item.sgst || 0);

    const igstAmount =
      Number(item.igst || 0);


    if (
      cgstAmount > 0 &&
      item.cgstLedger
    ) {

      const current =
        taxLedgerMap.get(
          item.cgstLedger
        ) || 0;

      taxLedgerMap.set(
        item.cgstLedger,
        current + cgstAmount
      );

    }


    if (
      sgstAmount > 0 &&
      item.sgstLedger
    ) {

      const current =
        taxLedgerMap.get(
          item.sgstLedger
        ) || 0;

      taxLedgerMap.set(
        item.sgstLedger,
        current + sgstAmount
      );

    }


    if (
      igstAmount > 0 &&
      item.igstLedger
    ) {

      const current =
        taxLedgerMap.get(
          item.igstLedger
        ) || 0;

      taxLedgerMap.set(
        item.igstLedger,
        current + igstAmount
      );

    }

  }


  return [...taxLedgerMap.entries()]

    .map(([ledgerName, amount]) => `

<LEDGERENTRIES.LIST>

  <LEDGERNAME>${ledgerName}</LEDGERNAME>

  <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>

  <AMOUNT>${amount.toFixed(2)}</AMOUNT>

</LEDGERENTRIES.LIST>

`)

    .join("");

}

module.exports = ({
  company,
  voucherDate,
  voucherNumber,
  partyName,

  billingAddress,
 state,
shippingState,

partyGstin,

billingStateCode,

placeOfSupply,

buyerName,

shippingAddress,

billingPincode,

shippingPincode,

shippingStateCode,

country,

gstRegistrationType,
  
  items,
  invoiceAmount,

  cgst,
  sgst,
  igst,

  roundOff,

roundOffIsNegative,

roundOffLedger,

transporterName,

vehicleNo,

grRRNo,

ewayBillNo,

dispatchDate,

lrDate,

ewayDate,

creditPeriod
}) => `

<ENVELOPE>

  <HEADER>
    <TALLYREQUEST>Import Data</TALLYREQUEST>
  </HEADER>

  <BODY>

    <IMPORTDATA>

      <REQUESTDESC>

        <REPORTNAME>Vouchers</REPORTNAME>

        <STATICVARIABLES>
          <SVCURRENTCOMPANY>${company}</SVCURRENTCOMPANY>
        </STATICVARIABLES>

      </REQUESTDESC>

      <REQUESTDATA>

        <TALLYMESSAGE xmlns:UDF="TallyUDF">

          <VOUCHER
            VCHTYPE="Sales"
            ACTION="Create"
            OBJVIEW="Invoice Voucher View">

          <DATE>${voucherDate}</DATE>
 
            <VOUCHERTYPENAME>Sales</VOUCHERTYPENAME>

            <VOUCHERNUMBER>${voucherNumber}</VOUCHERNUMBER>

            <PERSISTEDVIEW>Invoice Voucher View</PERSISTEDVIEW>
            <ISINVOICE>Yes</ISINVOICE>

            <PARTYNAME>${partyName}</PARTYNAME>

<PARTYLEDGERNAME>${partyName}</PARTYLEDGERNAME>

<ADDRESS.LIST TYPE="String">
  <ADDRESS>${billingAddress}</ADDRESS>
</ADDRESS.LIST>

<BASICBUYERADDRESS.LIST TYPE="String">
  <BASICBUYERADDRESS>${shippingAddress || billingAddress}</BASICBUYERADDRESS>
</BASICBUYERADDRESS.LIST>

<GSTREGISTRATIONTYPE>${gstRegistrationType}</GSTREGISTRATIONTYPE>

<STATENAME>${state}</STATENAME>

<COUNTRYOFRESIDENCE>${country}</COUNTRYOFRESIDENCE>

<PARTYGSTIN>${partyGstin || ""}</PARTYGSTIN>

<PARTYPINCODE>${billingPincode || ""}</PARTYPINCODE>
<CONSIGNEEGSTIN>${partyGstin || ""}</CONSIGNEEGSTIN>
<CONSIGNEECOUNTRYNAME>${country}</CONSIGNEECOUNTRYNAME>

<PLACEOFSUPPLY>${placeOfSupply}</PLACEOFSUPPLY>

<BASICBUYERNAME>${buyerName}</BASICBUYERNAME>

<PARTYMAILINGNAME>${partyName}</PARTYMAILINGNAME>

${lrDate ? `
<BILLOFLADINGDATE>${lrDate}</BILLOFLADINGDATE>
` : ""}

${grRRNo ? `
<BILLOFLADINGNO>${grRRNo}</BILLOFLADINGNO>
` : ""}

${transporterName ? `
<EICHECKPOST>${transporterName}</EICHECKPOST>
` : ""}

${vehicleNo ? `
<BASICSHIPVESSELNO>${vehicleNo}</BASICSHIPVESSELNO>
` : ""}

${creditPeriod ? `
<BASICDUEDATEOFPYMT>${creditPeriod} Days</BASICDUEDATEOFPYMT>
` : ""}

${ewayBillNo ? `
<EWAYBILLDETAILS.LIST>

<BILLDATE>${ewayDate || dispatchDate}</BILLDATE>

<BILLNUMBER>${ewayBillNo}</BILLNUMBER>

<SHIPPEDFROMSTATE>${state}</SHIPPEDFROMSTATE>

<SHIPPEDTOSTATE>${shippingState || state}</SHIPPEDTOSTATE>

</EWAYBILLDETAILS.LIST>
` : ""}


<BASICSHIPTOADDRESS.LIST TYPE="String">
  <BASICSHIPTOADDRESS>${shippingAddress || billingAddress}</BASICSHIPTOADDRESS>
</BASICSHIPTOADDRESS.LIST>

<BASICSHIPTOPINCODE>${shippingPincode || billingPincode}</BASICSHIPTOPINCODE>

<BASICSHIPTOSTATE>${shippingState || state}</BASICSHIPTOSTATE>
<CONSIGNEESTATENAME>${shippingState || state}</CONSIGNEESTATENAME>
<CONSIGNEEMAILINGNAME>${partyName}</CONSIGNEEMAILINGNAME>

<BASICSHIPTOSTATECODE>${shippingStateCode || billingStateCode}</BASICSHIPTOSTATECODE>



${items.map(item => `

<ALLINVENTORYENTRIES.LIST>

  <STOCKITEMNAME>${item.stockName}</STOCKITEMNAME>

  <GSTHSNNAME>${item.hsn}</GSTHSNNAME>

  <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>

  <AMOUNT>${item.taxable_amount}</AMOUNT>
<ACTUALQTY>${item.qty} ${item.unit}</ACTUALQTY>
<BILLEDQTY>${item.qty} ${item.unit}</BILLEDQTY>

<RATE>${item.rate}/${item.unit}</RATE>

<ACCOUNTINGALLOCATIONS.LIST>

  <LEDGERNAME>${item.salesLedger}</LEDGERNAME>

  <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>

  <LEDGERFROMITEM>No</LEDGERFROMITEM>

  <AMOUNT>${item.taxable_amount}</AMOUNT>

</ACCOUNTINGALLOCATIONS.LIST>

</ALLINVENTORYENTRIES.LIST>

`).join("")}

<LEDGERENTRIES.LIST>

  <LEDGERNAME>${partyName}</LEDGERNAME>

  <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>

  <LEDGERFROMITEM>No</LEDGERFROMITEM>

  <AMOUNT>-${invoiceAmount}</AMOUNT>

</LEDGERENTRIES.LIST>

${buildTaxLedgerEntries(items)}

${roundOff !== 0 ? `
<LEDGERENTRIES.LIST>

<LEDGERNAME>${roundOffLedger}</LEDGERNAME>

<ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>

<AMOUNT>${roundOffIsNegative ? "-" : ""}${roundOff.toFixed(2)}</AMOUNT>

<VATEXPAMOUNT>${roundOffIsNegative ? "-" : ""}${roundOff.toFixed(2)}</VATEXPAMOUNT>

</LEDGERENTRIES.LIST>
` : ""}


          </VOUCHER>

        </TALLYMESSAGE>

      </REQUESTDATA>

    </IMPORTDATA>

  </BODY>

</ENVELOPE>

`;
