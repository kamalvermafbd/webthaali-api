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

cgstLedger,
sgstLedger,
igstLedger,

roundOffLedger,

salesLedger
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

  <STOCKITEMNAME>${item.hsn}</STOCKITEMNAME>

  <GSTHSNNAME>${item.hsn}</GSTHSNNAME>

  <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>

  <AMOUNT>${item.taxable_amount}</AMOUNT>
<ACTUALQTY>${item.qty} ${item.unit}</ACTUALQTY>
<BILLEDQTY>${item.qty} ${item.unit}</BILLEDQTY>

<RATE>${item.rate}/${item.unit}</RATE>

<ACCOUNTINGALLOCATIONS.LIST>

  <LEDGERNAME>${salesLedger}</LEDGERNAME>

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

${cgst > 0 ? `
<LEDGERENTRIES.LIST>

  <LEDGERNAME>${cgstLedger}</LEDGERNAME>

  <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>

  <AMOUNT>${cgst}</AMOUNT>

</LEDGERENTRIES.LIST>
` : ""}

${sgst > 0 ? `
<LEDGERENTRIES.LIST>

  <LEDGERNAME>${sgstLedger}</LEDGERNAME>

  <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>

  <AMOUNT>${sgst}</AMOUNT>

</LEDGERENTRIES.LIST>
` : ""}

${igst > 0 ? `
<LEDGERENTRIES.LIST>

  <LEDGERNAME>${igstLedger}</LEDGERNAME>

  <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>

  <AMOUNT>${igst}</AMOUNT>

</LEDGERENTRIES.LIST>
` : ""}

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
