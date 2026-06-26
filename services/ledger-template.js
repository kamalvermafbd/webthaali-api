module.exports = ({
  company,
  name,
  gstin = "",
  address = "",
  state = "",
  pincode = "",
 mobile = "",
email = "",
contactPerson = "",
country = "India",
  creditPeriod = 0,
  openingBalance = 0,
gstRegistered = false,
parent = "Sundry Debtors"
}) => {

    const amount =
    parent === "Sundry Debtors"
      ? -Math.abs(Number(openingBalance))
      : Math.abs(Number(openingBalance));

  return `


<ENVELOPE>

  <HEADER>
    <TALLYREQUEST>Import Data</TALLYREQUEST>
  </HEADER>

  <BODY>

    <IMPORTDATA>

      <REQUESTDESC>

        <REPORTNAME>All Masters</REPORTNAME>

        <STATICVARIABLES>
          <SVCURRENTCOMPANY>${company}</SVCURRENTCOMPANY>
        </STATICVARIABLES>

      </REQUESTDESC>

      <REQUESTDATA>

        <TALLYMESSAGE xmlns:UDF="TallyUDF">

          <LEDGER NAME="${name}" ACTION="Create">

            <NAME>${name}</NAME>

            <PARENT>Sundry Debtors</PARENT>

            <OPENINGBALANCE>${amount.toFixed(2)}</OPENINGBALANCE>

            <ISBILLWISEON>Yes</ISBILLWISEON>

            <BILLCREDITPERIOD>${creditPeriod} Days</BILLCREDITPERIOD>

<EMAIL>${email}</EMAIL>

<LEDGERCONTACT>${contactPerson}</LEDGERCONTACT>

<LEDGERMOBILE>${mobile}</LEDGERMOBILE>

<LEDGERCOUNTRYISDCODE>+91</LEDGERCOUNTRYISDCODE>

            <LEDMAILINGDETAILS.LIST>

  <ADDRESS.LIST TYPE="String">
    <ADDRESS>${address}</ADDRESS>
  </ADDRESS.LIST>

  <APPLICABLEFROM>20260401</APPLICABLEFROM>

  <MAILINGNAME>${name}</MAILINGNAME>

  <STATE>${state}</STATE>

<COUNTRY>${country}</COUNTRY>

<PINCODE>${pincode}</PINCODE>

</LEDMAILINGDETAILS.LIST>

<LEDGSTREGDETAILS.LIST>

  <APPLICABLEFROM>20260401</APPLICABLEFROM>

  <GSTREGISTRATIONTYPE>
    ${gstRegistered ? "Regular" : "Unregistered"}
  </GSTREGISTRATIONTYPE>

  <PLACEOFSUPPLY>${state}</PLACEOFSUPPLY>

  <GSTIN>${gstin}</GSTIN>

</LEDGSTREGDETAILS.LIST>

<CONTACTDETAILS.LIST>

  <NAME>${contactPerson}</NAME>

  <PHONENUMBER>${mobile}</PHONENUMBER>

  <COUNTRYISDCODE>+91</COUNTRYISDCODE>

  <ISDEFAULTWHATSAPPNUM>Yes</ISDEFAULTWHATSAPPNUM>

</CONTACTDETAILS.LIST>

</LEDGER>

        </TALLYMESSAGE>

      </REQUESTDATA>

    </IMPORTDATA>

  </BODY>

</ENVELOPE>

`;
}