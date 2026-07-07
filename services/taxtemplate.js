module.exports = ({
  company,
  ledgerName,
  dutyHead,
}) => {

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

          <LEDGER NAME="${ledgerName}" ACTION="Create">

            <NAME>${ledgerName}</NAME>

            <PARENT>Duties &amp; Taxes</PARENT>

            <TAXTYPE>GST</TAXTYPE>

            <GSTDUTYHEAD>${dutyHead}</GSTDUTYHEAD>

            <ROUNDINGMETHOD>&#4; Not Applicable</ROUNDINGMETHOD>

          </LEDGER>

        </TALLYMESSAGE>

      </REQUESTDATA>

    </IMPORTDATA>

  </BODY>

</ENVELOPE>

`;

};