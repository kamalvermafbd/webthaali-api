module.exports = ({
  company,
  ledgerName,
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

            <PARENT>Sales Accounts</PARENT>

          </LEDGER>

        </TALLYMESSAGE>

      </REQUESTDATA>

    </IMPORTDATA>

  </BODY>

</ENVELOPE>

`;

};