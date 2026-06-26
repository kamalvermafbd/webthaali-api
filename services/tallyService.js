const axios = require("axios");

const TALLY_URL = "http://localhost:9000";

async function sendToTally(xml) {

  try {

    const response = await axios.post(
      TALLY_URL,
      xml,
      {
        headers: {
          "Content-Type": "application/xml"
        }
      }
    );

    return response.data;

  } catch (err) {

    console.error(
      "Tally Error:",
      err.response?.data || err.message
    );

    throw err;

  }

}

async function getTallyCompanies() {

  const xml = `
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Export</TALLYREQUEST>
    <TYPE>Collection</TYPE>
    <ID>List of Companies</ID>
  </HEADER>

  <BODY>

    <DESC>

      <STATICVARIABLES>
        <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      </STATICVARIABLES>

    </DESC>

  </BODY>

</ENVELOPE>
`;

 const result = await sendToTally(xml);

 console.log("RAW TALLY RESPONSE:");
console.log(result);

// Company names extract
const companies = [];

const regex = /<COMPANY\s+NAME="([^"]+)"/g;

let match;

while ((match = regex.exec(result)) !== null) {

  companies.push({
    name: match[1]
  });

}

console.log(
  "EXTRACTED COMPANIES:",
  companies
);

return {
  success: true,
  companies
};

}

// =========================
// SELECT TALLY COMPANY
// =========================

async function selectCompany(
  companyName
) {

  const xml = `
<ENVELOPE>

  <HEADER>

    <TALLYREQUEST>
      Export
    </TALLYREQUEST>

  </HEADER>

  <BODY>

    <DESC>

      <STATICVARIABLES>

        <SVCURRENTCOMPANY>
          ${companyName}
        </SVCURRENTCOMPANY>

        <SVEXPORTFORMAT>
          $$SysName:XML
        </SVEXPORTFORMAT>

      </STATICVARIABLES>

      <REPORTNAME>
        List of Accounts
      </REPORTNAME>

    </DESC>

  </BODY>

</ENVELOPE>
`;

  return await sendToTally(xml);

}

async function createLedger({
    company,

  name,

  gstin = "",

  mobile = "",

  address = "",

  state = "",

  pincode = "",

  email = "",

  contactPerson = "",

  parent = "Sundry Debtors",

  billWise = true

}) {

  const xml = `
<ENVELOPE>
  <HEADER>
    <TALLYREQUEST>Import Data</TALLYREQUEST>
  </HEADER>

  <BODY>

    <IMPORTDATA>

     <REQUESTDESC>

  <REPORTNAME>

    All Masters

  </REPORTNAME>

  <STATICVARIABLES>

    <SVCURRENTCOMPANY>

      ${company}

    </SVCURRENTCOMPANY>

  </STATICVARIABLES>

</REQUESTDESC>

      <REQUESTDATA>

        <TALLYMESSAGE xmlns:UDF="TallyUDF">

          <LEDGER NAME="${name}" ACTION="Create">

            <NAME>${name}</NAME>

            <PARENT>${parent}</PARENT>

            <ISBILLWISEON>${billWise ? "Yes" : "No"}</ISBILLWISEON>

          </LEDGER>

        </TALLYMESSAGE>

      </REQUESTDATA>

    </IMPORTDATA>

  </BODY>

</ENVELOPE>
`;

  return await sendToTally(xml);

}

module.exports = {

  sendToTally,

  createLedger,

  getTallyCompanies,

  selectCompany

};