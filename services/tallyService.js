const axios = require("axios");

const {

  XMLParser

} = require(
  "fast-xml-parser"
);

const fs = require("fs");

const path = require("path");

const DEBUG_FILE = path.join(
  __dirname,
  "tally-debug.log"
);

const ledgerTemplate =
  require("./ledger-template");

const saleTemplate =
  require("./sale-template");

const stockTemplate =
  require("./stock-template");

  const salesLedgerTemplate =
require("./sales-ledger-template");

const TALLY_URL = "http://localhost:9000";

const parser =
  new XMLParser({

    ignoreAttributes: false,

    attributeNamePrefix: "",

    parseTagValue: true,

    trimValues: true

  });

function toArray(value) {

  if (!value) {

    return [];

  }

  return Array.isArray(value)

    ? value

    : [value];

}

async function sendToTally(xml) {

  try {

    // Har API call pe purani log clear
    fs.writeFileSync(DEBUG_FILE, "");

    // XML save
    fs.appendFileSync(
      DEBUG_FILE,
      "\n========== XML SENT ==========\n\n" +
      xml +
      "\n\n"
    );

    const response = await axios.post(
      TALLY_URL,
      xml,
      {
        headers: {
          "Content-Type": "application/xml"
        }
      }
    );

    // Tally response save
    fs.appendFileSync(
      DEBUG_FILE,
      "\n========== TALLY RESPONSE ==========\n\n" +
      response.data +
      "\n"
    );

    return response.data;

  } catch (err) {

    fs.appendFileSync(
      DEBUG_FILE,
      "\n========== ERROR ==========\n\n" +
      (err.response?.data || err.message) +
      "\n"
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



// Company names extract
const companies = [];

const regex = /<COMPANY\s+NAME="([^"]+)"/g;

let match;

while ((match = regex.exec(result)) !== null) {

  companies.push({
    name: match[1]
  });

}



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

// =========================
// GET ALL LEDGERS
// =========================

async function getAllLedgers(
  company
) {

  // Company select (future compatibility)
  await selectCompany(
    company
  );

  const xml = `
<ENVELOPE>

  <HEADER>

    <VERSION>1</VERSION>

    <TALLYREQUEST>Export</TALLYREQUEST>

    <TYPE>Collection</TYPE>

    <ID>List of Ledgers</ID>

  </HEADER>

  <BODY>

    <DESC>

      <STATICVARIABLES>

        <SVCURRENTCOMPANY>
          ${company}
        </SVCURRENTCOMPANY>

        <SVEXPORTFORMAT>
          $$SysName:XML
        </SVEXPORTFORMAT>

      </STATICVARIABLES>

    </DESC>

  </BODY>

</ENVELOPE>
`;

  const result = await sendToTally(xml);

  const ledgers = [];

  const regex = /<LEDGER NAME="([^"]+)"/g;

  let match;

  while ((match = regex.exec(result)) !== null) {

    ledgers.push({

      name: match[1]

    });

  }

  console.log(
    "TALLY LEDGERS",
    ledgers
  );

  // =========================
// BUILD SALES GL
// =========================

const salesGL = ledgers.filter((x) => {

  const name =
    x.name.toUpperCase();

  return (

    name.includes("SALE")

  );

});

console.log(
  "SALES GL",
  salesGL
);

// =========================
// BUILD TAX GL
// =========================

const taxGL = ledgers.filter((x) => {

  const name =
    x.name.toUpperCase();

  return (

    name.includes("CGST") ||

    name.includes("SGST") ||

    name.includes("IGST")

  );

});

console.log(
  "TAX GL",
  taxGL
);

// =========================
// BUILD DEBTORS
// =========================

const debtors = ledgers.filter((x) => {

  const name =
    x.name.toUpperCase();

  return !(

    name.includes("SALE") ||

    name.includes("CGST") ||

    name.includes("SGST") ||

    name.includes("IGST") ||

    name.includes("CASH") ||

    name.includes("ROUND") ||

    name.includes("PROFIT")

  );

});

console.log(
  "DEBTORS",
  debtors
);


return {

  salesGL,

  taxGL,

  debtors

};

}

// =========================
// GET STOCK ITEMS
// =========================

async function getStockItems(
  company
) {

  const xml = `
<ENVELOPE>

  <HEADER>

    <VERSION>1</VERSION>

    <TALLYREQUEST>Export</TALLYREQUEST>

    <TYPE>Collection</TYPE>

    <ID>BilleyStockCollection</ID>

  </HEADER>

  <BODY>

    <DESC>

      <STATICVARIABLES>

        <SVCURRENTCOMPANY>
          ${company}
        </SVCURRENTCOMPANY>

        <SVEXPORTFORMAT>
          $$SysName:XML
        </SVEXPORTFORMAT>

      </STATICVARIABLES>

      <TDL>

        <TDLMESSAGE>

          <COLLECTION NAME="BilleyStockCollection">

            <TYPE>Stock Item</TYPE>

            <FETCH>
                Name,
                Parent,
                BaseUnits,
                GSTHSNName,
                HSNCode
              </FETCH>

          </COLLECTION>

        </TDLMESSAGE>

      </TDL>

    </DESC>

  </BODY>

</ENVELOPE>
`;



 const result =
  await sendToTally(xml);

// =========================
// XML TO JSON
// =========================

const json =
  parser.parse(result);


return json;

}

async function getUnits(company) {

  await selectCompany(company);

  const xml = `
<ENVELOPE>

  <HEADER>

    <VERSION>1</VERSION>

    <TALLYREQUEST>Export</TALLYREQUEST>

    <TYPE>Data</TYPE>

    <ID>List of Accounts</ID>

  </HEADER>

  <BODY>

    <DESC>

      <STATICVARIABLES>

        <SVCURRENTCOMPANY>${company}</SVCURRENTCOMPANY>

        <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>

        <ACCOUNTTYPE>Units</ACCOUNTTYPE>

      </STATICVARIABLES>

    </DESC>

  </BODY>

</ENVELOPE>
`;

  const result = await sendToTally(xml);

  const json = parser.parse(result);

  return json;
}

async function createStockItem({

  company,

  stockName,

  unit,

  hsn,

  gstRate

}) {

  const xml = stockTemplate({

    company,

    stockName,

    unit,

    hsn,

    gstRate

  });

  // =========================
  // DEBUG XML
  // =========================

  fs.writeFileSync(

    path.join(
      __dirname,
      "stock-debug.log"
    ),

    "========== XML ==========\n\n" +

    xml +

    "\n\n"

  );

  // =========================
  // RETURN XML
  // =========================

  return xml;

}
async function createSalesLedger({

  company,

  ledgerName,

}) {

  const xml = salesLedgerTemplate({

    company,

    ledgerName,

  });

  // =========================
  // DEBUG XML
  // =========================

  fs.writeFileSync(

    path.join(
      __dirname,
      "sales-ledger-debug.log"
    ),

    "========== XML ==========\n\n" +

    xml +

    "\n\n"

  );

  const result = await sendToTally(xml);

  // =========================
  // DEBUG RESPONSE
  // =========================

  fs.appendFileSync(

    path.join(
      __dirname,
      "sales-ledger-debug.log"
    ),

    "========== RESPONSE ==========\n\n" +

    result +

    "\n"

  );

  return result;

}

// =========================
// CREATE UNIT XML
// =========================

function createUnit({

  company,

  unitName,

  uqcCode,

}) {

  console.log(
    "CREATE UNIT PARAMS",
    {
      company,
      unitName,
      uqcCode,
    }
  );

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
<SVIMPORTMODE>Alter</SVIMPORTMODE>


</STATICVARIABLES>

</REQUESTDESC>

<REQUESTDATA>

<TALLYMESSAGE xmlns:UDF="TallyUDF">

<UNIT NAME="${unitName}" RESERVEDNAME="">

<NAME>${unitName}</NAME>


<ISUPDATINGTARGETID>No</ISUPDATINGTARGETID>

<ISDELETED>No</ISDELETED>

<ISSECURITYONWHENENTERED>No</ISSECURITYONWHENENTERED>

<ASORIGINAL>Yes</ASORIGINAL>

<ISGSTEXCLUDED>No</ISGSTEXCLUDED>

<ISSIMPLEUNIT>Yes</ISSIMPLEUNIT>

<REPORTINGUQCDETAILS.LIST>

<APPLICABLEFROM>20260401</APPLICABLEFROM>

<REPORTINGUQCNAME>${uqcCode}</REPORTINGUQCNAME>

</REPORTINGUQCDETAILS.LIST>

</UNIT>

</TALLYMESSAGE>

</REQUESTDATA>

</IMPORTDATA>

</BODY>

</ENVELOPE>

`;

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

creditPeriod = 0,

openingBalance = 0,

gstRegistered = false,

country = "India",

  parent = "Sundry Debtors",

  billWise = true

}) {

  const xml = ledgerTemplate({

  company,

  name,

  gstin,

  mobile,

  address,

  state,

  pincode,

  email,

  contactPerson,

  country,

  creditPeriod,

  openingBalance,

  gstRegistered,

  parent,

  billWise

});

  return await sendToTally(xml);

}


async function createSale({

  company,

  voucherDate,

  voucherNumber,

  partyName,

  billingAddress,

  state,

shippingState,

  country,

  gstRegistrationType,

partyGstin,

billingStateCode,

placeOfSupply,

buyerName,

shippingAddress,

billingPincode,

shippingPincode,

shippingStateCode,

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

  transporterName,

vehicleNo,

grRRNo,

ewayBillNo,

dispatchDate,

lrDate,

ewayDate,

creditPeriod,

  salesLedger

}) {

  const xml = saleTemplate({

  company,

  voucherDate,

  voucherNumber,

  partyName,

  billingAddress,

  state,
shippingState,

  country,

  gstRegistrationType,

partyGstin,

billingStateCode,

placeOfSupply,

buyerName,

shippingAddress,

billingPincode,

shippingPincode,

shippingStateCode,

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

  transporterName,

vehicleNo,

grRRNo,

ewayBillNo,

dispatchDate,

lrDate,

ewayDate,

creditPeriod,

  salesLedger

});

  return await sendToTally(xml);

}

module.exports = {

  sendToTally,

  createUnit,

  createStockItem,

    createSalesLedger,

  createLedger,

  createSale,

  getTallyCompanies,

  selectCompany,
  
  getAllLedgers,

  getStockItems,

  

  getUnits,

  //getStockMasters

};