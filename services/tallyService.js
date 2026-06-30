const axios = require("axios");

const {

  XMLParser

} = require(
  "fast-xml-parser"
);

const fs = require("fs");
const ledgerTemplate =
  require("./ledger-template");

const saleTemplate =
  require("./sale-template");

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

 console.log("ENTER sendToTally");

    console.log("===== XML SENT TO TALLY =====");
    console.log(xml);

    console.log("BEFORE AXIOS");

    const response = await axios.post(
      TALLY_URL,
      xml,
      {
        headers: {
          "Content-Type": "application/xml"
        }
      }
    );

  console.log("AFTER AXIOS");

console.log(response.data);

// 👇 YAHAN ADD KARO
fs.writeFileSync(
  "tally-response.xml",
  response.data
);

console.log("Response saved to tally-response.xml");

if (response.data.includes("<LINEERROR>")) {

  console.log("===== LINE ERROR =====");

  const errors =
    response.data.match(
      /<LINEERROR>(.*?)<\/LINEERROR>/gs
    );

  console.log(errors);

}

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

    <ID>List of Stock Items</ID>

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

console.log(
  "STOCK JSON"
);

console.dir(
  json,
  {
    depth: null
  }
);

return json;

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

  createLedger,

  createSale,

  getTallyCompanies,

  selectCompany,
  
  getAllLedgers,

  getStockItems,

  //getStockMasters

};