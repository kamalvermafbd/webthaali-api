require("dotenv").config();

const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const nodemailer =
  require("nodemailer");

 const { Resend } = require("resend");

const resend = new Resend(
  process.env.RESEND_API_KEY
); 

const {
  createClient
} = require("@supabase/supabase-js");

const app = express();

app.use(cors());

app.use(

  express.json({

    limit: "10mb"

  })

);

app.use(

  express.urlencoded({

    extended: true,

    limit: "10mb"

  })

);

const supabase =
  createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
  );

const transporter =
  nodemailer.createTransport({

    service: "gmail",

    auth: {

      user:
        process.env.EMAIL_USER,

      pass:
        process.env.EMAIL_PASS

    }

});

console.log(
  "EMAIL USER:",
  process.env.EMAIL_USER
);

app.get("/", (req, res) => {

  res.json({
    success: true,
    message: "billey API Running"
  });

});


function formatDateByCountry(
  date,
  country
) {

  const timezone =
    String(country || "")
      .trim()
      .toUpperCase() === "INDIA"
      ? "Asia/Kolkata"
      : "UTC";

  return new Intl.DateTimeFormat(
    "sv-SE",
    {
      timeZone: timezone
    }
  ).format(date);

}

app.post(
  "/saveInvoice",
  async (req, res) => {

    try {

      const invoice =
        req.body;

      console.log(
  "REQ BODY:",
  req.body
);


  const isEdit =
  invoice.isEdit === true;

  // =========================
  // GENERATE INVOICE ID
  // =========================

  const companyCode =

    String(
      invoice.company?.company_code || ""
    ).trim();


  const clientCode =

    String(
      invoice.customer?.client_code || ""
    ).trim();

  const documentNumber =

  String(

    invoice.doc_type === "quotation"

      ? invoice.Quote_No || ""

      : invoice.invoiceNumber || ""

  ).trim();

// INV/26-27/0018
const parts =
  documentNumber.split("/");

  const fy =
    parts[1]
      ? parts[1].replace("-", "")
      : "";

  const sequence =
    parts[2] || "";

  const generatedInvoiceId =
  invoice.doc_type === "quotation"
    ? `QT-${companyCode}-${clientCode}-${fy}-${sequence}`
    : `${companyCode}-${clientCode}-${fy}-${sequence}`;

const invoiceId =

  isEdit

    ? invoice.invoiceId

    : generatedInvoiceId;

console.log(
  "IS EDIT:",
  isEdit
);

console.log(
  "REQ BODY invoiceId:",
  invoice.invoiceId
);

console.log(
  "GENERATED invoiceId:",
  invoiceId
);

const creditPeriod =
  Number(invoice.creditPeriod || 0);


// =========================
// CALCULATE DUE DATE
// =========================
let dueDate =

  invoice.doc_type === "quotation"

    ? invoice.Quote_Dt || ""

    : invoice.invoiceDate || "";

if (creditPeriod > 0) {

 const date = new Date(

  invoice.doc_type === "quotation"

    ? invoice.Quote_Dt

    : invoice.invoiceDate

);

  date.setDate(
    date.getDate() + creditPeriod
  );

 dueDate =
  date
    .toISOString()
    .split("T")[0];

}

  // =========================
  // SAVE MAIN INVOICE
  // =========================

 // =========================
// UPDATE / SAVE MAIN INVOICE
// =========================


if (isEdit) {

  let oldInvoice = null;

const {
  data: existingInvoice
} = await supabase

  .from("invoices")

  .select(
    "invoice_id,invoice_number,status"
  )

  .eq(
    "invoice_id",
    invoiceId
  )

  .single();

oldInvoice =
  existingInvoice;

console.log(
  "FINAL UPDATE OBJECT:",
  {

    quote_no:
      invoice.Quote_No || "",

    quote_dt:
      invoice.Quote_Dt || ""

  }
);
  await supabase

    .from("invoices")

    .update({

    invoice_number:

  invoice.doc_type === "quotation"

    ? null

    : invoice.invoiceNumber || "",

invoice_date:

  invoice.doc_type === "quotation"

    ? null

    : invoice.invoiceDate || "",

customer_name:
  invoice.customer?.name || "",

customer_mobile:
  invoice.customer?.mobile || "",

customer_gst:
  invoice.customer?.gstin || "",

     billing_address:
  invoice.customer?.billingAddress || "",

    shipping_address:
  invoice.customer?.shippingAddress || "",

      state:
        invoice.customer?.state || "",

      state_code:
  invoice.customer?.stateCode || "",

      subtotal:
  invoice.totals?.taxableAmount || 0,

cgst:
  invoice.totals?.cgstTotal || 0,

sgst:
  invoice.totals?.sgstTotal || 0,

igst:
  invoice.totals?.igstTotal || 0,

  gst_percent:
  invoice.totals?.gstPercent || 0,

grand_total:
  invoice.totals?.rounded || 0,

 payment_mode:
  invoice.paymentMode || "",
  

      notes:
        invoice.notes || "",

      terms:
        invoice.terms || "",

      company_code:
        companyCode,

      client_code:
        clientCode,

      billing_pincode:
        invoice.customer?.billingPincode || "",

      shipping_state:
        invoice.customer?.shippingState || "",

      shipping_state_code:
  invoice.customer?.shippingStateCode || "",

      shipping_pincode:
        invoice.customer?.shippingPincode || "",

      due_date:
        dueDate,

      credit_period:
        creditPeriod,

      status:
        invoice.status || "",

      doc_type:
        invoice.doc_type || "invoice",

   quote_no:

  invoice.doc_type === "quotation"

    ? invoice.Quote_No || ""

    : "",

quote_dt:

  invoice.doc_type === "quotation"

    ? invoice.Quote_Dt || null

    : null,

  

    })

 .eq(
  "invoice_id",
  invoiceId
);


console.log(
  "AUDIT INSERT",
  {
    invoiceId,
    oldStatus:
      oldInvoice?.status,
    newStatus:
      invoice.status
  }
);

// =========================
// AUDIT LOG
// =========================

if (
  oldInvoice?.status === "Posted"
) {

  const {
  error: auditError
} = await supabase

    .from(
      "invoice_audit_log"
    )

    .insert([{

      company_code:
        companyCode,

      invoice_id:
        invoiceId,

      invoice_number:
        oldInvoice
          .invoice_number ||

        invoice.invoiceNumber ||

        "",

      action:
        "POSTED_INVOICE_EDITED",

      old_status:
        "Posted",

      new_status:
        invoice.status || "",

      changed_by_name:
        invoice.company
          ?.businessName ||

        "Company User",

      reason:
  invoice.editReason || ""

    }]);

    if (auditError) {

  console.error(
    "AUDIT ERROR:",
    auditError
  );

}

}







} else {


  console.log(
  "FINAL INSERT OBJECT:",
  {

    quote_no:
      invoice.Quote_No || "",

    quote_dt:
      invoice.Quote_Dt || ""

  }
);

console.log(
  "FINAL INSERT DATA:",
  {
    invoice_id: invoiceId,
    doc_type: invoice.doc_type,
    quote_no: invoice.Quote_No,
    quote_dt: invoice.Quote_Dt,
    invoice_number: invoice.invoiceNumber
  }
);

console.log(
  "QQQ-1 DOC TYPE =>",
  invoice.doc_type
);

console.log(
  "QQQ-2 QUOTE NO =>",
  invoice.Quote_No
);

console.log(
  "QQQ-3 QUOTE DT =>",
  invoice.Quote_Dt
);

console.log(
  "QQQ-4 GENERATED ID =>",
  invoiceId
);

console.log(
  "QQQ-5 INSERT PAYLOAD =>",
  {
    invoice_number:
      invoice.invoiceNumber,

    invoice_date:
      invoice.invoiceDate,

    quote_no:
      invoice.Quote_No,

    quote_dt:
      invoice.Quote_Dt
  }
);
  const {
  error: invoiceError
} = await supabase

    .from("invoices")

    
    .insert([{

invoice_id:
  invoiceId,

share_token:
  crypto.randomBytes(16).toString("hex"),

invoice_number:

  invoice.doc_type === "quotation"

    ? null

    : invoice.invoiceNumber || "",

invoice_date:

  invoice.doc_type === "quotation"

    ? null

    : invoice.invoiceDate || "",

customer_name:
  invoice.customer?.name || "",

customer_mobile:
  invoice.customer?.mobile || "",

customer_gst:
  invoice.customer?.gstin || "",

     billing_address:
  invoice.customer?.billingAddress || "",

    shipping_address:
  invoice.customer?.shippingAddress || "",

      state:
        invoice.customer?.state || "",

      state_code:
  invoice.customer?.stateCode || "",

      subtotal:
  invoice.totals?.taxableAmount || 0,

cgst:
  invoice.totals?.cgstTotal || 0,

sgst:
  invoice.totals?.sgstTotal || 0,

igst:
  invoice.totals?.igstTotal || 0,

  gst_percent:
  invoice.totals?.gstPercent || 0,

grand_total:
  invoice.totals?.rounded || 0,

 payment_mode:
  invoice.paymentMode || "",

      notes:
        invoice.notes || "",

      terms:
        invoice.terms || "",

      company_code:
        companyCode,

      client_code:
        clientCode,

      billing_pincode:
        invoice.customer?.billingPincode || "",

      shipping_state:
        invoice.customer?.shippingState || "",

shipping_state_code:
  invoice.customer?.shippingStateCode || "",

      shipping_pincode:
        invoice.customer?.shippingPincode || "",

      due_date:
        dueDate,

      credit_period:
        creditPeriod,

      status:
        invoice.status || "",

      doc_type:
        invoice.doc_type || "invoice",

     quote_no:

  invoice.doc_type === "quotation"

    ? invoice.Quote_No || ""

    : "",

quote_dt:

  invoice.doc_type === "quotation"

    ? invoice.Quote_Dt || null

    : null,


    }]);

    console.log(
  "INVOICE ERROR FULL:",
  invoiceError
);

console.log(
  "QQQ-6 SUPABASE ERROR =>",
  invoiceError
);

if (invoiceError) {

  return res.status(400).json({

    success: false,

    error: JSON.stringify(invoiceError)

  });

}
}


// =========================
// DELETE OLD ITEMS ON EDIT
// =========================

if (isEdit) {

  await supabase

    .from("invoice_items")

    .delete()

  .eq(
  "invoice_id",
      invoiceId
    );

}

  // =========================
  // SAVE ITEMS
  // =========================

  const items = invoice.items || [];

  for (const item of items) {

    const qty =
      Number(item.quantity || 0);

    const rate =
      Number(item.rate || 0);

    const gstPct =
      Number(item.gstPct || 0);

    const discountPct =
      Number(item.discountPct || 0);

    const gross =
      qty * rate;

    const discountAmt =
      gross * discountPct / 100;

    const taxable =
      gross - discountAmt;

    const gstAmt =
      taxable * gstPct / 100;

    const isInterState =
      invoice.isInterState || false;

    const cgst =
      isInterState
        ? 0
        : gstAmt / 2;

    const sgst =
      isInterState
        ? 0
        : gstAmt / 2;

    const igst =
      isInterState
        ? gstAmt
        : 0;

    const total =
      taxable + gstAmt;

    await supabase

  .from("invoice_items")

  .insert([{

   invoice_id:
  invoiceId,

    item_name:
      item.name || "",

    hsn:
      String(item.hsn || ""),

    qty:
      qty,

    unit:
  item.unit || "",

  packingqty:
  Number(
    item.packingQty || 0
  ),

packingunit:
  item.packingUnit || "",

    rate:
      rate,

    gst_percent:
      gstPct,

    taxable_amount:
      taxable,

    cgst:
      cgst,

    sgst:
      sgst,

    igst:
      igst,

    total:
      total,

    disc_percent:
      discountPct,

    disc_amt:
      discountAmt,

    source:
      item.invoiceMode || "free"

  }]);

 }


  res.json({

  success: true,

  message:
    "Invoice saved successfully",

  invoiceId

});

    }

    catch (err) {

      return res.status(500).json({

        success: false,

        error: err.message

      });

    }

  }
);


// =========================
// GET INVOICE BY ID
// =========================

app.get(
  "/getInvoiceById",
  async (req, res) => {

    try {

      const invoiceId =

        String(
          req.query.invoiceId || ""
        ).trim();

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

        
      if (!invoiceId) {

        return res.status(400).json({

          success: false,

          error:
            "invoiceId required"

        });

      }

      

      if (!company_code) {

        return res.status(400).json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // GET MAIN INVOICE
      // =========================

      const {
        data: invoice,
        error: invoiceError
      } = await supabase

        .from("invoices")

        .select("*")

        .eq(
          "invoice_id",
          invoiceId
        )

        .eq(
          "company_code",
          company_code
        )

        .single();

        console.log("RECEIVED invoiceId:", invoiceId);
console.log("RECEIVED company_code:", company_code);
console.log("FOUND INVOICE:", invoice);
console.log("INVOICE ERROR:", invoiceError);
      if (invoiceError || !invoice) {

        return res.status(404).json({

          success: false,

          error:
            "Invoice not found"

        });

      }
 // =========================
// GET COMPANY
// =========================

const {
  data: company
} = await supabase

  .from("company")

  .select("*")

  .eq(
    "company_code",
    company_code
  )

  .single();


  console.log(
  "COMPANY STATE CODE:",
  company?.statecode
);

console.log(
  "CUSTOMER STATE CODE:",
  invoice.state_code
);

console.log(
  "isInterState:",
  String(
    company?.statecode || ""
  ).trim()

  !==

  String(
    invoice.state_code || ""
  ).trim()
);

      // =========================
      // GET ITEMS
      // =========================

      const {
        data: items,
        error: itemsError
      } = await supabase

     
        .from("invoice_items")

        .select("*")

        .eq(
          "invoice_id",
          invoiceId
        );

         console.log(
  "RAW DB ITEMS:",
  items
);

      if (itemsError) {

        return res.status(400).json({

          success: false,

          error:
            itemsError.message

        });

      }

      // =========================
      // FINAL RESPONSE
      // =========================
console.log(
  "FINAL RESPONSE ITEMS:",
  (items || []).map((item) => ({

    id:
      item.id,

    name:
      item.item_name,

    hsn:
      item.hsn,

    quantity:
      item.qty,

     unit:
      item.unit,

      packingQty:
  item.packingqty,

packingUnit:
  item.packingunit,

    rate:
      item.rate,

    gstPct:
      item.gst_percent,

    discountPct:
      item.disc_percent,

    discountAmt:
      item.disc_amt,

    taxable:
      item.taxable_amount,

    cgst:
      item.cgst,

    sgst:
      item.sgst,

    igst:
      item.igst,

    total:
      item.total,

    source:
  item.source

  }))
);
      return res.json({

        success: true,

        data: {

          invoiceId:
            invoice.invoice_id,

          invoiceNumber:
            invoice.invoice_number,

          share_token:
  invoice.share_token,

          invoiceDate:
            invoice.invoice_date,

          customerName:
            invoice.customer_name,

          customerMobile:
            invoice.customer_mobile,

          customerGST:
            invoice.customer_gst,

          billingAddress:
            invoice.billing_address,

          shippingAddress:
            invoice.shipping_address,

          state:
            invoice.state,

          stateCode:
            invoice.state_code,
          
          isInterState:

  String(
    company?.statecode || ""
  ).trim()

  !==

  String(
    invoice.state_code || ""
  ).trim(),  

          subtotal:
            invoice.subtotal,

          cgst:
            invoice.cgst,

          sgst:
            invoice.sgst,

          igst:
            invoice.igst,

          gstPercent:
            invoice.gst_percent,

          grandTotal:
            invoice.grand_total,

          paymentMode:
            invoice.payment_mode,

          notes:
            invoice.notes,

          terms:
            invoice.terms,

          createdAt:
            invoice.created_at,

          company_code:
            invoice.company_code,

          client_code:
            invoice.client_code,

          billing_pincode:
            invoice.billing_pincode,

          shipping_state:
            invoice.shipping_state,

          shipping_stateCode:
            invoice.shipping_state_code,

          shipping_pincode:
            invoice.shipping_pincode,

          due_date:
            invoice.due_date,

          credit_period:
            invoice.credit_period,

          status:
            invoice.status,

          doc_type:
            invoice.doc_type,

          Quote_No:
            invoice.quote_no,

          Quote_Dt:
            invoice.quote_dt,

transporter_name:
  invoice.transporter_name,

vehicle_no:
  invoice.vehicle_no,

driver_mobile:
  invoice.driver_mobile,

gr_rr_no:
  invoice.gr_rr_no,

eway_bill_no:
  invoice.eway_bill_no,
  
            company: {

name:
  company?.businessname || "",

 businessName:
  company?.businessname || "",

  mobile:
    company?.mobile || "",

  address:
    company?.address || "",

  gstin:
    company?.gstin || "",

  state:
    company?.state || "",

  statecode:
    company?.statecode || "",

  pincode:
  company?.pincode || ""  ,

  country:
  company?.country || "",

currency:
  company?.currency || "",

currency_symbol:
  company?.currency_symbol || "",

defaultNotes:
  company?.defaultnotes || "",

defaultTerms:
  company?.defaultterms || "",

},
         items:

  (items || []).map((item) => ({

    id:
      item.id,

    name:
      item.item_name,

    hsn:
      item.hsn,

    quantity:
      item.qty,

    unit:
      item.unit,

     packingQty:
      Number(
        item.packingqty || 0
      ),

    packingUnit:
      item.packingunit || "",

    rate:
      item.rate,

    gstPct:
      item.gst_percent,

    discountPct:
      item.disc_percent,

    discountAmt:
      item.disc_amt,

    taxable:
      item.taxable_amount,

    cgst:
      item.cgst,

    sgst:
      item.sgst,

    igst:
      item.igst,

    total:
      item.total,

    invoiceMode:
      item.source

  }))

        }

      });

    }

    catch (err) {

      return res.status(500).json({

        success: false,

        error:
          err.message

      });

    }

  }
);


app.get(
  "/publicInvoice/:token",

  async (req, res) => {

    try {

      const token =

        String(
          req.params.token || ""
        ).trim();

      if (!token) {

        return res.json({

          success: false,

          error: "token missing"

        });

      }

      const {
        data: invoice,
        error: invoiceError
      } = await supabase

        .from("invoices")

        .select("*")

        .eq(
          "share_token",
          token
        )

        .single();

      if (invoiceError || !invoice) {

        return res.json({

          success: false,

          error: "Invoice not found"

        });

      }

      const {
        data: company
      } = await supabase

        .from("company")

        .select("*")

        .eq(
          "company_code",
          invoice.company_code
        )

        .single();

      const {
        data: items
      } = await supabase

        .from("invoice_items")

        .select("*")

        .eq(
          "invoice_id",
          invoice.invoice_id
        );

      return res.json({

        success: true,

        data: {

          invoice,
          company,
          items

        }

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error: err.message

      });

    }

  }
);

app.get("/api/getNextInvoiceNumber", async (req, res) => {

  console.log(
    "GET NEXT INVOICE API HIT"
  );

  console.log(
    "QUERY:",
    req.query
  );

  try {

    const company_code =
      String(
        req.query.company_code || ""
      ).trim();

    if (!company_code) {

      return res.json({
        success: false,
        error: "company_code missing"
      });

    }

    const invoiceDate =

  String(
    req.query.invoiceDate || ""
  ).trim();

    // =========================
    // COMPANY
    // =========================

  // =========================
// GET COMPANY
// =========================

const {
  data: company,
  error: companyError
} = await supabase

  .from("company")

  .select("*")

  .eq(
    "company_code",
    company_code
  )

  .single();

if (companyError || !company) {

  return res.json({

    success: false,

    error: "Company not found"

  });

}


const prefix =

  company.invoiceprefix || "INV";

const startNumber =

  Number(
    company.invoicestartnumber || 1
  );
    // =========================
    // FY
    // =========================

    const now =

  invoiceDate

    ? new Date(invoiceDate)

    : new Date();
    
    const fy =
      now.getMonth() >= 3
        ? now.getFullYear()
        : now.getFullYear() - 1;

    const fyStr =
      `${String(fy).slice(-2)}-${String(fy + 1).slice(-2)}`;

      console.log(
  "FY:",
  fyStr
);
    // =========================
    // ERP START FY
    // =========================

    const companyStart =

  new Date(
    company.company_start
  );

const startFY =

  companyStart.getMonth() >= 3

    ? companyStart.getFullYear()

    : companyStart.getFullYear() - 1;

const startFYStr =

  `${String(startFY).slice(-2)}-${String(startFY + 1).slice(-2)}`;


    // =========================
    // FETCH INVOICES
    // =========================

    const { data: invoices, error } =
      await supabase
        .from("invoices")
        .select("invoice_number")
        .eq("company_code", company_code);

        console.log(
  "INVOICES:",
  invoices
);

    if (error) {

      return res.json({
        success: false,
        error: error.message
      });

    }

    let maxNumber = 0;

    (invoices || []).forEach((row) => {

      const invoiceNumber =
  String(
    row.invoice_number || ""
  );

      // Example:
      // INV/26-27/0017

      const parts =
        invoiceNumber.split("/");

      if (
        parts.length === 3 &&
        parts[1] === fyStr
      ) {

        const num =
          parseInt(parts[2], 10);

        if (!isNaN(num)) {

          maxNumber =
            Math.max(maxNumber, num);

        }

      }

    });
// =========================
// FINAL NUMBER
// =========================

let nextNumber = 1;

// =========================
// COMPANY START FY
// =========================

if (fyStr === startFYStr) {

  const effectiveMax =

    Math.max(
      maxNumber,
      startNumber - 1
    );

  nextNumber =
    effectiveMax + 1;

}

// =========================
// OTHER FY
// =========================

else {

  nextNumber =
    maxNumber > 0

      ? maxNumber + 1

      : 1;

}

    console.log(
  "FINAL NEXT NUMBER:",
  `${prefix}/${fyStr}/${String(nextNumber).padStart(4, "0")}`
);



console.log(
  "PREFIX:",
  prefix
);
    return res.json({

      success: true,

      data: {

        invoiceNumber:
          `${prefix}/${fyStr}/${String(nextNumber).padStart(4, "0")}`

      }

    });

  } catch (err) {

    console.error(
      "GET NEXT INVOICE ERROR:",
      err
    );

    return res.json({
      success: false,
      error: err.message
    });

  }

});


// =========================
// GET LATEST INVOICE DATE
// =========================

app.get(
  "/api/getLatestInvoiceDate",
  async (req, res) => {

    try {

      console.log(
        "GET LATEST INVOICE DATE API HIT"
      );

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
// GET COMPANY
// =========================

const {
  data: company,
  error: companyError
} = await supabase


  .from("company")

  .select("company_start")

  .eq(
    "company_code",
    company_code
  )

  .single();

console.log(
  "COMPANY RAW:",
  company
);

console.log(
  "BUSINESS NAME:",
  company?.businessname
);
  
if (companyError || !company) {

  return res.json({

    success: false,

    error: "Company not found"

  });

}

      // =========================
      // FETCH LATEST DATE
      // =========================

      const {
        data,
        error
      } = await supabase

        .from("invoices")

        .select("invoice_date")

        .eq(
          "company_code",
          company_code
        )

        .not(
          "invoice_date",
          "is",
          null
        )

        .order(
          "invoice_date",
          {
            ascending: false
          }
        )

        .limit(1);

      if (error) {

        console.log(
          "GET LATEST INVOICE DATE ERROR:",
          error
        );

        return res.json({

          success: false,

          error:
            error.message

        });

      }


      console.log(
  "LATEST INVOICE DATE DATA:",
  data
);

console.log(
  "FINAL LATEST DATE:",
  data &&
  data.length > 0

    ? data[0].invoice_date

    : ""
);

      const latestInvoiceDate =

  data &&
  data.length > 0

    ? data[0].invoice_date

    : "";

const companyStart =

  company.company_start || "";

const finalDate =

  latestInvoiceDate >
  companyStart

    ? latestInvoiceDate

    : companyStart;

console.log(
  "COMPANY START:",
  companyStart
);

console.log(
  "FINAL ALLOWED DATE:",
  finalDate
);

return res.json({

  success: true,

  data: {

    invoiceDate:
      finalDate

  }

});

    }

    catch (err) {

      console.log(
        "GET LATEST INVOICE DATE ERROR:",
        err
      );

      
      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


  // =========================
  // GET INVOICES
  // =========================

  app.get(
    "/getInvoices",
    async (req, res) => {

      try {

        const company_code =

          String(
            req.query.company_code || ""
          ).trim();

        if (!company_code) {

          return res.json({

            success: false,

            error: "company_code missing"

          });

        }

        const page = Number(
          req.query.page || 1
        );

        const limit = Number(
          req.query.limit || 10
        );

        const period =
  String(req.query.period || "all");

const statusFilter =
  String(req.query.statusFilter || "all");

const docTypeFilter =
  String(req.query.docTypeFilter || "invoice");

  console.log({
  period,
  statusFilter,
  docTypeFilter
});

        const from =
          (page - 1) * limit;

        const to =
          from + limit - 1;

          console.log(
  "PAGE:",
  page,
  "LIMIT:",
  limit,
  "FROM:",
  from,
  "TO:",
  to
);
        const {
          data,
          error
        } = await supabase

          .from("invoices")

          .select("*")

          .eq(
            "company_code",
            company_code
          )

      .order(
  "created_at",
  {
    ascending: false
  }
);
console.log(
  "ROWS RETURNED:",
  data?.length
);

        if (error) {

          return res.json({

            success: false,

            error: error.message

          });

        }

        const result =

          (data || []).map((row) => ({

            invoiceId:
              row.invoice_id,

            invoiceNumber:
              row.invoice_number,

            invoiceDate:
              row.invoice_date,

            customerName:
              row.customer_name,

            customerMobile:
              row.customer_mobile,

            customerGST:
              row.customer_gst,

            billingAddress:
              row.billing_address,

            shippingAddress:
              row.shipping_address,

            state:
              row.state,

            stateCode:
              row.state_code,

            subtotal:
              row.subtotal,

            cgst:
              row.cgst,

            sgst:
              row.sgst,

            igst:
              row.igst,

            gstPercent:
              row.gst_percent,

            grandTotal:
              row.grand_total,

            paymentMode:
              row.payment_mode,

            notes:
              row.notes,

            terms:
              row.terms,

            createdAt:
              row.created_at,

            company_code:
              row.company_code,

            client_code:
              row.client_code,

            billing_pincode:
              row.billing_pincode,

            shipping_state:
              row.shipping_state,

            shipping_stateCode:
              row.shipping_state_code,

            shipping_pincode:
              row.shipping_pincode,

            due_date:
              row.due_date,

            credit_period:
              row.credit_period,

            status:
              row.status,

            doc_type:

              String(
                row.doc_type || ""
              )
                .toLowerCase()
                .trim() === "quotation"

                ? "quotation"

                : "invoice",

            quote_no:
              row.quote_no,

          quote_dt:
    row.quote_dt,

  transporter_name:
    row.transporter_name,

  vehicle_no:
    row.vehicle_no,

  driver_mobile:
    row.driver_mobile,

  gr_rr_no:
    row.gr_rr_no,

  eway_bill_no:
    row.eway_bill_no,

  share_token:
    row.share_token

          }));

          let filteredResult = [...result];

// Doc Type Filter
if (docTypeFilter !== "all") {
  filteredResult = filteredResult.filter(
    (i) => i.doc_type === docTypeFilter
  );
}

// Status Filter
if (statusFilter === "posted") {
  filteredResult = filteredResult.filter(
    (i) =>
      String(i.status || "")
        .trim()
        .toLowerCase() === "posted"
  );
}

if (statusFilter === "not_posted") {
  filteredResult = filteredResult.filter(
    (i) =>
      String(i.status || "")
        .trim()
        .toLowerCase() !== "posted"
  );
}

  // Period Filter
if (period !== "all") {

  const now = new Date();
  let f = new Date(0);
  let t = new Date();

  if (period === "today") {

    f = new Date(
      now.getFullYear(),
      now.getMonth(),
      now.getDate()
    );

    t = new Date(
      now.getFullYear(),
      now.getMonth(),
      now.getDate(),
      23,
      59,
      59
    );

  }

  else if (period === "week") {

    const day = now.getDay();

    f = new Date(
      now.getFullYear(),
      now.getMonth(),
      now.getDate() - day
    );

  }

  else if (period === "month") {

    f = new Date(
      now.getFullYear(),
      now.getMonth(),
      1
    );

  }

  else if (period === "lastMonth") {

    f = new Date(
      now.getFullYear(),
      now.getMonth() - 1,
      1
    );

    t = new Date(
      now.getFullYear(),
      now.getMonth(),
      0,
      23,
      59,
      59
    );

  }

  

  filteredResult = filteredResult.filter((i) => {

    const filterDate =
      i.doc_type === "quotation"
        ? i.quote_dt
        : i.invoiceDate;

    const d = new Date(filterDate);

    return d >= f && d <= t;

  });

}


// Pagination AFTER filter
const paginatedData =
  filteredResult.slice(
    from,
    to + 1
  );

     return res.json({

  success: true,

  data: paginatedData,

  total: filteredResult.length

});

      }

      catch (err) {

        return res.json({

          success: false,

          error: err.message

        });

      }

    }
  );


// =========================
// GET INVOICE ITEMS
// =========================

app.get(

  "/getInvoiceItems",

  async (req, res) => {

    try {

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // MM DATA
      // =========================

      const {
        data: mmRows,
        error: mmError
      } = await supabase

        .from("mm")

        .select("*")

        .eq(
          "company_code",
          company_code
        );

      if (mmError) {

        return res.json({

          success: false,

          error:
            mmError.message

        });

      }

      // =========================
      // VARIANT DATA
      // =========================

      const {
        data: variantRows,
        error: variantError
      } = await supabase

        .from("variant")

        .select("*")

        .eq(
          "company_code",
          company_code
        );

      if (variantError) {

        return res.json({

          success: false,

          error:
            variantError.message

        });

      }

      // =========================
      // FINAL RESULT
      // =========================

      const result = [];

      // =========================
      // SINGLE ITEMS
      // =========================

      mmRows.forEach((row) => {

        const matType =

          String(
            row.mat_type || ""
          )
            .trim()
            .toLowerCase();

        if (

          matType === "single"

          &&

          row.is_active === true

          &&

          !row.hold

        ) {

 // =========================
  // DEBUG SINGLE ITEM
  // =========================

  console.log(
    "FINAL SINGLE ITEM:",
    {

      material_id:
        row.material_id,

      item_name:
        row.item_name,

      mrp:
        row.mrp,

      disc_percent:
        row.disc_percent,

      disc_amt:
        row.disc_amt,

      sale_rate:
        row.sale_rate,

      base_unit:
        row.base_unit,

      finalResponse: {

        rate:
          Number(
            row.mrp || 0
          ),

        discountPct:
          Number(
            row.disc_percent || 0
          ),

        discountAmt:
          Number(
            row.disc_amt || 0
          ),

        sale_rate:
          Number(
            row.sale_rate || 0
          )

      }

    }
  );

          
          result.push({

  mode: "single",

  material_id:
    row.material_id,

  item_code:
    String(
      row.item_code || ""
    ),

  item_name:
    row.item_name || "",

  display_name:
    row.item_name || "",

  hsn_code:
    String(
      row.hsn_code || ""
    ),

  gst_percent:
    Number(
      row.gst_percent || 0
    ),

  unit:
    row.base_unit || "",

  // =========================
  // PRICING
  // =========================

  mrp:
    Number(
      row.mrp || 0
    ),

  discountPct:
    Number(
      row.disc_percent || 0
    ),

  discountAmt:
    Number(
      row.disc_amt || 0
    ),

  disc_percent:
    Number(
      row.disc_percent || 0
    ),

  disc_amt:
    Number(
      row.disc_amt || 0
    ),

  sale_rate:
    Number(
      row.sale_rate || 0
    ),

  // IMPORTANT
  // frontend expects MRP here
  rate:
    Number(
      row.mrp || 0
    )

});

        }

      });

      // =========================
      // MULTI ITEMS
      // =========================

      variantRows.forEach((row) => {

        console.log(
              "RAW VARIANT ROW:",
              row
            );

       if (

  row.is_active !== true

  ||

  row.hold

) {

  return;

}

        // =========================
        // FIND MM ROW
        // =========================

        const mmRow =

          mmRows.find(

            (m) =>

              Number(
                m.material_id
              ) ===

              Number(
                row.material_id
              )

          );

        if (!mmRow) return;

                if (

          mmRow.is_active !== true

          ||

          mmRow.hold

        ) {

          return;

        }

        const matType =

          String(
            mmRow.mat_type || ""
          )
            .trim()
            .toLowerCase();

        if (
          matType !== "multi"
        ) {

          return;

        }

        console.log(
  "FINAL MULTI ITEM:",
  {

    description:
      row.description,

    variant:
      row.variant,

    selling_rate:
      row.selling_rate,

    sale_rate:
      row.sale_rate,

    mrp:
      row.mrp,

    disc_percent:
      row.disc_percent,

    disc_amt:
      row.disc_amt,

    final_multiplier:
      row.final_multiplier

  }
);

      // =========================
// CALCULATE VALUES
// =========================

const mrp =
  Number(row.mrp || 0);

const discountPct =
  Number(
    row.disc_percent || 0
  );

const discountAmt =
  Number(
    row.disc_amt || 0
  );

const finalRate =

  Number(
    row.selling_rate ||
    row.sale_rate ||
    0
  );

// =========================
// DEBUG
// =========================

console.log(
  "RATE DEBUG:",
  {

    material_id:
      row.material_id,

    description:
      row.description,

    variant:
      row.variant,

    mrp,

    discountPct,

    discountAmt,

    finalRate,

    expectedFrontend: {

      rate: mrp,

      discountPct,

      netRate:
        finalRate

    }

  }
);

// =========================
// PUSH FINAL RESULT
// =========================

result.push({

  mode: "multi",

  material_id:
    row.material_id,

  variant_code:
    String(
      row.variant_code || ""
    ),

  item_name:
    row.item_name || "",

  description:
    row.description || "",

  variant:
    row.variant || "",

  sub_variant:
    row.sub_variant || "",

  qty:
    Number(
      row.qty || 0
    ),

  final_multiplier:
    Number(
      row.final_multiplier || 0
    ),

  base_unit:
    row.base_unit || "",

  display_name:
    row.description || "",

  hsn_code:
    String(
      mmRow.hsn_code || ""
    ),

  gst_percent:
    Number(
      mmRow.gst_percent || 0
    ),

  unit:
    row.variant || "",

  // =========================
  // PRICING
  // =========================

  mrp,

  discountPct,

  discountAmt,

  disc_percent:
    discountPct,

  disc_amt:
    discountAmt,

  discount:
    discountPct,

  discount_amount:
    discountAmt,

  sale_rate:
    finalRate,

  // IMPORTANT
  // frontend expects ORIGINAL MRP
  // not discounted rate
  rate:
    mrp ||

    finalRate ||

    0

});

      });

      console.log(
  "FINAL RESULT COUNT:",
  result.length
);

console.log(
  "FIRST RESULT:",
  result[0]
);

      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        data: result

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }

);


// =========================
// UPDATE TRANSPORT
// =========================

app.post(
  "/updateTransport",

  async (req, res) => {

    try {

      console.log(
        "UPDATE TRANSPORT BODY:",
        req.body
      );

      const invoiceId =

        String(
          req.body.invoiceId || ""
        ).trim();

      const company_code =

        String(
          req.body.company_code || ""
        ).trim();

      if (!invoiceId) {

        return res.json({

          success: false,

          error:
            "invoiceId missing"

        });

      }

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      const {
        error
      } = await supabase

        .from("invoices")

        .update({

          transporter_name:
            req.body.transporter_name || "",

          vehicle_no:
            req.body.vehicle_no || "",

          driver_mobile:
            req.body.driver_mobile || "",

          gr_rr_no:
            req.body.gr_rr_no || "",

          eway_bill_no:
            req.body.eway_bill_no || ""

        })

        .eq(
          "invoice_id",
          invoiceId
        )

        .eq(
          "company_code",
          company_code
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      return res.json({

        success: true,

        message:
          "Transport updated successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// POST INVOICE
// =========================

app.post(
  "/postInvoice",
  async (req, res) => {

    try {

      const invoiceId =

        String(
          req.body.invoiceId || ""
        ).trim();

      const company_code =

        String(
          req.body.company_code || ""
        ).trim();

      console.log(
        "========================="
      );

      console.log(
        "POST INVOICE HIT"
      );

      console.log(
        "INVOICE ID:",
        invoiceId
      );

      console.log(
        "COMPANY CODE:",
        company_code
      );

      if (!invoiceId) {

        return res.json({

          success: false,

          error:
            "invoiceId missing"

        });

      }

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // GET CURRENT INVOICE
      // =========================

      const {
        data: currentInvoice,
        error: currentError
      } = await supabase

        .from("invoices")

        .select("*")

        .eq(
          "invoice_id",
          invoiceId
        )

        .eq(
          "company_code",
          company_code
        )

        .single();

      if (currentError || !currentInvoice) {

        return res.json({

          success: false,

          error:
            "Invoice not found"

        });

      }

      console.log(
        "CURRENT INVOICE NUMBER:",
        currentInvoice.invoice_number
      );

      console.log(
        "CURRENT INVOICE DATE:",
        currentInvoice.invoice_date
      );

      // =========================
      // EXTRACT FY + SEQUENCE
      // =========================

      const currentParts =

        String(
          currentInvoice.invoice_number || ""
        ).split("/");

      const currentFY =
        currentParts[1] || "";

      const currentSequence =
        currentParts[2] || "";

      console.log(
        "CURRENT FY:",
        currentFY
      );

      console.log(
        "CURRENT SEQUENCE:",
        currentSequence
      );

      // =========================
      // FIND DUPLICATE
      // SAME COMPANY
      // SAME FY
      // SAME SEQUENCE
      // POSTED ONLY
      // =========================

      const {
        data: postedInvoices,
        error: postedError
      } = await supabase

        .from("invoices")

        .select(`
          invoice_id,
          invoice_number,
          invoice_date,
          status
        `)

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "status",
          "Posted"
        );

      if (postedError) {

        return res.json({

          success: false,

          error:
            postedError.message

        });

      }

      let duplicateFound =
        false;

      let maxSequence = 0;

      let latestPostedDate =

        currentInvoice.invoice_date;

      (postedInvoices || []).forEach((row) => {

        const parts =

          String(
            row.invoice_number || ""
          ).split("/");

        const fy =
          parts[1] || "";

        const seq =
          Number(parts[2] || 0);

        // =========================
        // SAME FY
        // =========================

        if (fy === currentFY) {

          // latest sequence

          maxSequence =
            Math.max(
              maxSequence,
              seq
            );

          // latest invoice date

          if (

            row.invoice_date >

            latestPostedDate

          ) {

            latestPostedDate =
              row.invoice_date;

          }

          // duplicate sequence
console.log(
  "CHECKING SEQUENCE:",
  seq,
  currentSequence
);
          if (

  Number(seq) ===
  Number(currentSequence)

)  {

            duplicateFound =
              true;

          }

        }

      });

      console.log(
        "DUPLICATE FOUND:",
        duplicateFound
      );

      console.log(
        "MAX SEQUENCE:",
        maxSequence
      );

      console.log(
        "LATEST POSTED DATE:",
        latestPostedDate
      );

      let finalInvoiceNumber =

        currentInvoice.invoice_number;

      let finalInvoiceDate =

        currentInvoice.invoice_date;

      // =========================
      // DUPLICATE CASE
      // =========================

      if (duplicateFound) {

        console.log(
          "DUPLICATE CASE STARTED"
        );

        const {
          data: company,
          error: companyError
        } = await supabase

          .from("company")

          .select("*")

          .eq(
            "company_code",
            company_code
          )

          .single();

        if (companyError || !company) {

          return res.json({

            success: false,

            error:
              "Company not found"

          });

        }

        const prefix =

          company.invoiceprefix || "INV";

        const nextSequence =

          maxSequence + 1;

        finalInvoiceNumber =

          `${prefix}/${currentFY}/${String(nextSequence).padStart(4, "0")}`;

        finalInvoiceDate =
          latestPostedDate;

        console.log(
          "NEW INVOICE NUMBER:",
          finalInvoiceNumber
        );

        console.log(
          "NEW INVOICE DATE:",
          finalInvoiceDate
        );

        // =========================
        // UPDATE INVOICE
        // =========================

        const {
          error: updateError
        } = await supabase

          .from("invoices")

          .update({

            invoice_number:
              finalInvoiceNumber,

            invoice_date:
              finalInvoiceDate

          })

          .eq(
            "invoice_id",
            invoiceId
          )

          .eq(
            "company_code",
            company_code
          );

        if (updateError) {

          return res.json({

            success: false,

            error:
              updateError.message

          });

        }

      }

      // =========================
      // FINAL POST
      // =========================

      const {
        data,
        error
      } = await supabase

        .from("invoices")

        .update({

          status:
            "Posted"

        })

        .eq(
          "invoice_id",
          invoiceId
        )

        .eq(
          "company_code",
          company_code
        )

        .select();

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      console.log(
        "FINAL POST SUCCESS"
      );

      return res.json({

        success: true,

        message:
          "Invoice posted",

        data: {

          invoiceNumber:
            finalInvoiceNumber,

          invoiceDate:
            finalInvoiceDate

        }

      });

    }

    catch (err) {

      console.log(
        "POST ERROR:",
        err
      );

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);



// =========================
// GET CLIENTS
// =========================

app.get(
  "/getClients",
  async (req, res) => {

    try {

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

      console.log(
        "GET CLIENTS API HIT"
      );

      console.log(
        "COMPANY CODE:",
        company_code
      );

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // FETCH CLIENTS
      // =========================

      const {
        data,
        error
      } = await supabase

        .from("client")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .order(
          "created_at",
          {
            ascending: false
          }
        );

      console.log(
        "RAW CLIENT DATA:",
        data
      );

      if (error) {

        console.log(
          "GET CLIENTS ERROR:",
          error
        );

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      // =========================
      // FINAL RESPONSE
      // =========================

      const result =

        (data || []).map((row) => ({

          company_code:
            String(
              row.company_code || ""
            ),

          client_code:
            String(
              row.client_code || ""
            ),

          client_name:
            row.client_name || "",

          gstin:
            row.gstin || "",

          billing_address:
            row.billing_address || "",

          bill_state:
            row.bill_state || "",

          bill_stateCode:
            String(
              row.bill_statecode || ""
            ),

          bill_pincode:
            String(
              row.bill_pincode || ""
            ),

          mobile:
            String(
              row.mobile || ""
            ),

          email:
            row.email || "",

          same_address:
            row.same_address || "",

          shipping_address:
            row.shipping_address || "",

          shipping_state:
            row.shipping_state || "",

          shipping_stateCode:
            String(
              row.shipping_statecode || ""
            ),

          shipping_pincode:
            String(
              row.shipping_pincode || ""
            ),

          createdAt:
            row.created_at || "",

          updatedAt:
            row.updated_at || "",

          is_active:
            row.is_active,

          contact_person:
            row.contact_person || "",

          credit_limit:
            row.credit_limit || 0,

          opening_balance:
            row.opening_balance || 0,

          remarks:
            row.remarks || "",

          credit_period:
            row.credit_period || 0,

            gst_registered:

  row.gst_registered !== false

        }));

      console.log(
        "FINAL CLIENT RESPONSE:",
        result
      );

      return res.json({

        success: true,

        data: result

      });

    }

    catch (err) {

      console.log(
        "GET CLIENTS CATCH ERROR:",
        err
      );

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


// =========================
// SAVE CLIENT
// =========================

app.post(
  "/saveClient",
  async (req, res) => {

    try {

      const body =
        req.body || {};

      console.log(
        "SAVE CLIENT API HIT"
      );

      console.log(
        "REQ BODY:",
        body
      );

      // =========================
      // COMPANY CODE
      // =========================

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // FETCH ALL CLIENTS
      // =========================

      const {
        data: allClients,
        error: fetchError
      } = await supabase

        .from("client")

        .select("*")

        .eq(
          "company_code",
          company_code
        );

      if (fetchError) {

        console.log(
          "FETCH CLIENT ERROR:",
          fetchError
        );

        return res.json({

          success: false,

          error:
            fetchError.message

        });

      }

      console.log(
        "ALL CLIENTS:",
        allClients
      );

      // =========================
      // DUPLICATE GST CHECK
      // =========================

     const gstin =

  String(
    body.gstin || ""
  )
    .trim()
    .toUpperCase();

const duplicateGST =

  body.gst_registered !== false

  &&

  gstin

    ? (allClients || []).find((row) => {

        return (

          String(
            row.gstin || ""
          )
            .trim()
            .toUpperCase()

          ===

          gstin

        );

      })

    : null;

if (duplicateGST) {

  return res.json({

    success: false,

    error:
      "Client with same GSTIN already exists"

  });

}
      // =========================
      // GENERATE CLIENT CODE
      // =========================

      let maxNumber = 0;

      (allClients || []).forEach((row) => {

        const code =

          String(
            row.client_code || ""
          );

        const num =

          parseInt(
            code.replace("CL", "")
          );

        if (!isNaN(num)) {

          maxNumber =

            Math.max(
              maxNumber,
              num
            );

        }

      });

      const nextClientCode =

        "CL" +

        String(maxNumber + 1)
          .padStart(4, "0");

      console.log(
        "NEXT CLIENT CODE:",
        nextClientCode
      );

      // =========================
      // FALLBACK DUPLICATE
      // =========================

      const duplicateClientCode =

        (allClients || []).find((row) => {

          return (

            String(
              row.client_code || ""
            ).trim()

            ===

            nextClientCode

          );

        });

      if (duplicateClientCode) {

        return res.json({

          success: false,

          error:
            "Client code already exists. Please save again."

        });

      }

      // =========================
      // INSERT CLIENT
      // =========================

      const insertObj = {

        company_code:
          company_code,

        client_code:
          nextClientCode,

        client_name:
          body.client_name || "",

        gstin:
          body.gstin || "",

        billing_address:
          body.billing_address || "",

        bill_state:
          body.bill_state || "",

        bill_statecode:
          body.bill_stateCode || "",

        bill_pincode:
          body.bill_pincode || "",

        mobile:
          body.mobile || "",

        email:
          body.email || "",

        same_address:
          body.same_address || "",

        shipping_address:
          body.shipping_address || "",

        shipping_state:
          body.shipping_state || "",

        shipping_statecode:
          body.shipping_stateCode || "",

        shipping_pincode:
          body.shipping_pincode || "",

        created_at:
          new Date(),

        updated_at:
          new Date(),

        is_active:

          body.is_active !== false,

        contact_person:
          body.contact_person || "",

        credit_limit:
          Number(
            body.credit_limit || 0
          ),

        opening_balance:
          Number(
            body.opening_balance || 0
          ),

        remarks:
          body.remarks || "",

        credit_period:
          Number(
            body.credit_period || 0
          ),

          gst_registered:

  body.gst_registered !== false

      };

      console.log(
        "FINAL INSERT OBJECT:",
        insertObj
      );

      const {
        error: insertError
      } = await supabase

        .from("client")

        .insert([

          insertObj

        ]);

      if (insertError) {

        console.log(
          "INSERT ERROR:",
          insertError
        );

        return res.json({

          success: false,

          error:
            insertError.message

        });

      }

      console.log(
        "CLIENT SAVED SUCCESS"
      );

      return res.json({

        success: true,

        message:
          "Client saved successfully",

        client_code:
          nextClientCode

      });

    }

    catch (err) {

      console.log(
        "SAVE CLIENT ERROR:",
        err
      );

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

app.post(
  "/updateClient",
  async (req, res) => {

    try {

      const body =
        req.body || {};

      console.log(
        "UPDATE CLIENT API HIT"
      );

      console.log(
        "REQ BODY:",
        body
      );

      // =========================
      // VALIDATION
      // =========================

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      const client_code =

        String(
          body.client_code || ""
        ).trim();

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!client_code) {

        return res.json({

          success: false,

          error:
            "client_code missing"

        });

      }

      // =========================
      // FETCH ALL CLIENTS
      // =========================

      const {
        data: allClients,
        error: fetchError
      } = await supabase

        .from("client")

        .select("*")

        .eq(
          "company_code",
          company_code
        );

      if (fetchError) {

        console.log(
          "FETCH CLIENT ERROR:",
          fetchError
        );

        return res.json({

          success: false,

          error:
            fetchError.message

        });

      }

      // =========================
      // DUPLICATE GST CHECK
      // =========================
const gstin =

  String(
    body.gstin || ""
  )
    .trim()
    .toUpperCase();

const duplicateGST =

  body.gst_registered !== false

  &&

  gstin

    ? (allClients || []).find((row) => {

        return (

          String(
            row.gstin || ""
          )
            .trim()
            .toUpperCase()

          ===

          gstin

          &&

          String(
            row.client_code || ""
          ).trim()

          !==

          client_code

        );

      })

    : null;

if (duplicateGST) {

  return res.json({

    success: false,

    error:
      "Client with same GSTIN already exists"

  });

}
      // =========================
      // CHECK CLIENT EXISTS
      // =========================

      const existingClient =

        (allClients || []).find((row) => {

          return (

            String(
              row.client_code || ""
            ).trim()

            ===

            client_code

          );

        });

      if (!existingClient) {

        return res.json({

          success: false,

          error:
            "Client not found"

        });

      }

      // =========================
      // UPDATE OBJECT
      // =========================

      const updateObj = {

        client_name:
          body.client_name || "",

        gstin:
          body.gstin || "",

        billing_address:
          body.billing_address || "",

        bill_state:
          body.bill_state || "",

        bill_statecode:
          body.bill_stateCode || "",

        bill_pincode:
          body.bill_pincode || "",

        mobile:
          body.mobile || "",

        email:
          body.email || "",

        same_address:
          body.same_address || "",

        shipping_address:
          body.shipping_address || "",

        shipping_state:
          body.shipping_state || "",

        shipping_statecode:
          body.shipping_stateCode || "",

        shipping_pincode:
          body.shipping_pincode || "",

        updated_at:
          new Date(),

        is_active:

          body.is_active !== false,

        contact_person:
          body.contact_person || "",

        credit_limit:
          Number(
            body.credit_limit || 0
          ),

        opening_balance:
          Number(
            body.opening_balance || 0
          ),

        remarks:
          body.remarks || "",

        credit_period:
          Number(
            body.credit_period || 0
          ),

          gst_registered:

  body.gst_registered !== false

      };

      console.log(
        "FINAL UPDATE OBJECT:",
        updateObj
      );

      // =========================
      // UPDATE CLIENT
      // =========================

      const {
        error: updateError
      } = await supabase

        .from("client")

        .update(updateObj)

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "client_code",
          client_code
        );

      if (updateError) {

        console.log(
          "UPDATE ERROR:",
          updateError
        );

        return res.json({

          success: false,

          error:
            updateError.message

        });

      }

      console.log(
        "CLIENT UPDATED SUCCESS"
      );

      return res.json({

        success: true,

        message:
          "Client updated successfully"

      });

    }

    catch (err) {

      console.log(
        "UPDATE CLIENT ERROR:",
        err
      );

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

console.log("SIGNUP ROUTE LOADED");

// =========================
// SIGNUP USER
// =========================

app.post(
  "/signupUser",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      // =========================
      // VALIDATION
      // =========================

      const name =
        String(body.name || "").trim();

      const mobile =
        String(body.mobile || "").trim();

      const email =
        String(body.email || "")
          .trim()
          .toLowerCase();

      const password =
        String(body.password || "").trim();

      if (
        !name ||
        !mobile ||
        !email ||
        !password
      ) {

        return res.json({

          success: false,

          error:
            "All fields required"

        });

      }

      // =========================
      // DUPLICATE CHECK
      // =========================

      const {
        data: existingUsers,
        error: duplicateError
      } = await supabase

        .from("usersheet")

        .select("*")

        .or(
          `email.eq.${email},mobile.eq.${mobile}`
        );

      if (duplicateError) {

        console.log(
          "DUPLICATE CHECK ERROR:",
          duplicateError
        );

        return res.json({

          success: false,

          error:
            duplicateError.message

        });

      }

      if (
        existingUsers &&
        existingUsers.length > 0
      ) {

        return res.json({

          success: false,

          error:
            "User already exists"

        });

      }

      // =========================
      // GET LAST COMPANY
      // =========================

      const {
        data: companies,
        error: companyError
      } = await supabase

        .from("company")

        .select("company_code")

        .order(
          "id",
          {
            ascending: false
          }
        )

        .limit(1);

      if (companyError) {

        console.log(
          "COMPANY FETCH ERROR:",
          companyError
        );

        return res.json({

          success: false,

          error:
            companyError.message

        });

      }

      let nextCompanyCode =
        "C0001";

      if (
        companies &&
        companies.length > 0
      ) {

        const lastCode =

          String(
            companies[0]
              ?.company_code || ""
          );

        const lastNumber =

          parseInt(
            lastCode.replace("C", "")
          ) || 0;

        nextCompanyCode =

          "C" +

          String(
            lastNumber + 1
          ).padStart(4, "0");

      }

      console.log(
        "NEXT COMPANY CODE:",
        nextCompanyCode
      );

      // =========================
      // INSERT USER
      // =========================

      const {
        error: userInsertError
      } = await supabase

        .from("usersheet")

        .insert([{

          user_id:
            mobile,

          password,

          email,

          mobile,

          company_code:
            nextCompanyCode,

          is_active:
            true,

          created_at:
            new Date(),

          name

        }]);

      if (userInsertError) {

        console.log(
          "USER INSERT ERROR:",
          userInsertError
        );

        return res.json({

          success: false,

          error:
            userInsertError.message

        });

      }

      // =========================
      // INSERT COMPANY
      // =========================

      const {
        error: companyInsertError
      } = await supabase

        .from("company")

        .insert([{

          businessname:
            "",

          gstin:
            "",

          mobile,

          email,

          address:
            "",

          state:
            "",

          statecode:
            "",

          pincode:
            "",

          country:
            "",

          currency:
            "",

          currency_symbol:
            "",

          invoiceprefix:
            "INV",

          invoicestartnumber:
            1,

          company_code:
            nextCompanyCode,

          is_active:
            true,

          company_start:
            new Date(),

         license_type:
  "Royal"

        }]);

      if (companyInsertError) {

        console.log(
          "COMPANY INSERT ERROR:",
          companyInsertError
        );

        return res.json({

          success: false,

          error:
            companyInsertError.message

        });

      }

      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        message:
          "Signup successful",

        data: {

          company_code:
            nextCompanyCode,

          email,

          mobile,

          name

        }

      });

    }

    catch (err) {

      console.log(
        "SIGNUP ERROR:",
        err
      );

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// GET STATES
// =========================

app.get(
  "/getStates",

  async (req, res) => {

    try {

      const {
        data,
        error
      } = await supabase

        .from("state_master")

        .select("*")

        .order(
          "state_name",
          {
            ascending: true
          }
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      return res.json({

        success: true,

        data: data || []

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// GET COMPANY
// =========================

app.get(
  "/getCompany",

  async (req, res) => {

    try {

      const companyCode =

        String(
          req.query.companyCode || ""
        ).trim();

      if (!companyCode) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      const {
        data,
        error
      } = await supabase

        .from("company")

        .select("*")

        .eq(
          "company_code",
          companyCode
        )

        .single();

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

   const formattedData = {

        ...data,

        business_name:
          data.businessname,

        businessName:
          data.businessname,

  // =====================
  // COUNTRY
  // =====================

  country:
    String(
      data.country || ""
    ).trim(),

  // =====================
  // CURRENCY
  // =====================

  currency:
    String(
      data.currency || ""
    ).trim(),

  currency_symbol:
    String(
      data.currency_symbol || ""
    ).trim(),

     // =====================
  // DEFAULT NOTES
  // =====================

  defaultNotes:
    data.defaultnotes || "",

  defaultTerms:
    data.defaultterms || "",


      };

      return res.json({

        success: true,

         data: formattedData

      });

    }

    catch (err) {

      
      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


app.post(
  "/completeCompanySetup",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      const {
        error
      } = await supabase

        .from("company")

        .update(body)

        .eq(
          "company_code",
          company_code
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      return res.json({

        success: true,

        message:
          "Company updated"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

app.post(
  "/saveCompany",

  async (req, res) => {

    try {

      const companyCode =

        String(
          req.query.companyCode || ""
        ).trim();

      const body =
        req.body || {};

      if (!companyCode) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      const updateObj = {

        businessname:
          body.businessName || "",

        gstin:
          body.gstin || "",

        address:
          body.address || "",

        state:
          body.state || "",

        statecode:
          body.stateCode || "",

        country:
          body.country || "",

        currency:
          body.currency || "",

        currency_symbol:
          body.currency_symbol || "",

        mobile:
          body.mobile || "",

        email:
          body.email || "",

        bankname:
          body.bankName || "",

        accountnumber:
          body.accountNumber || "",

        ifsc:
          body.ifsc || "",

        upiid:
          body.upiId || "",

        logo:
          body.logo || "",

        defaultnotes:
          body.defaultNotes || "",

        defaultterms:
          body.defaultTerms || "",

        invoiceprefix:
          body.invoicePrefix || "INV",

        invoicestartnumber:
          Number(
            body.invoiceStartNumber || 1
          ),

        pincode:
          body.pincode || "",

        company_start:
          body.company_start || null,

        ca:
          body.CA || "",

        license_type:
          body.license_type || "",

          allow_posted_invoice_edit:
  body.allow_posted_invoice_edit ?? false,

        updatedat:
          new Date()

      };

      console.log(
        "FINAL UPDATE OBJECT:",
        updateObj
      );

      const {
        error
      } = await supabase

        .from("company")

        .update(updateObj)

        .eq(
          "company_code",
          companyCode
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      return res.json({

        success: true,

        message:
          "Company saved successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


// =========================
// GET CATEGORY MASTER
// =========================

app.get(
  "/getCategoryMaster",

  async (req, res) => {

    try {

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // FETCH CAT MASTER
      // =========================

      const {
        data: catData,
        error: catError
      } = await supabase

        .from("cat")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "active",
          true
        );

      if (catError) {

        return res.json({

          success: false,

          error:
            catError.message

        });

      }

      // =========================
      // FETCH CATEGORY MAPPING
      // =========================

      const {
        data: categoryData,
        error: categoryError
      } = await supabase

        .from("category")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "active",
          true
        );

      if (categoryError) {

        return res.json({

          success: false,

          error:
            categoryError.message

        });

      }

      // =========================
      // FINAL RESULT
      // =========================

      const result =

        (catData || []).map((row) => {

          const catName =

            String(
              row.cat || ""
            )
              .trim()
              .toLowerCase();

          const is_used =

            (categoryData || []).some((mapRow) => {

              return (

                String(
                  mapRow.cat || ""
                )
                  .trim()
                  .toLowerCase()

                ===

                catName

              );

            });

          return {

            ...row,

            is_used

          };

        });

      return res.json({

        success: true,

        data: result

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


// =========================
// GET SUB CATEGORY MASTER
// =========================

app.get(
  "/getSubCategoryMaster",

  async (req, res) => {

    try {

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

      const cat_id =

        String(
          req.query.cat_id || ""
        ).trim();

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // FETCH SUB CATEGORY MASTER
      // =========================

      let query =

        supabase

          .from("sub_category_master")

          .select("*")

          .eq(
            "company_code",
            company_code
          )

          .eq(
            "active",
            true
          );

      // =========================
      // OPTIONAL CAT FILTER
      // =========================

      if (cat_id) {

        query =

          query.eq(
            "cat_id",
            Number(cat_id)
          );

      }

      const {
        data,
        error
      } = await query;

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      // =========================
      // FETCH CATEGORY TABLE
      // =========================

      const {
        data: categoryData,
        error: categoryError
      } = await supabase

        .from("category")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "active",
          true
        );

      if (categoryError) {

        return res.json({

          success: false,

          error:
            categoryError.message

        });

      }

      // =========================
      // FINAL RESULT
      // =========================

      const result =

        (data || []).map((row) => {

          const subCatName =

            String(
              row.sub_cat || ""
            )
              .trim()
              .toLowerCase();

          const is_used =

            (categoryData || []).some((catRow) => {

              return (

                String(
                  catRow.sub_cat || ""
                )
                  .trim()
                  .toLowerCase()

                ===

                subCatName

              );

            });

          return {

  ...row,

  HSN:
    row.hsn || "",

  is_used

};
        });

      return res.json({

        success: true,

        data: result

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// SAVE CATEGORY
// =========================

app.post(
  "/saveCategory",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      const cat =

        String(
          body.cat || ""
        ).trim();

      const active =

        String(
          body.active || ""
        )
          .trim()
          .toUpperCase() === "TRUE";

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!cat) {

        return res.json({

          success: false,

          error:
            "Category required"

        });

      }

      // =========================
      // DUPLICATE CHECK
      // =========================

      const {
        data: existingData,
        error: fetchError
      } = await supabase

        .from("cat")

        .select("*")

        .eq(
          "company_code",
          company_code
        );

      if (fetchError) {

        return res.json({

          success: false,

          error:
            fetchError.message

        });

      }

      const exists =

        (existingData || []).some((row) => {

          return (

            String(
              row.cat || ""
            )
              .trim()
              .toLowerCase()

            ===

            cat.toLowerCase()

          );

        });

      if (exists) {

        return res.json({

          success: false,

          error:
            "Category already exists"

        });

      }

      // =========================
      // GENERATE NEXT CAT ID
      // =========================

      let nextId = 1;

      (existingData || []).forEach((row) => {

        const currentId =

          Number(
            row.cat_id || 0
          );

        nextId =

          Math.max(
            nextId,
            currentId + 1
          );

      });

      // =========================
      // INSERT CATEGORY
      // =========================

      const {
        error: insertError
      } = await supabase

        .from("cat")

        .insert([{

          company_code:
            company_code,

          cat_id:
            nextId,

          cat:
            cat,

          active:
            active,

          created_at:
            new Date(),

          updated_at:
            null

        }]);

      if (insertError) {

        return res.json({

          success: false,

          error:
            insertError.message

        });

      }

      return res.json({

        success: true,

        message:
          "Category saved successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// UPDATE CATEGORY
// =========================

app.post(
  "/updateCategory",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      const cat_id =

        Number(
          body.cat_id || 0
        );

      const cat =

        String(
          body.cat || ""
        ).trim();

      const active =

        String(
          body.active || ""
        )
          .trim()
          .toUpperCase() === "TRUE";

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!cat_id) {

        return res.json({

          success: false,

          error:
            "cat_id missing"

        });

      }

      if (!cat) {

        return res.json({

          success: false,

          error:
            "Category required"

        });

      }

      // =========================
      // CHECK CATEGORY EXISTS
      // =========================

      const {
        data: existingCategory,
        error: fetchError
      } = await supabase

        .from("cat")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "cat_id",
          cat_id
        )

        .single();

      if (fetchError || !existingCategory) {

        return res.json({

          success: false,

          error:
            "Category not found"

        });

      }

      // =========================
      // UPDATE CATEGORY
      // =========================

      const {
        error: updateError
      } = await supabase

        .from("cat")

        .update({

          cat:
            cat,

          active:
            active,

          updated_at:
            new Date()

        })

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "cat_id",
          cat_id
        );

      if (updateError) {

        return res.json({

          success: false,

          error:
            updateError.message

        });

      }

      return res.json({

        success: true,

        message:
          "Category updated successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


// =========================
// GET HSN
// =========================

app.get(
  "/getHSN",

  async (req, res) => {

    try {

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // FETCH HSN
      // =========================

      const {
        data,
        error
      } = await supabase

        .from("hsn")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "active",
          true
        )

        .order(
          "hsn_code",
          {
            ascending: true
          }
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      // =========================
      // FINAL RESULT
      // =========================

      const result =

        (data || []).map((row) => ({

          ...row,

          HSN_code:
            row.hsn_code || "",

          GST:
            row.gst || 0

        }));

      return res.json({

        success: true,

        data: result

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// SAVE HSN
// =========================

app.post(
  "/saveHSN",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      const HSN_code =

        String(
          body.HSN_code || ""
        ).trim();

      const GST =

        String(
          body.GST || ""
        ).trim();

      const active =

        String(
          body.active || ""
        )
          .trim()
          .toUpperCase() === "TRUE";

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!HSN_code) {

        return res.json({

          success: false,

          error:
            "HSN code required"

        });

      }

      // =========================
      // FETCH EXISTING HSN
      // =========================

      const {
        data: existingData,
        error: fetchError
      } = await supabase

        .from("hsn")

        .select("*")

        .eq(
          "company_code",
          company_code
        );

      if (fetchError) {

        return res.json({

          success: false,

          error:
            fetchError.message

        });

      }

      // =========================
      // DUPLICATE CHECK
      // =========================

      const duplicateHSN =

        (existingData || []).find((row) => {

          return (

            String(
              row.hsn_code || ""
            ).trim()

            ===

            HSN_code

          );

        });

      if (duplicateHSN) {

        const isActive =

          duplicateHSN.active === true;

        // already active

        if (isActive) {

          return res.json({

            success: false,

            error:
              "HSN already exists"

          });

        }

        // inactive found

        return res.json({

          success: false,

          restore_available: true,

          message:
            "This HSN code already exists but is currently inactive. Would you like to restore it?"

        });

      }

      // =========================
      // INSERT HSN
      // =========================

      const {
        error: insertError
      } = await supabase

        .from("hsn")

        .insert([{

          company_code:
            company_code,

          hsn_code:
            HSN_code,

          active:
            active,

          gst:
            Number(GST || 0),

          created_at:
            new Date(),

          updated_at:
            new Date()

        }]);

      if (insertError) {

        return res.json({

          success: false,

          error:
            insertError.message

        });

      }

      return res.json({

        success: true,

        message:
          "HSN saved successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);



// =========================
// UPDATE HSN
// =========================

app.post(
  "/updateHSN",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      const old_HSN_code =

        String(
          body.old_HSN_code || ""
        ).trim();

      const HSN_code =

        String(
          body.HSN_code || ""
        ).trim();

      const GST =

        String(
          body.GST || ""
        ).trim();

      const active =

        String(
          body.active || ""
        )
          .trim()
          .toUpperCase() === "TRUE";

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!old_HSN_code) {

        return res.json({

          success: false,

          error:
            "old_HSN_code missing"

        });

      }

      if (!HSN_code) {

        return res.json({

          success: false,

          error:
            "HSN code required"

        });

      }

      // =========================
      // CHECK HSN EXISTS
      // =========================

      const {
        data: existingHSN,
        error: fetchError
      } = await supabase

        .from("hsn")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "hsn_code",
          old_HSN_code
        )

        .single();

      if (fetchError || !existingHSN) {

        return res.json({

          success: false,

          error:
            "HSN not found"

        });

      }

      // =========================
      // UPDATE HSN
      // =========================

      const {
        error: updateError
      } = await supabase

        .from("hsn")

        .update({

          hsn_code:
            HSN_code,

          active:
            active,

          gst:
            Number(GST || 0),

          updated_at:
            new Date()

        })

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "hsn_code",
          old_HSN_code
        );

      if (updateError) {

        return res.json({

          success: false,

          error:
            updateError.message

        });

      }

      return res.json({

        success: true,

        message:
          "HSN updated successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// RESTORE HSN
// =========================

app.post(
  "/restoreHSN",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      const HSN_code =

        String(
          body.HSN_code || ""
        ).trim();

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!HSN_code) {

        return res.json({

          success: false,

          error:
            "HSN_code missing"

        });

      }

      // =========================
      // CHECK HSN EXISTS
      // =========================

      const {
        data: existingHSN,
        error: fetchError
      } = await supabase

        .from("hsn")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "hsn_code",
          HSN_code
        )

        .single();

      if (fetchError || !existingHSN) {

        return res.json({

          success: false,

          error:
            "HSN not found"

        });

      }

      // =========================
      // RESTORE HSN
      // =========================

      const {
        error: updateError
      } = await supabase

        .from("hsn")

        .update({

          active:
            true,

          updated_at:
            new Date()

        })

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "hsn_code",
          HSN_code
        );

      if (updateError) {

        return res.json({

          success: false,

          error:
            updateError.message

        });

      }

      return res.json({

        success: true,

        message:
          "HSN restored successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

app.get(

  "/getCategories",

  async (req, res) => {

    try {

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      const {

        data,
        error

      } = await supabase

        .from("category")

        .select(`
  id,
  company_code,
  cat,
  sub_cat,
  hsn,
  active
`)

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "active",
          true
        )

        .order(
          "cat",
          { ascending: true }
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      const formatted =

  (data || []).map(item => ({

    cat_id:
      item.id,

    company_code:
      item.company_code,

    cat:
      item.cat,

    Cat:
      item.cat,

    sub_cat:
      item.sub_cat,

    hsn:
      item.hsn,

    HSN:
      item.hsn,

    active:
      item.active

  }));

return res.json({

  success: true,

  data:
    formatted

});

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// MAP SUB CATEGORY
// =========================

app.post(
  "/mapSubCategory",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      const id =

        Number(
          body.id || 0
        );

      const cat_id =

        Number(
          body.cat_id || 0
        );

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!id) {

        return res.json({

          success: false,

          error:
            "id missing"

        });

      }

      if (!cat_id) {

        return res.json({

          success: false,

          error:
            "cat_id missing"

        });

      }

      // =========================
      // FETCH SUB CATEGORY
      // =========================

      const {
        data: subCategory,
        error: subCategoryError
      } = await supabase

        .from("sub_category_master")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "id",
          id
        )

        .single();

      if (
        subCategoryError ||
        !subCategory
      ) {

        return res.json({

          success: false,

          error:
            "Sub category not found"

        });

      }

      // =========================
      // UPDATE SUB CATEGORY
      // =========================

      const {
        error: updateError
      } = await supabase

        .from("sub_category_master")

        .update({

          cat_id:
            cat_id,

          updated_at:
            new Date()

        })

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "id",
          id
        );

      if (updateError) {

        return res.json({

          success: false,

          error:
            updateError.message

        });

      }

      // =========================
      // FETCH CATEGORY NAME
      // =========================

      const {
        data: categoryData,
        error: categoryError
      } = await supabase

        .from("cat")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "cat_id",
          cat_id
        )

        .single();

      if (
        categoryError ||
        !categoryData
      ) {

        return res.json({

          success: false,

          error:
            "Category not found"

        });

      }

      const categoryName =

        String(
          categoryData.cat || ""
        ).trim();

      const subCatName =

        String(
          subCategory.sub_cat || ""
        ).trim();

      const HSN =

        String(
          subCategory.hsn || ""
        ).trim();

      // =========================
      // CHECK CATEGORY EXISTS
      // =========================

      const {
        data: existingCategory,
        error: existingError
      } = await supabase

        .from("category")

        .select("*")

        .eq(
          "company_code",
          company_code
        );

      if (existingError) {

        return res.json({

          success: false,

          error:
            existingError.message

        });

      }

      const alreadyExists =

        (existingCategory || []).some((row) => {

          return (

            String(
              row.cat || ""
            )
              .trim()
              .toLowerCase()

            ===

            categoryName
              .toLowerCase()

            &&

            String(
              row.sub_cat || ""
            )
              .trim()
              .toLowerCase()

            ===

            subCatName
              .toLowerCase()

          );

        });

      // =========================
      // INSERT CATEGORY
      // =========================

      if (!alreadyExists) {

        const {
          error: insertError
        } = await supabase

          .from("category")

          .insert([{

            company_code:
              company_code,

            cat:
              categoryName,

            sub_cat:
              subCatName,

            hsn:
              HSN,

            active:
              true

          }]);

        if (insertError) {

          return res.json({

            success: false,

            error:
              insertError.message

          });

        }

      }

      return res.json({

        success: true,

        message:
          "Sub category mapped successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// REMOVE SUB CATEGORY MAPPING
// =========================

app.post(
  "/removeSubCategoryMapping",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      const id =

        Number(
          body.id || 0
        );

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!id) {

        return res.json({

          success: false,

          error:
            "id missing"

        });

      }

      // =========================
      // FETCH SUB CATEGORY
      // =========================

      const {
        data: subCategory,
        error: subCategoryError
      } = await supabase

        .from("sub_category_master")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "id",
          id
        )

        .single();

      if (
        subCategoryError ||
        !subCategory
      ) {

        return res.json({

          success: false,

          error:
            "Sub category not found"

        });

      }

      const subCatName =

        String(
          subCategory.sub_cat || ""
        )
          .trim()
          .toLowerCase();

      // =========================
      // REMOVE CAT ID
      // =========================

      const {
        error: updateError
      } = await supabase

        .from("sub_category_master")

        .update({

          cat_id:
            null,

          updated_at:
            new Date()

        })

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "id",
          id
        );

      if (updateError) {

        return res.json({

          success: false,

          error:
            updateError.message

        });

      }

      // =========================
      // REMOVE CATEGORY MAPPING
      // =========================

      const {
        error: deleteError
      } = await supabase

        .from("category")

        .delete()

        .eq(
          "company_code",
          company_code
        )

        .ilike(
          "sub_cat",
          subCatName
        );

      if (deleteError) {

        return res.json({

          success: false,

          error:
            deleteError.message

        });

      }

      return res.json({

        success: true,

        message:
          "Mapping removed successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// GET MAPPED SUB CATEGORIES
// =========================

app.get(
  "/getMappedSubCategories",

  async (req, res) => {

    try {

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // FETCH CATEGORIES
      // =========================

      const {
        data: catData,
        error: catError
      } = await supabase

        .from("cat")

        .select("*")

        .eq(
          "company_code",
          company_code
        );

      if (catError) {

        return res.json({

          success: false,

          error:
            catError.message

        });

      }

      // =========================
      // FETCH SUB CATEGORIES
      // =========================

      const {
        data: subData,
        error: subError
      } = await supabase

        .from("sub_category_master")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "active",
          true
        )

        .not(
          "cat_id",
          "is",
          null
        );

      if (subError) {

        return res.json({

          success: false,

          error:
            subError.message

        });

      }

      // =========================
      // CATEGORY MAP
      // =========================

      const catMap = {};

      (catData || []).forEach((row) => {

        catMap[row.cat_id] =

          row.cat || "";

      });

      // =========================
      // FINAL RESULT
      // =========================

      const result =

        (subData || []).map((row) => ({

          id:
            row.id,

          cat_id:
            row.cat_id,

          Cat:
            catMap[row.cat_id] || "",

          sub_cat:
            row.sub_cat || ""

        }));

      return res.json({

        success: true,

        data: result

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// GET MAPPING CATEGORIES
// =========================

app.get(
  "/getMappingCategories",

  async (req, res) => {

    try {

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // FETCH CATEGORIES
      // =========================

      const {
        data,
        error
      } = await supabase

        .from("cat")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "active",
          true
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      // =========================
      // FINAL RESULT
      // =========================

      const result =

        (data || []).map((row) => ({

          cat_id:
            row.cat_id,

          cat:
            row.cat || ""

        }));

      return res.json({

        success: true,

        data: result

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


// =========================
// GET UNMAPPED SUB CATEGORIES
// =========================

app.get(
  "/getUnmappedSubCategories",

  async (req, res) => {

    try {

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // FETCH SUB CATEGORIES
      // =========================

      const {
        data,
        error
      } = await supabase

        .from("sub_category_master")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "active",
          true
        )

        .is(
          "cat_id",
          null
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      // =========================
      // FINAL RESULT
      // =========================

      const result =

        (data || []).map((row) => ({

          id:
            row.id,

          sub_cat:
            row.sub_cat || ""

        }));

      return res.json({

        success: true,

        data: result

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


// =========================
// UPDATE SUB CATEGORY
// =========================

app.post(
  "/updateSubCategory",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      const sub_cat_id =

        String(
          body.sub_cat_id || ""
        ).trim();

      const sub_cat =

        String(
          body.sub_cat || ""
        ).trim();

      const active =

        String(
          body.active || ""
        )
          .trim()
          .toUpperCase() === "TRUE";

      const HSN =

        String(
          body.HSN || ""
        ).trim();

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!sub_cat_id) {

        return res.json({

          success: false,

          error:
            "sub_cat_id missing"

        });

      }

      if (!sub_cat) {

        return res.json({

          success: false,

          error:
            "sub_cat required"

        });

      }

      // =========================
      // FETCH SUB CATEGORY
      // =========================

      const {
        data: subCategory,
        error: subError
      } = await supabase

        .from("sub_category_master")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "id",
          sub_cat_id
        )

        .single();

      if (
        subError ||
        !subCategory
      ) {

        return res.json({

          success: false,

          error:
            "Sub category not found"

        });

      }

      const oldSubCatName =

        String(
          subCategory.sub_cat || ""
        )
          .trim()
          .toLowerCase();

      const newSubCatName =

        sub_cat
          .trim()
          .toLowerCase();

      // =========================
      // CHECK SUB CATEGORY USAGE
      // =========================

      const {
        data: categoryData,
        error: categoryError
      } = await supabase

        .from("category")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "active",
          true
        );

      if (categoryError) {

        return res.json({

          success: false,

          error:
            categoryError.message

        });

      }

      const isUsed =

        (categoryData || []).some((row) => {

          return (

            String(
              row.sub_cat || ""
            )
              .trim()
              .toLowerCase()

            ===

            oldSubCatName

          );

        });

      if (

        isUsed

        &&

        oldSubCatName !==
          newSubCatName

      ) {

        return res.json({

          success: false,

          error:
            "Mapped sub category name cannot be changed"

        });

      }

      // =========================
      // UPDATE SUB CATEGORY
      // =========================

      const {
        error: updateError
      } = await supabase

        .from("sub_category_master")

        .update({

          sub_cat:
            sub_cat,

          active:
            active,

          hsn:
            HSN,

          updated_at:
            new Date()

        })

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "id",
          sub_cat_id
        );

      if (updateError) {

        return res.json({

          success: false,

          error:
            updateError.message

        });

      }

      // =========================
      // UPDATE CATEGORY HSN
      // =========================

      const {
        error: categoryUpdateError
      } = await supabase

        .from("category")

        .update({

          hsn:
            HSN

        })

        .eq(
          "company_code",
          company_code
        )

        .ilike(
          "sub_cat",
          oldSubCatName
        );

      if (categoryUpdateError) {

        return res.json({

          success: false,

          error:
            categoryUpdateError.message

        });

      }

      return res.json({

        success: true,

        message:
          "Sub category updated successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


// =========================
// SAVE SUB CATEGORY
// =========================

app.post(
  "/saveSubCategory",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      const sub_cat =

        String(
          body.sub_cat || ""
        ).trim();

      const active =

        String(
          body.active || ""
        )
          .trim()
          .toUpperCase() === "TRUE";

      const HSN =

        String(
          body.HSN || ""
        ).trim();

      const cat_id =

        body.cat_id || null;

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!sub_cat) {

        return res.json({

          success: false,

          error:
            "Sub category required"

        });

      }

      // =========================
      // CHECK DUPLICATE
      // =========================

      const {
        data: existingData,
        error: fetchError
      } = await supabase

        .from("sub_category_master")

        .select("*")

        .eq(
          "company_code",
          company_code
        );

      if (fetchError) {

        return res.json({

          success: false,

          error:
            fetchError.message

        });

      }

      const exists =

        (existingData || []).some((row) => {

          return (

            String(
              row.sub_cat || ""
            )
              .trim()
              .toLowerCase()

            ===

            sub_cat
              .toLowerCase()

          );

        });

      if (exists) {

        return res.json({

          success: false,

          error:
            "Sub category already exists"

        });

      }

      
      // =========================
      // INSERT SUB CATEGORY
      // =========================

      const {
        error: insertError
      } = await supabase

        .from("sub_category_master")

        .insert([{

         

          company_code:
            company_code,

          cat_id:
            cat_id,

          sub_cat:
            sub_cat,

          active:
            active,

          created_at:
            new Date(),

          updated_at:
            null,

          hsn:
            HSN

        }]);

      if (insertError) {

        return res.json({

          success: false,

          error:
            insertError.message

        });

      }

      return res.json({

        success: true,

        message:
          "Sub category saved successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


// =========================
// GENERATE VARIANT ROWS
// =========================

async function generateVariantRows(
  materialId,
  variantCode,
  body,
  packaging
) {

  const rows = [];

  // =========================
  // GET LAST ROW ID
  // =========================

  const {
    data: lastVariant
  } = await supabase

    .from("variant")

    .select("row_id")

    .order(
      "row_id",
      {
        ascending: false
      }
    )

    .limit(1)

    .single();

  let rowId =

    lastVariant?.row_id

      ? Number(
          lastVariant.row_id
        ) + 1

      : 1;

  // =========================
  // LOOP PACKAGING
  // =========================

  packaging.forEach((p) => {

    // =========================
    // FINAL MULTIPLIER
    // =========================

    let finalMultiplier = 1;

    const currentIndex =

      packaging.findIndex(
        row =>
          row.variant === p.variant
      );

    for (
      let i = currentIndex;
      i < packaging.length;
      i++
    ) {

      finalMultiplier =

        finalMultiplier *

        Number(
          packaging[i].qty || 1
        );

    }

    // =========================
    // DESCRIPTION
    // =========================

    const cleanPackaging =

  String(

    p.item_packaging

    || body.variant_label

    || ""

  ).trim();

    const parentItemName =

      String(
        body.item_name || ""
      ).trim();

    const description =

      String(
        body.measurement || ""
      )
        .toLowerCase() === "weight"

        ? cleanPackaging
            .toLowerCase()
            .startsWith(
              parentItemName
                .trim()
                .toLowerCase()
            )

            ? cleanPackaging

            : `${parentItemName}-${cleanPackaging}`

        : body.item_name || "";

    // =========================
    // PUSH ROW
    // =========================

    rows.push({

      row_id:
        rowId++,

      material_id:
        materialId,

      variant_code:
        variantCode,

      item_name:
        parentItemName,

      measurement:
        body.measurement || "",

      item_packaging:
  cleanPackaging,

      description:
        description,

      variant:
        p.variant || "",

      sub_variant:
        p.sub_variant || "",

      qty:
        Number(p.qty || 1),

      final_multiplier:
        finalMultiplier,

      base_unit:
        body.base_unit || "",

      purchase_rate:
        Number(body.purchase_rate || 0),

      mrp:
        Number(p.mrp || 0),

      disc_percent:
  Number(
    p.Disc_Percent
    ?? p.disc_percent
    ?? 0
  ),

disc_amt:
  Number(
    p.Disc_Amt
    ?? p.disc_amt
    ?? 0
  ),

      selling_rate:
        Number(p.selling_rate || 0),

      opening_stock:
        Number(body.opening_stock || 0),

      purchase: 0,

      sale: 0,

      retrun: 0,

      closing:
        Number(body.opening_stock || 0),

      min_stock:
        Number(body.min_stock || 0),

      barcode: "",

      sku: "",

      item_type:
        body.item_type || "Goods",

      batch_required:
        false,

      expiry_required:
        false,

      serial_required:
        false,

      is_active:
        true,

      remarks: "",

      created_at:
        new Date(),

      updated_at:
        new Date(),

      company_code:
        body.company_code || "",

      branch_id:
        body.branch_id || ""

    });

  });

  return rows;

}



// =========================
// SAVE MATERIAL
// =========================

app.post(
  "/saveMaterial",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      // =========================
      // VALIDATION
      // =========================

      if (!body.company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // DUPLICATE CHECK
      // =========================

      const {
        data: existingItems,
        error: fetchError
      } = await supabase

        .from("mm")

        .select("*")

        .eq(
          "company_code",
          body.company_code
        )

        .eq(
          "branch_id",
          body.branch_id || ""
        );

      if (fetchError) {

        return res.json({

          success: false,

          error:
            fetchError.message

        });

      }

      const duplicateItem =

        (existingItems || []).find((row) => {

          return (

            String(
              row.item_name || ""
            )
              .trim()
              .toUpperCase()

            ===

            String(
              body.item_name || ""
            )
              .trim()
              .toUpperCase()

          );

        });

      if (duplicateItem) {

        return res.json({

          success: false,

          error:
            "Item already exists for this branch"

        });

      }

      // =========================
      // NEXT MATERIAL ID
      // =========================

      const {
        data: lastMaterial
      } = await supabase

        .from("mm")

        .select("material_id")

        .order(
          "material_id",
          {
            ascending: false
          }
        )

        .limit(1)

        .single();

      const materialId =

        lastMaterial?.material_id

          ? Number(
              lastMaterial.material_id
            ) + 1

          : 1;

      // =========================
      // ITEM NUMBER
      // =========================

      let itemNumber = 1;

      const companyRows =

        (existingItems || []).filter(
          row =>

            String(
              row.company_code || ""
            ).trim()

            ===

            String(
              body.company_code || ""
            ).trim()
        );

      if (companyRows.length > 0) {

        let maxItem = 0;

        companyRows.forEach((row) => {

          const code =

            String(
              row.item_code || ""
            );

          const num =

            parseInt(
              code.replace("ITM", "")
            );

          if (!isNaN(num)) {

            maxItem =

              Math.max(
                maxItem,
                num
              );

          }

        });

        itemNumber = maxItem + 1;

      }

      // =========================
      // AUTO ITEM CODE
      // =========================

      const itemCode =

        "ITM" +

        String(itemNumber)
          .padStart(3, "0");

      // =========================
      // BLOCK VARIANT MODE
      // =========================

      if (
        body.mode ===
        "add_variant"
      ) {

        return res.json({

          success: false,

          error:
            "Variant mode cannot create material"

        });

      }

      // =========================
      // SAVE MM
      // =========================

      const insertObj = {

        material_id:
          materialId,

        item_code:
          itemCode,

        mat_type:
          body.mat_type || "",

        item_name:
          body.item_name || "",

        short_name:
          body.short_name || "",

        measurement:
          body.measurement || "",

        category:
          body.category || "",

        sub_category:
          body.sub_category || "",

        brand:
          body.brand || "",

        hsn_code:
          String(
            body.hsn_code || ""
          ),

        gst_percent:
          Number(
            body.gst_percent || 0
          ),

        cess_percent:
          Number(
            body.cess_percent || 0
          ),

        base_unit:
          body.base_unit || "",

        purchase_rate:
          Number(
            body.purchase_rate || 0
          ),

        mrp:
          Number(
            body.mrp || 0
          ),

        disc_percent:
  Number(
    body.Disc_Percent
    ?? body.disc_percent
    ?? body.discount_percent
    ?? 0
  ),

disc_amt:
  Number(
    body.Disc_Amt
    ?? body.disc_amt
    ?? body.discount_amount
    ?? 0
  ),

        sale_rate:
          Number(
            body.sale_rate || 0
          ),

        opening_stock:
          Number(
            body.opening_stock || 0
          ),

        purchase: 0,

        sale: 0,

        retrun: 0,

        closing:
          Number(
            body.opening_stock || 0
          ),

        min_stock:
          Number(
            body.min_stock || 0
          ),

        barcode:
          body.barcode || "",

        sku:
          body.sku || "",

        item_type:
          body.item_type || "Goods",

        batch_required:
          body.batch_required || false,

        expiry_required:
          body.expiry_required || false,

        serial_required:
          body.serial_required || false,

        is_active:
          true,

        remarks:
          body.remarks || "",

        created_at:
          new Date(),

        updated_at:
          new Date(),

        company_code:
          body.company_code || "",

        branch_id:
          body.branch_id || "",

        image_url:
          body.image_url || ""

      };

      const {
        error: insertError
      } = await supabase

        .from("mm")

        .insert([

          insertObj

        ]);

      if (insertError) {

        return res.json({

          success: false,

          error:
            insertError.message

        });

      }

      // =========================
      // GENERATE VARIANTS
      // =========================

      const packaging =
        body.packaging || [];

      const variantRows =

        await generateVariantRows(

          materialId,

          `${materialId}A`,

          body,

          packaging

        );

      // =========================
      // SAVE VARIANTS
      // =========================

      if (variantRows.length > 0) {

        const {
          error: variantError
        } = await supabase

          .from("variant")

          .insert(
            variantRows
          );

        if (variantError) {

          return res.json({

            success: false,

            error:
              variantError.message

          });

        }

      }

      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        material_id:
          materialId,

        item_code:
          itemCode,

        message:
          "Material saved successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// UPDATE SINGLE MATERIAL
// =========================

app.post(
  "/updateSingleMaterial",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      // =========================
      // VALIDATION
      // =========================

      if (!body.material_id) {

        return res.json({

          success: false,

          error:
            "material_id missing"

        });

      }

      if (!body.company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // FIND MATERIAL
      // =========================

      const {
        data: material,
        error: materialError
      } = await supabase

        .from("mm")

        .select("*")

        .eq(
          "material_id",
          Number(body.material_id)
        )

        .eq(
          "company_code",
          String(
            body.company_code || ""
          ).trim()
        )

        .single();

      if (materialError || !material) {

        return res.json({

          success: false,

          error:
            "Material not found"

        });

      }

      // =========================
      // CHECK VARIANTS
      // =========================

      const {
        data: variants,
        error: variantError
      } = await supabase

        .from("variant")

        .select("row_id")

        .eq(
          "material_id",
          Number(body.material_id)
        )

        .eq(
          "company_code",
          String(
            body.company_code || ""
          ).trim()
        );

      if (variantError) {

        return res.json({

          success: false,

          error:
            variantError.message

        });

      }

      const hasVariants =

        (variants || []).length > 0;

      // =========================
      // PREVENT TYPE CHANGE
      // =========================

      if (hasVariants) {

        const currentType =

          String(
            material.mat_type || ""
          ).trim();

        if (

          currentType !==

          String(
            body.mat_type || ""
          ).trim()

        ) {

          return res.json({

            success: false,

            error:
              "Material type cannot change because variants exist"

          });

        }

      }

      // =========================
      // UPDATE MM
      // =========================

      const updateObj = {

        mat_type:
          body.mat_type || "",

        item_name:
          body.item_name || "",

        measurement:
          body.measurement || "",

        category:
          body.category || "",

        sub_category:
          body.sub_category || "",

        brand:
          body.brand || "",

        hsn_code:
          String(
            body.hsn_code || ""
          ),

        gst_percent:
          Number(
            body.gst_percent || 0
          ),

        base_unit:
          body.base_unit || "",

        mrp:
          Number(
            body.mrp || 0
          ),

        disc_percent:
          Number(

            body.Disc_Percent

            ?? body.disc_percent

            ?? body.discount_percent

            ?? 0

          ),

        disc_amt:
          Number(

            body.Disc_Amt

            ?? body.disc_amt

            ?? body.discount_amount

            ?? 0

          ),

        sale_rate:
          Number(
            body.sale_rate || 0
          ),

        updated_at:
          new Date(),

      };

      const {
        error: updateError
      } = await supabase

        .from("mm")

        .update(updateObj)

        .eq(
          "material_id",
          Number(body.material_id)
        )

        .eq(
          "company_code",
          String(
            body.company_code || ""
          ).trim()
        );

      if (updateError) {

        return res.json({

          success: false,

          error:
            updateError.message

        });

      }

      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        message:
          "Material updated successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// ADD MATERIAL VARIANT
// =========================

app.post(
  "/addMaterialVariant",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      // =========================
      // VALIDATION
      // =========================

      if (!body.material_id) {

        return res.json({

          success: false,

          error:
            "material_id missing"

        });

      }

      if (!body.company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // FIND MATERIAL
      // =========================

      const {
        data: material,
        error: materialError
      } = await supabase

        .from("mm")

        .select("*")

        .eq(
          "material_id",
          Number(body.material_id)
        )

        .eq(
          "company_code",
          String(
            body.company_code || ""
          ).trim()
        )

        .single();

      if (materialError || !material) {

        return res.json({

          success: false,

          error:
            "Material not found"

        });

      }

      // =========================
      // EXISTING ITEM CODE
      // =========================

      const materialId =
        Number(body.material_id);

      const itemCode =
        String(
          material.item_code || ""
        );

      const parentItemName =
        String(
          material.item_name || ""
        ).trim();

      // =========================
      // DUPLICATE VARIANT CHECK
      // =========================

      const newDescription =

        `${parentItemName}-${body.variant_label}`
          .trim()
          .toUpperCase();

      const {
        data: existingVariants,
        error: variantError
      } = await supabase

        .from("variant")

        .select("*")

        .eq(
          "material_id",
          materialId
        )

        .eq(
          "company_code",
          String(
            body.company_code || ""
          ).trim()
        );

      if (variantError) {

        return res.json({

          success: false,

          error:
            variantError.message

        });

      }

      const duplicateVariant =

        (existingVariants || []).find(
          (row) =>

            String(
              row.description || ""
            )
              .trim()
              .toUpperCase()

            ===

            newDescription
        );

      if (duplicateVariant) {

        return res.json({

          success: false,

          error:
            "Variant already exists"

        });

      }

      // =========================
      // PREPARE PACKAGING
      // =========================

      const packaging =
        body.packaging || [];

      body.item_name =
        parentItemName;

      body.packaging =

        packaging.map((row) => ({

          ...row,

          item_packaging:
            body.variant_label || ""

        }));

      // =========================
      // GENERATE VARIANT CODE
      // =========================

      const existingVariantCodes =

        (existingVariants || []).map(
          (row) =>

            String(
              row.variant_code || ""
            )
        );

      let nextLetter = "A";

      while (

        existingVariantCodes.includes(
          `${materialId}${nextLetter}`
        )

      ) {

        nextLetter =

          String.fromCharCode(

            nextLetter.charCodeAt(0) + 1

          );

      }

      const variantCode =

        `${materialId}${nextLetter}`;

      // =========================
      // GENERATE VARIANT ROWS
      // =========================

      const variantRows =

        await generateVariantRows(

          materialId,

          variantCode,

          body,

          body.packaging

        );

      // =========================
      // INSERT VARIANTS
      // =========================

      if (variantRows.length > 0) {

        const {
          error: insertError
        } = await supabase

          .from("variant")

          .insert(variantRows);

        if (insertError) {

          return res.json({

            success: false,

            error:
              insertError.message

          });

        }

      }

      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        material_id:
          materialId,

        item_code:
          itemCode,

        message:
          "Variant added successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


// =========================
// UPDATE MATERIAL HIERARCHY
// =========================

app.post(
  "/updateMaterialHierarchy",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      // =========================
      // VALIDATION
      // =========================

      if (!body.company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!body.material_id) {

        return res.json({

          success: false,

          error:
            "material_id missing"

        });

      }

      // =========================
      // FIND MATERIAL
      // =========================

      const {
        data: material,
        error: materialError
      } = await supabase

        .from("mm")

        .select("*")

        .eq(
          "material_id",
          Number(body.material_id)
        )

        .eq(
          "company_code",
          String(
            body.company_code || ""
          ).trim()
        )

        .single();

      if (materialError || !material) {

        return res.json({

          success: false,

          error:
            "Material not found"

        });

      }

      // =========================
      // GET ITEM CODE
      // =========================

      const materialId =
        Number(body.material_id);

      const itemCode =
        String(
          material.item_code || ""
        );

      const variantCode =

        String(
          body.variant_code || ""
        ).trim();

      // =========================
      // UPDATE MM
      // =========================

      const updateObj = {

        mat_type:
          body.mat_type || "",

        base_unit:
          body.base_unit || "",

        mrp:
          Number(
            body.mrp || 0
          ),

        disc_percent:
          Number(

            body.Disc_Percent

            ?? body.disc_percent

            ?? body.discount_percent

            ?? 0

          ),

        disc_amt:
          Number(

            body.Disc_Amt

            ?? body.disc_amt

            ?? body.discount_amount

            ?? 0

          ),

        sale_rate:
          Number(
            body.sale_rate || 0
          ),

        updated_at:
          new Date(),

      };

      const {
        error: updateError
      } = await supabase

        .from("mm")

        .update(updateObj)

        .eq(
          "material_id",
          materialId
        )

        .eq(
          "company_code",
          String(
            body.company_code || ""
          ).trim()
        );

      if (updateError) {

        return res.json({

          success: false,

          error:
            updateError.message

        });

      }

      // =========================
      // DELETE OLD VARIANTS
      // =========================

      const {
        error: deleteError
      } = await supabase

        .from("variant")

        .delete()

        .eq(
          "variant_code",
          variantCode
        )

        .eq(
          "company_code",
          String(
            body.company_code || ""
          ).trim()
        )

        .eq(
          "material_id",
          materialId
        );

      if (deleteError) {

        return res.json({

          success: false,

          error:
            deleteError.message

        });

      }

      // =========================
      // GENERATE NEW VARIANTS
      // =========================

      const packaging =
        body.packaging || [];

      const variantRows =

        await generateVariantRows(

          materialId,

          variantCode,

          body,

          packaging

        );

      // =========================
      // INSERT NEW VARIANTS
      // =========================

      if (variantRows.length > 0) {

        const {
          error: insertError
        } = await supabase

          .from("variant")

          .insert(variantRows);

        if (insertError) {

          return res.json({

            success: false,

            error:
              insertError.message

          });

        }

      }

      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        material_id:
          materialId,

        item_code:
          itemCode,

        message:
          "Hierarchy updated successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// GET MATERIALS
// =========================

app.get(
  "/getMaterials",

  async (req, res) => {

    try {

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // FETCH MATERIALS
      // =========================

      const {
        data,
        error
      } = await supabase

        .from("mm")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .order(
          "material_id",
          {
            ascending: true
          }
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      // =========================
      // FORMAT RESULT
      // =========================

      const result =

        (data || []).map((row) => {

          return {

            ...row,

                // =========================
      // DISCOUNT FIELDS
      // =========================

      Disc_Percent:
        Number(
          row.disc_percent || 0
        ),

      Disc_Amt:
        Number(
          row.disc_amt || 0
        ),

    
            item_code:
              String(
                row.item_code || ""
              ),

            hsn_code:
              String(
                row.hsn_code || ""
              ),

            barcode:
              String(
                row.barcode || ""
              ),

            sku:
              String(
                row.sku || ""
              ),

            company_code:
              String(
                row.company_code || ""
              ),

            branch_id:
              String(
                row.branch_id || ""
              ),



          };

        });

      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        data: result

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// GET MATERIAL VARIANTS
// =========================

app.get(
  "/getMaterialVariants",

  async (req, res) => {

    try {

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

      const material_id =

        String(
          req.query.material_id || ""
        ).trim();

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!material_id) {

        return res.json({

          success: false,

          error:
            "material_id missing"

        });

      }

      // =========================
      // FETCH VARIANTS
      // =========================

      const {
        data,
        error
      } = await supabase

        .from("variant")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "material_id",
          Number(material_id)
        )

        .order(
          "row_id",
          {
            ascending: true
          }
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      // =========================
      // FORMAT RESULT
      // =========================

      const result =

        (data || []).map((row) => {

          return {

            // =========================
            // RAW DATA
            // =========================

            ...row,

                // =========================
      // DISCOUNT FIELDS
      // =========================

      Disc_Percent:
        Number(
          row.disc_percent || 0
        ),

      Disc_Amt:
        Number(
          row.disc_amt || 0
        ),

            // =========================
            // SAFE STRING FIELDS
            // =========================

            material_id:
              String(
                row.material_id || ""
              ),

            variant_code:
              String(
                row.variant_code || ""
              ),

            barcode:
              String(
                row.barcode || ""
              ),

            sku:
              String(
                row.sku || ""
              ),

            company_code:
              String(
                row.company_code || ""
              ),

            branch_id:
              String(
                row.branch_id || ""
              ),

            item_name:
              String(
                row.item_name || ""
              ),

            description:
              String(
                row.description || ""
              ),

            variant:
              String(
                row.variant || ""
              ),

            sub_variant:
              String(
                row.sub_variant || ""
              ),

            item_packaging:
              String(
                row.item_packaging || ""
              ),

            // =========================
            // SAFE NUMBER FIELDS
            // =========================

            Qty:
              Number(
                row.qty || 0
              ),

            final_multiplier:
              Number(
                row.final_multiplier || 0
              ),

            mrp:
              Number(
                row.mrp || 0
              ),

            purchase_rate:
              Number(
                row.purchase_rate || 0
              ),

            selling_rate:
              Number(
                row.selling_rate || 0
              ),

            Disc_Percent:
              Number(
                row.disc_percent || 0
              ),

            Disc_Amt:
              Number(
                row.disc_amt || 0
              ),

            opening_stock:
              Number(
                row.opening_stock || 0
              ),

            purchase:
              Number(
                row.purchase || 0
              ),

            sale:
              Number(
                row.sale || 0
              ),

            retrun:
              Number(
                row.retrun || 0
              ),

            closing:
              Number(
                row.closing || 0
              ),

            min_stock:
              Number(
                row.min_stock || 0
              ),

            // =========================
            // SAFE BOOLEAN FIELDS
            // =========================

            batch_required:
              Boolean(
                row.batch_required
              ),

            expiry_required:
              Boolean(
                row.expiry_required
              ),

            serial_required:
              Boolean(
                row.serial_required
              ),

            is_active:
              Boolean(
                row.is_active
              ),

          };

        });

      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        data: result

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

app.post(

  "/holdMaterial",

  async (req, res) => {

    try {

      const {

        material_id,
        company_code,
        hold

      } = req.body || {};

      // =========================
      // VALIDATION
      // =========================

      if (!material_id) {

        return res.json({

          success: false,

          error:
            "material_id missing"

        });

      }

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // UPDATE HOLD
      // =========================

      const {
        error
      } = await supabase

        .from("mm")

        .update({

          hold:
            hold === "yes"
              ? "yes"
              : null,

          updated_at:
            new Date()

        })

        .eq(
          "material_id",
          Number(material_id)
        )

        .eq(
          "company_code",
          String(company_code)
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        message:

          hold === "yes"

            ? "Material placed on hold"

            : "Material restored"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

app.post(

  "/deactivateMaterial",

  async (req, res) => {

    try {

      const {

        material_id,
        company_code,
        is_active

      } = req.body || {};

      // =========================
      // VALIDATION
      // =========================

      if (!material_id) {

        return res.json({

          success: false,

          error:
            "material_id missing"

        });

      }

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // UPDATE STATUS
      // =========================

      const {
        error
      } = await supabase

        .from("mm")

        .update({

          is_active:
            is_active,

          updated_at:
            new Date()

        })

        .eq(
          "material_id",
          Number(material_id)
        )

        .eq(
          "company_code",
          String(company_code)
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        message:

          is_active

            ? "Material activated"

            : "Material permanently deactivated"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);
// =========================
// CANCEL MATERIAL
// =========================

console.log(
  "cancelMaterial route registered"
);

app.post(

  "/cancelMaterial",

  async (req, res) => {

    console.log(
      "========== CANCEL MATERIAL API HIT =========="
    );

    try {

      console.log(
        "REQ BODY =>",
        req.body
      );

      const {

        material_id,
        company_code,
        hold,
        is_active

      } = req.body || {};

      console.log(
        "material_id =>",
        material_id
      );

      console.log(
        "company_code =>",
        company_code
      );

      console.log(
        "hold =>",
        hold
      );

      console.log(
        "is_active =>",
        is_active
      );

      // =========================
      // VALIDATION
      // =========================

      if (!material_id) {

        console.log(
          "VALIDATION FAILED : material_id missing"
        );

        return res.json({

          success: false,

          error:
            "material_id missing"

        });

      }

      if (!company_code) {

        console.log(
          "VALIDATION FAILED : company_code missing"
        );

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // UPDATE STATUS
      // =========================

      console.log(
        "UPDATING MATERIAL STATUS..."
      );

      const {
        error
      } = await supabase

        .from("mm")

        .update({

          hold:
            hold || null,

          is_active:
            Boolean(is_active),

          updated_at:
            new Date()

        })

        .eq(
          "material_id",
          Number(material_id)
        )

        .eq(
          "company_code",
          String(company_code)
        );

      console.log(
        "SUPABASE UPDATE DONE"
      );

      if (error) {

        console.log(
          "SUPABASE ERROR =>",
          error
        );

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      console.log(
        "MATERIAL STATUS UPDATED SUCCESSFULLY"
      );

      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        message:
          "Material status updated"

      });

    }

    catch (err) {

      console.log(
        "CANCEL MATERIAL API ERROR =>",
        err
      );

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);



// =========================
// HOLD VARIANT
// =========================

app.post(

  "/holdVariant",

  async (req, res) => {

    try {

      const {

        material_id,
        company_code,
        variant_code,
        hold

      } = req.body || {};

      if (!material_id) {

        return res.json({

          success: false,

          error:
            "material_id missing"

        });

      }

      if (!variant_code) {

        return res.json({

          success: false,

          error:
            "variant_code missing"

        });

      }

      const {
        error
      } = await supabase

        .from("variant")

        .update({

          hold:
            hold || null,

          updated_at:
            new Date()

        })

        .eq(
          "material_id",
          Number(material_id)
        )

        .eq(
          "variant_code",
          String(variant_code)
        )

        .eq(
          "company_code",
          String(company_code)
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      return res.json({

        success: true,

        message:
          "Variant hold updated"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);



// =========================
// CANCEL VARIANT
// =========================

app.post(

  "/cancelVariant",

  async (req, res) => {

    try {

      const {

        material_id,
        company_code,
        variant_code,
        hold,
        is_active

      } = req.body || {};

      console.log(
        "========== CANCEL VARIANT API HIT =========="
      );

      console.log(
        "REQ BODY =>",
        req.body
      );

      const {
        error
      } = await supabase

        .from("variant")

        .update({

          hold:
            hold || null,

          is_active:
            Boolean(is_active),

          updated_at:
            new Date()

        })

        .eq(
          "material_id",
          Number(material_id)
        )

        .eq(
          "variant_code",
          String(variant_code)
        )

        .eq(
          "company_code",
          String(company_code)
        );

      if (error) {

        console.log(
          "SUPABASE ERROR =>",
          error
        );

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      console.log(
        "VARIANT STATUS UPDATED SUCCESSFULLY"
      );

      return res.json({

        success: true,

        message:
          "Variant status updated"

      });

    }

    catch (err) {

      console.log(
        "CANCEL VARIANT ERROR =>",
        err
      );

      return res.json({

        success: false,

        error:
          err.message

        });

    }

  }
);

// =========================
// GET PACKAGING UNITS
// =========================

app.get(
  "/getPackagingUnits",

  async (req, res) => {

    try {

      const GLOBAL_PACKING_UNITS = [

  "Box",
  "Pc",
  "Pkt",
  "Carton",
  "Tin",
  "Set"

];

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // FETCH PACKAGING UNITS
      // =========================

      console.log(
        "GET PACKAGING UNITS COMPANY:",
        company_code
      );

      const {
        data,
        error
      } = await supabase

        .from("pkg_unit")

        .select(`
        id,
        company_code,
        Pk_un:pk_un,
        active
      `)

        .eq(
          "company_code",
          company_code
        )

        .order(
          "pk_un",
          {
            ascending: true
          }
        );

      // =========================
      // ERROR
      // =========================

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      const existingUnits =

  (data || []).map(

    (item) =>

      String(item.Pk_un || "")
        .trim()
        .toLowerCase()

  );

      // =========================
      // SUCCESS
      // =========================

      const globalUnits =

  GLOBAL_PACKING_UNITS

    .filter(

      (unit) =>

        !existingUnits.includes(

          String(unit)
            .trim()
            .toLowerCase()

        )

    )

    .map((unit) => ({

    id:
  `global_${unit}`,

      company_code:
        company_code,

      Pk_un:
        unit,

      active:
        true,

      is_default:
        true

    }));

      return res.json({

        success: true,

        data: [

  ...globalUnits,

  ...(data || []).map(
    (item) => ({

      ...item,

      is_default:
        false

    })
  )

]

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// SAVE PACKAGING UNIT
// =========================

app.post(
  "/savePackagingUnit",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      const pk_un =

        String(
          body.pk_un || ""
        ).trim();

    const active =

  String(
    body.active || "TRUE"
  )
    .trim()
    .toUpperCase() === "TRUE";

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!pk_un) {

        return res.json({

          success: false,

          error:
            "Packing unit required"

        });

      }

      // =========================
      // FETCH EXISTING DATA
      // =========================

      const {
        data: existingData,
        error: fetchError
      } = await supabase

        .from("pkg_unit")

        .select("*")

        .eq(
          "company_code",
          company_code
        );

      if (fetchError) {

        return res.json({

          success: false,

          error:
            fetchError.message

        });

      }

      // =========================
      // DUPLICATE CHECK
      // =========================

      const exists =

        (existingData || []).some((row) => {

          return (

            String(
              row.pk_un || ""
            )
              .trim()
              .toLowerCase()

            ===

            pk_un.toLowerCase()

          );

        });

      if (exists) {

        return res.json({

          success: false,

          error:
            "Packing unit already exists"

        });

      }

      // =========================
      // INSERT
      // =========================

      const {
        error: insertError
      } = await supabase

        .from("pkg_unit")

        .insert([{

  company_code:
    company_code,

  pk_un:
    pk_un,

  active:
    active,

  created_at:
    new Date(),

  updated_at:
    null

}]);

      if (insertError) {

        return res.json({

          success: false,

          error:
            insertError.message

        });

      }

      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        message:
          "Packing unit saved successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// UPDATE PACKAGING UNIT
// =========================

app.post(

  "/updatePackagingUnit",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      console.log(
        "UPDATE PACKAGING UNIT BODY =>",
        body
      );

      const id =

        String(
          body.id || ""
        ).trim();

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      const pk_un =

        String(
          body.pk_un || ""
        ).trim();

      const active =

        String(
          body.active || "TRUE"
        )
          .trim()
          .toUpperCase() === "TRUE";

      // =========================
      // VALIDATION
      // =========================

      if (!id) {

        return res.json({

          success: false,

          error:
            "id missing"

        });

      }

      // BIGINT VALIDATION

      if (
        isNaN(Number(id))
      ) {

        return res.json({

          success: false,

          error:
            "Invalid id"

        });

      }

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!pk_un) {

        return res.json({

          success: false,

          error:
            "Packing unit required"

        });

      }

      // =========================
      // FETCH EXISTING DATA
      // =========================

      const {
        data: existingData,
        error: fetchError
      } = await supabase

        .from("pkg_unit")

        .select("*")

        .eq(
          "company_code",
          company_code
        );

      if (fetchError) {

        return res.json({

          success: false,

          error:
            fetchError.message

        });

      }

      // =========================
      // DUPLICATE CHECK
      // =========================

      const exists =

        (existingData || []).some((row) => {

          return (

            String(row.id) !==
              String(id)

            &&

            String(
              row.pk_un || ""
            )
              .trim()
              .toLowerCase()

            ===

            pk_un
              .trim()
              .toLowerCase()

          );

        });

      if (exists) {

        return res.json({

          success: false,

          error:
            "Packing unit already exists"

        });

      }

      // =========================
      // UPDATE
      // =========================

      console.log(
        "UPDATE WHERE =>",
        {
          id,
          company_code
        }
      );

      const {
        data: updatedData,
        error: updateError
      } = await supabase

        .from("pkg_unit")

        .update({

          pk_un:
            pk_un,

          active:
            active,

          updated_at:
            new Date()

        })

        .eq(
          "id",
          Number(id)
        )

        .eq(
          "company_code",
          company_code
        )

        .select();

      if (updateError) {

        return res.json({

          success: false,

          error:
            updateError.message

        });

      }

      // =========================
      // NOT FOUND CHECK
      // =========================

      if (
        !updatedData ||
        updatedData.length === 0
      ) {

        return res.json({

          success: false,

          error:
            "Record not found"

        });

      }

      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        message:
          "Packing unit updated successfully",

        data:
          updatedData

      });

    }

    catch (err) {

      console.log(
        "UPDATE PACKAGING UNIT ERROR =>",
        err
      );

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }

);

// =========================
// GET MEASUREMENT UNITS
// =========================

app.get(
  "/getMeasurementUnits",

  async (req, res) => {

    try {

      const GLOBAL_MEASUREMENT_UNITS = [

        "Gm",
        "Kg",
        "ML",
        "Ltr",
        "Qtl",
        "Ton",
        "Lb"

      ];

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      // =========================
      // FETCH MEASUREMENT UNITS
      // =========================

      console.log(
        "GET MEASUREMENT UNITS COMPANY:",
        company_code
      );

      const {
        data,
        error
      } = await supabase

        .from("measurement_unit")

        .select(`
          id,
          company_code,
          weight_unit,
          active
        `)

        .eq(
          "company_code",
          company_code
        )

        .order(
          "weight_unit",
          {
            ascending: true
          }
        );

      // =========================
      // ERROR
      // =========================

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      const existingUnits =

        (data || []).map(

          (item) =>

            String(
              item.weight_unit || ""
            )
              .trim()
              .toLowerCase()

        );

      // =========================
      // GLOBAL DEFAULT UNITS
      // =========================

      const globalUnits =

        GLOBAL_MEASUREMENT_UNITS

          .filter(

            (unit) =>

              !existingUnits.includes(

                String(unit)
                  .trim()
                  .toLowerCase()

              )

          )

          .map((unit) => ({

            id:
              `global_${unit}`,

            company_code:
              company_code,

            weight_unit:
              unit,

            active:
              true,

            is_default:
              true

          }));

      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        data: [

          ...globalUnits,

          ...(data || []).map(
            (item) => ({

              ...item,

              is_default:
                false

            })
          )

        ]

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// SAVE MEASUREMENT UNIT
// =========================

app.post(
  "/saveMeasurementUnit",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      const weight_unit =

        String(
          body.weight_unit || ""
        ).trim();

      const active =

        String(
          body.active || ""
        )
          .trim()
          .toUpperCase() === "TRUE";

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!weight_unit) {

        return res.json({

          success: false,

          error:
            "Measurement unit required"

        });

      }

      // =========================
      // DUPLICATE CHECK
      // =========================

      const {
        data: existingData,
        error: fetchError
      } = await supabase

        .from("measurement_unit")

        .select("*")

        .eq(
          "company_code",
          company_code
        );

      if (fetchError) {

        return res.json({

          success: false,

          error:
            fetchError.message

        });

      }

      const exists =

        (existingData || []).some((row) => {

          return (

            String(
              row.weight_unit || ""
            )
              .trim()
              .toLowerCase()

            ===

            weight_unit.toLowerCase()

          );

        });

      if (exists) {

        return res.json({

          success: false,

          error:
            "Measurement unit already exists"

        });

      }

      // =========================
      // INSERT
      // =========================

      const {
        error: insertError
      } = await supabase

        .from("measurement_unit")

        .insert([{

          company_code:
            company_code,

          weight_unit:
            weight_unit,

          active:
            active,

          created_at:
            new Date(),

          updated_at:
            new Date()

        }]);

      if (insertError) {

        return res.json({

          success: false,

          error:
            insertError.message

        });

      }

      return res.json({

        success: true,

        message:
          "Measurement unit saved successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// UPDATE MEASUREMENT UNIT
// =========================

app.post(
  "/updateMeasurementUnit",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      const company_code =

        String(
          body.company_code || ""
        ).trim();

      const id =

        Number(
          body.id || 0
        );

      const weight_unit =

        String(
          body.weight_unit || ""
        ).trim();

      const active =

        String(
          body.active || ""
        )
          .trim()
          .toUpperCase() === "TRUE";

      // =========================
      // VALIDATION
      // =========================

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!id) {

        return res.json({

          success: false,

          error:
            "id missing"

        });

      }

      if (!weight_unit) {

        return res.json({

          success: false,

          error:
            "Measurement unit required"

        });

      }

      // =========================
      // CHECK EXISTS
      // =========================

      const {
        data: existingRow,
        error: fetchError
      } = await supabase

        .from("measurement_unit")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "id",
          id
        )

        .single();

      if (fetchError || !existingRow) {

        return res.json({

          success: false,

          error:
            "Measurement unit not found"

        });

      }


      // =========================
// DUPLICATE CHECK
// =========================

const {
  data: allRows,
  error: duplicateError
} = await supabase

  .from("measurement_unit")

  .select("*")

  .eq(
    "company_code",
    company_code
  );

if (duplicateError) {

  return res.json({

    success: false,

    error:
      duplicateError.message

  });

}

const duplicateExists =

  (allRows || []).some((row) => {

    return (

      Number(row.id) !== id

      &&

      String(
        row.weight_unit || ""
      )
        .trim()
        .toLowerCase()

      ===

      weight_unit.toLowerCase()

    );

  });

if (duplicateExists) {

  return res.json({

    success: false,

    error:
      "Measurement unit already exists"

  });

}

      // =========================
      // UPDATE
      // =========================

      const {
        error: updateError
      } = await supabase

        .from("measurement_unit")

        .update({

          weight_unit:
            weight_unit,

          active:
            active,

          updated_at:
            new Date()

        })

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "id",
          id
        );

      if (updateError) {

        return res.json({

          success: false,

          error:
            updateError.message

        });

      }

      return res.json({

        success: true,

        message:
          "Measurement unit updated successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// LOGIN USER
// =========================

app.post(
  "/loginUser",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      // =========================
      // INPUTS
      // =========================

      const email =

        String(
          body.email || ""
        )
          .trim()
          .toLowerCase();

      const password =

        String(
          body.password || ""
        ).trim();

      // =========================
      // VALIDATION
      // =========================

      if (!email) {

        return res.json({

          success: false,

          error:
            "Email or mobile required"

        });

      }

      if (!password) {

        return res.json({

          success: false,

          error:
            "Password required"

        });

      }

      // =========================
      // FETCH USERS
      // =========================

      const {
        data,
        error
      } = await supabase

        .from("usersheet")

        .select("*");

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      // =========================
// FIND USER BY LOGIN ID
// =========================

const user =

  (data || []).find((row) => {

    const rowEmail =

      String(
        row.email || ""
      )
        .trim()
        .toLowerCase();

    const rowMobile =

      String(
        row.mobile || ""
      ).trim();

    return (

      rowEmail === email

      ||

      rowMobile === email

    );

  });

// =========================
// LOGIN ID NOT FOUND
// =========================

if (!user) {

  return res.json({

    success: false,

    error:
      "Invalid Login ID"

  });

}

// =========================
// PASSWORD CHECK
// =========================

const rowPassword =

  String(
    user.password || ""
  ).trim();

if (
  rowPassword !== password
) {

  return res.json({

    success: false,

    error:
      "Incorrect Password"

  });

}

// =========================
// ACTIVE CHECK
// =========================

if (
  user.is_active !== true
) {

  return res.json({

    success: false,

    error:
      "Account Inactive"

  });

}
      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        data: {

          user_id:
            user.user_id || "",

          email:
            user.email || "",

          mobile:
            user.mobile || "",

          company_code:
            user.company_code || "",

          name:
            user.name || "",

              role:
  user.role || "",

status:
  user.is_active === true
    ? "ACTIVE"
    : "INACTIVE"

        }

      });

    }

    catch (err) {

      console.log(
        "LOGIN ERROR:",
        err
      );

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// FORGOT PASSWORD
// =========================

app.post(
  "/forgotPassword",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      // =========================
      // INPUT
      // =========================

      const email =

        String(
          body.email || ""
        )
          .trim()
          .toLowerCase();

      // =========================
      // VALIDATION
      // =========================

      if (!email) {

        return res.json({

          success: false,

          error:
            "Email required"

        });

      }

      // =========================
      // FIND USER
      // =========================

      const {
        data,
        error
      } = await supabase

        .from("usersheet")

        .select("*")

        .eq(
          "email",
          email
        )

        .single();

      if (error || !data) {

        return res.json({

          success: false,

          error:
            "Email not found"

        });

      }

      // =========================
      // GET PASSWORD
      // =========================

      const password =

        String(
          data.password || ""
        );

      // =========================
      // SEND EMAIL
      // =========================
console.log("RESEND PAYLOAD:", {

  from: "Billey <noreply@billey.in>",

  to: email,

  replyTo: companyEmail || undefined,

  subject:
    `${companyName || "Company"} - Consolidated Invoice Statement`

});

    const result =
  await resend.emails.send({

   from: "noreply@billey.in",

   to:
  email,

    subject:
      "BillKaro Password Recovery",

    html: `

      <div style="font-family:Arial;padding:20px">

        <h2>
          BillKaro Password Recovery
        </h2>

        <p>
          Your password is:
        </p>

        <h1>
          ${password}
        </h1>

      </div>

    `

  });

console.log(
  "RESEND RESULT =",
  result
);

if (result.error) {

  return res.json({

    success: false,

    error:
      result.error

  });

}

      // =========================
      // SUCCESS
      // =========================

      return res.json({

        success: true,

        message:
          "Password sent to email"

      });

    }

    catch (err) {

      console.log(
        "FORGOT PASSWORD ERROR:",
        err
      );

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


app.post(
  "/searchLicenseUser",
  async (req, res) => {

    try {

      const { mobile } = req.body;

      if (!mobile) {

        return res.json({
          success: false,
          message: "Mobile number required",
        });

      }

      // STEP 1
      // SEARCH USER

      const { data: userRow, error: userError } =
        await supabase
          .from("usersheet")
          .select("*")
          .eq("mobile", mobile)
          .single();

      if (userError || !userRow) {

        return res.json({
          success: false,
          message: "User not found",
        });

      }

// =========================
// GET COMPANY DETAILS
// =========================

const { data: companyRow } =
  await supabase
    .from("company")
    .select(`
  company_code,
  businessname,
  address,
  state,
  partner_id,
  agent_id
`)
    .eq(
      "company_code",
      userRow.company_code
    )
    .single();


// =========================
// GET LAST LICENSE
// =========================

const { data: lastLicense } =
  await supabase
    .from("license_master")
    .select("*")
    .eq(
      "company_code",
      userRow.company_code
    )
    .order(
      "valid_to",
      {
        ascending: false
      }
    )
    .limit(1)
    .maybeSingle();

console.log(
  "LICENSE SEARCH RESULT",
  {
    mobile,
    userRow,
    companyRow,
    lastLicense
  }
);

     return res.json({

  success: true,

  data: {

    name:
      userRow?.name || "",

    company_code:
      userRow?.company_code || "",

    businessname:
  companyRow?.businessname || "",

    address:
      companyRow?.address || "",

    state:
      companyRow?.state || "",

    partner_id:
      companyRow?.partner_id || null,

    agent_id:
      companyRow?.agent_id || null,

    license_type:
      lastLicense
        ? "Renewal"
        : "Fresh Activation",

    last_renewal_date:
      lastLicense?.valid_to || null,

  },

});

    } catch (err) {

      console.error(
        "searchLicenseUser error",
        err
      );

      res.json({
        success: false,
        message: err.message,
      });

    }

  }
);


app.post(
  "/saveLicense",
  async (req, res) => {

    try {

      const {

        company_code,
        valid_from,
        valid_to,
        status,
        created_by,
        collection_status,
         login_mobile,
        role,
        selected_plan,
        subscription_amt

      } = req.body;

      console.log(
  "SAVE LICENSE BODY",
  req.body
);

const roleUpper =
  String(role || "")
    .toUpperCase();

// =========================
// AGENT ACTIVE CHECK
// =========================

if (
  roleUpper === "AGENT"
) {

  const {
    data: userRow,
    error: userError
  } = await supabase

    .from("usersheet")

    .select("is_active")

    .eq(
      "mobile",
      login_mobile
    )

    .single();

  if (
    userError ||
    !userRow ||
    userRow.is_active !== true
  ) {

    return res.json({

      success: false,

      forceLogout: true,

      message:
        "Agent account inactive"

    });

  }

}

      if (
        !company_code ||
        !valid_from ||
        !valid_to
      ) {

        return res.json({

          success: false,
          message:
            "Missing required fields"

        });

      }

// =========================
// ROLE SECURITY CHECK
// =========================



if (
  roleUpper === "AGENT" ||
  roleUpper === "PARTNER"
) {

  const {
    data: companyRow,
    error: companyFetchError
  } = await supabase
    .from("company")
    .select(`
      agent_id,
      partner_id
    `)
    .eq(
      "company_code",
      company_code
    )
    .single();

  if (
    companyFetchError ||
    !companyRow
  ) {

    return res.json({

      success: false,

      message:
        "Company not found"

    });

  }

  if (
    roleUpper === "AGENT" &&
    String(
      companyRow.agent_id || ""
    ).trim() !==
    String(
      login_mobile || ""
    ).trim()
  ) {

    return res.json({

      success: false,

      message:
        "This company is not assigned to you"

    });

  }

  if (
    roleUpper === "PARTNER" &&
    String(
      companyRow.partner_id || ""
    ).trim() !==
    String(
      login_mobile || ""
    ).trim()
  ) {

    return res.json({

      success: false,

      message:
        "This company is not assigned to you"

    });

  }

}

const { data: existingLicense } =
  await supabase
    .from("license_master")
    .select("id")
    .eq(
      "company_code",
      company_code
    )
    .limit(1)
    .maybeSingle();

    let partner_id = null;
let agent_id = null;

if (
  roleUpper === "PARTNER"
) {

  partner_id =
    login_mobile;

}

else if (
  roleUpper === "AGENT"
) {

  agent_id =
    login_mobile;

  const {
  data: mappingRow,
  error: mappingError
} = await supabase
  .from(
    "user_mapping"
  )
    .select(`
      creator_role,
      creator_mobile
    `)
    .eq(
      "user_mobile",
      login_mobile
    )
    .eq(
      "user_role",
      "AGENT"
    )
    .maybeSingle();

    if (
  mappingError
) {

  throw mappingError;

}

  if (
    mappingRow &&
    mappingRow.creator_role ===
      "PARTNER"
  ) {

    partner_id =
      mappingRow.creator_mobile;

  }

}

      const { data, error } =
        await supabase
          .from(
            "license_master"
          )
          .insert([{

            company_code,

            valid_from,

            valid_to,

            status:
              status ||
              "Active",

            created_by:
              created_by ||
              "OWNER",

            collection_status:
              collection_status ||
              "Pending",

              partner_id,

              agent_id,

              selected_plan,

              subscription_amt,

          }])
          .select();

      if (error) {

        console.error(
          "SAVE LICENSE ERROR",
          error
        );

        return res.json({

          success: false,
          message:
            error.message

        });

      }


// =========================
// UPDATE COMPANY LICENSE
// =========================

const updateData = {

  license_from: valid_from,

  license_till: valid_to

};

if (!existingLicense) {

  updateData.license_start =
    valid_from;

}

const {
  error: companyError
} = await supabase
  .from("company")
  .update(updateData)
  .eq(
    "company_code",
    company_code
  );

if (companyError) {

  console.error(
    "COMPANY UPDATE ERROR",
    companyError
  );

  return res.json({

    success: false,

    message:
      companyError.message

  });

}

      return res.json({

        success: true,

        message:
          "License activated successfully",

        data

      });

    } catch (err) {

      console.error(
        "saveLicense error",
        err
      );

      return res.json({

        success: false,

        message:
          err.message

      });

    }

  }
);


app.post(
  "/createAgent",

  async (req, res) => {

    try {

      const body =
        req.body || {};

      const agentName =

        String(
          body.agentName || ""
        ).trim();

      const mobile =

  String(
    body.mobile || ""
  )

    .replace(/\r/g, "")
    .replace(/\n/g, "")
    .trim();

      const email =

        String(
          body.email || ""
        )
          .trim()
          .toLowerCase();

      const creatorRole =

        String(
          body.creatorRole || ""
        )
          .trim()
          .toUpperCase();

      const creatorMobile =

        String(
          body.creatorMobile || ""
        ).trim();

      console.log(
        "CREATE AGENT BODY:",
        body
      );

      // =========================
      // CHECK USER EXISTS
      // =========================

      const {
        data: existingUser,
        error: userError
      } = await supabase

        .from("usersheet")

        .select("*")

        .eq(
          "mobile",
          mobile
        )

        .limit(1);

      if (userError) {

        return res.json({

          success: false,

          error:
            userError.message

        });

      }

      if (
        !existingUser ||
        existingUser.length === 0
      ) {

        return res.json({

          success: false,

          error:
            "User not found"

        });

      }

      // =========================
      // ROLE MUST BE BLANK
      // =========================

      if (
        String(
          existingUser[0]?.role || ""
        ).trim() !== ""
      ) {

        return res.json({

          success: false,

          error:
            "User already assigned role"

        });

      }

      console.log(
  "MOBILE CHECK:",
  JSON.stringify(mobile)
);

      // =========================
      // CHECK ALREADY MAPPED
      // =========================

      const {
        data: existingMapping,
        error: mappingError
      } = await supabase

        .from("user_mapping")

        .select("*")
        
        .eq(
          "user_mobile",
          mobile
        )

        .limit(1);

        console.log(
  "EXISTING MAPPING:",
  existingMapping
);


      if (mappingError) {

        return res.json({

          success: false,

          error:
            mappingError.message

        });

      }

      if (
        existingMapping &&
        existingMapping.length > 0
      ) {

        return res.json({

          success: false,

          error:
            "Mobile already mapped"

        });

      }

      // =========================
      // INSERT MAPPING
      // =========================

      const {
        data: insertData,
        error: insertError
      } = await supabase

        .from("user_mapping")

        .insert([

          {

            user_mobile:
              mobile,

            user_role:
              "AGENT",

            creator_role:
              creatorRole,

            creator_mobile:
              creatorMobile

          }

        ])

        .select();

      if (insertError) {

        return res.json({

          success: false,

          error:
            insertError.message

        });

      }

      console.log(
        "AGENT INSERTED:",
        insertData
      );

      // =========================
      // UPDATE USER ROLE
      // =========================

      const {
        error: roleUpdateError
      } = await supabase

        .from("usersheet")

        .update({

          role: "AGENT"

        })

        .eq(
          "mobile",
          mobile
        );

      if (
        roleUpdateError
      ) {

        return res.json({

          success: false,

          error:
            roleUpdateError.message

        });

      }

      return res.json({

        success: true,

        message:
          "Agent created"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


app.post(
  "/searchAgentUser",
  async (req, res) => {

    try {

      const { mobile } =
        req.body;

      if (!mobile) {

        return res.json({
          success: false,
          error: "Mobile number required",
        });

      }

      const {
        data,
        error,
      } = await supabase
        .from("usersheet")
        .select("*")
        .eq(
          "mobile",
          mobile
        )
        .single();

      if (error || !data) {

        return res.json({
          success: false,
          error: "User not found",
        });

      }

      return res.json({
        success: true,
        data,
      });

    } catch (error) {

      console.error(error);

      return res.json({
        success: false,
        error:
          "Something went wrong",
      });

    }

  }
);

app.post(
"/createPartner",
async (req, res) => {


try {

  const {

    mobile,

    creatorRole,

    creatorMobile

  } = req.body;

  if (!mobile) {

    return res.json({

      success: false,

      error:
        "Mobile number required"

    });

  }

  // =========================
  // FIND USER
  // =========================

  const {

    data: user,

    error: userError

  } = await supabase

    .from("usersheet")

    .select("*")

    .eq(
      "mobile",
      mobile
    )

    .maybeSingle();

  if (
    userError ||
    !user
  ) {

    return res.json({

      success: false,

      error:
        "User not found"

    });

  }

  // =========================
  // ROLE VALIDATION
  // =========================

  if (
    user.role ===
    "AGENT"
  ) {

    return res.json({

      success: false,

      error:
        "This mobile number is already registered as an Agent."

    });

  }

  if (
    user.role ===
    "PARTNER"
  ) {

    return res.json({

      success: false,

      error:
        "This mobile number is already registered as a Partner."

    });

  }

  if (
    user.role ===
    "OWNER"
  ) {

    return res.json({

      success: false,

      error:
        "Owner account cannot be registered as Partner."

    });

  }

  // =========================
  // DUPLICATE MAPPING CHECK
  // =========================

  const {

    data: existingMapping

  } = await supabase

    .from("user_mapping")

    .select("id")

    .eq(
      "user_mobile",
      mobile
    )

    .eq(
      "user_role",
      "PARTNER"
    )

    .maybeSingle();

  if (
    existingMapping
  ) {

    return res.json({

      success: false,

      error:
        "Partner mapping already exists"

    });

  }

  // =========================
  // UPDATE USER ROLE
  // =========================

  const {

    error: updateError

  } = await supabase

    .from("usersheet")

    .update({

      role:
        "PARTNER"

    })

    .eq(
      "mobile",
      mobile
    );

  if (
    updateError
  ) {

    return res.json({

      success: false,

      error:
        updateError.message

    });

  }

  // =========================
  // USER MAPPING ENTRY
  // =========================

  const {

    error: mappingError

  } = await supabase

    .from("user_mapping")

    .insert({

      user_mobile:
        mobile,

      user_role:
        "PARTNER",

      creator_role:
        creatorRole,

      creator_mobile:
        creatorMobile

    });

  if (
    mappingError
  ) {

    return res.json({

      success: false,

      error:
        mappingError.message

    });

  }

  return res.json({

    success: true,

    message:
      "Partner created successfully"

  });

} catch (error) {

  console.error(
    error
  );

  return res.json({

    success: false,

    error:
      "Something went wrong"

  });

}


}
);



// =========================
// GET TOTAL PARTNERS
// =========================

app.get(
  "/getTotalPartners",

  async (req, res) => {

    try {

      const {
        data,
        error
      } = await supabase

        .from("usersheet")

        .select("mobile")

        .eq(
          "role",
          "PARTNER"
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      return res.json({

        success: true,

        data: {

          totalPartners:

            data?.length || 0

        }

      });

    } catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

app.post(
  "/createCustomer",

  async (req, res) => {

    try {

      const {

        customerName,
        mobile,
        email,
        creatorRole,
        creatorMobile

      } = req.body;

      if (!mobile) {

        return res.json({

          success: false,

          error:
            "Mobile number required"

        });

      }

      // =========================
      // FIND USER
      // =========================

      const {

        data: user,

        error: userError

      } = await supabase

        .from("usersheet")

        .select("*")

        .eq(
          "mobile",
          mobile
        )

        .maybeSingle();

      if (
        userError ||
        !user
      ) {

        return res.json({

          success: false,

          error:
            "User not found"

        });

      }

      // =========================
      // ROLE MUST BE BLANK
      // MAIN CONTROL
      // =========================

      if (
        String(
          user.role || ""
        ).trim() !== ""
      ) {

        return res.json({

          success: false,

          error:
            "User already assigned role"

        });

      }

      // =========================
      // FIND COMPANY
      // =========================

      const {

        data: company,

        error: companyError

      } = await supabase

        .from("company")

        .select("*")

        .eq(
          "mobile",
          mobile
        )

        .maybeSingle();

      if (
        companyError ||
        !company
      ) {

        return res.json({

          success: false,

          error:
            "Company not found"

        });

      }

      // =========================
      // ALREADY ASSIGNED ?
      // SAFETY CHECK
      // =========================

      if (

        String(
          company.partner_id || ""
        ).trim() !== ""

        ||

        String(
          company.agent_id || ""
        ).trim() !== ""

      ) {

        return res.json({

          success: false,

          error:
            "Customer already assigned"

        });

      }

      let partnerId = null;

      let agentId = null;

      // =========================
      // OWNER
      // =========================

      if (
        creatorRole ===
        "OWNER"
      ) {

        partnerId = null;

        agentId = null;

      }

      // =========================
      // PARTNER
      // =========================

      else if (
        creatorRole ===
        "PARTNER"
      ) {

        partnerId =
          creatorMobile;

        agentId = null;

      }

      // =========================
      // AGENT
      // =========================

      else if (
        creatorRole ===
        "AGENT"
      ) {

        agentId =
          creatorMobile;

        const {

          data: agentMapping,

          error: mappingError

        } = await supabase

          .from(
            "user_mapping"
          )

          .select("*")

          .eq(
            "user_mobile",
            creatorMobile
          )

          .eq(
            "user_role",
            "AGENT"
          )

          .maybeSingle();

        if (
          mappingError
        ) {

          return res.json({

            success: false,

            error:
              mappingError.message

          });

        }

        // AGENT UNDER PARTNER

        if (
          agentMapping &&
          String(
            agentMapping.creator_role || ""
          ).toUpperCase() ===
            "PARTNER"
        ) {

          partnerId =
            agentMapping.creator_mobile;

        }

      }
      // =========================
      // UPDATE COMPANY
      // =========================

      const {

        error: updateError

      } = await supabase

        .from("company")

        .update({

          partner_id:
            partnerId,

          agent_id:
            agentId

        })

        .eq(
          "mobile",
          mobile
        );

      if (
        updateError
      ) {

        return res.json({

          success: false,

          error:
            updateError.message

        });

      }

      // =========================
      // BACKFILL LICENSE MASTER
      // =========================

      const {

        error: backfillError

      } = await supabase

        .from("license_master")

        .update({

          partner_id:
            partnerId,

          agent_id:
            agentId

        })

        .eq(
          "company_code",
          company.company_code
        )

        .or(
          "partner_id.is.null,agent_id.is.null"
        );

      if (
        backfillError
      ) {

        console.log(
          "LICENSE MASTER BACKFILL ERROR:",
          backfillError
        );

      }

      // =========================
      // UPDATE USER ROLE
      // =========================

      const {

        error: roleError

      } = await supabase

        .from("usersheet")

        .update({

          role: "USER"

        })

        .eq(
          "mobile",
          mobile
        );

      if (
        roleError
      ) {

        return res.json({

          success: false,

          error:
            roleError.message

        });

      }

      return res.json({

        success: true,

        message:
          "Customer added successfully"

      });

    }

    catch (error) {

      console.error(
        error
      );

      return res.json({

        success: false,

        error:
          error.message

      });

    }

  }

);


app.post(
  "/searchCustomerUser",
  async (req, res) => {

    try {

      const { mobile } =
        req.body;

      if (!mobile) {

        return res.json({
          success: false,
          error: "Mobile number required",
        });

      }

      // STEP 1
      // FIND USER

      const {
        data: userData,
        error: userError,
      } = await supabase
        .from("usersheet")
        .select("*")
        .eq(
          "mobile",
          mobile
        )
        .single();

      if (
        userError ||
        !userData
      ) {

        return res.json({
          success: false,
          error: "User not found",
        });

      }

      // STEP 2
      // FIND COMPANY

      let companyData = null;

      if (
        userData.company_code
      ) {

        const {
          data,
          error,
        } = await supabase
          .from("company")
          .select("*")
          .eq(
            "company_code",
            userData.company_code
          )
          .single();

        if (!error) {

          companyData = data;

        }

      }

      // STEP 3
      // RETURN COMBINED DATA

      return res.json({

        success: true,

        data: {

          mobile:
            userData.mobile || "",

          name:
            userData.name || "",

          email:
            userData.email || "",

          role:
           userData.role || "",

          companyCode:
            userData.company_code || "",

          companyName:
            companyData?.businessname || "",

          address:
            companyData?.address || "",

        },

      });

    } catch (error) {

      console.error(error);

      return res.json({

        success: false,

        error:
          "Something went wrong",

      });

    }

  }
);


app.post(
  "/getMyCompanies",
  async (req, res) => {

    try {

      const {
        mobile,
        role,
      } = req.body;

console.log(
  "ROLE RECEIVED:",
  role
);

      let query =
        supabase
          .from("company")
          .select("*");
if (role === "OWNER") {

  const {
    data: users,
    error: userError,
  } = await supabase
    .from("usersheet")
    .select("mobile, role")
    .eq("role", "USER");

  if (userError) {
    return res.json({
      success: false,
      error: userError.message,
    });
  }

  const userMobiles =
    (users || []).map(
      (u) => String(u.mobile)
    );

  console.log(
    "USER MOBILES:",
    userMobiles
  );

  if (userMobiles.length === 0) {

    return res.json({
      success: true,
      data: []
    });

  }

  query =
    query.in(
      "mobile",
      userMobiles
    );

}

      else if (
        role === "PARTNER"
      ) {

        query =
          query.eq(
            "partner_id",
            mobile
          );

      }

      else if (
        role === "AGENT"
      ) {

        query =
          query.eq(
            "agent_id",
            mobile
          );

      }

      else {

        return res.json({
          success: false,
          error:
            "Invalid role",
        });

      }

      const {
        data,
        error,
      } = await query;

      if (error) {

        return res.json({
          success: false,
          error: error.message,
        });

      }

      console.log(
  "GET MY COMPANIES RESULT:",
  data
);


const {
  data: allUsers,
  error: usersError
} = await supabase

  .from("usersheet")

  .select(
    "mobile,name,role"
  );

if (usersError) {

  return res.json({

    success: false,

    error:
      usersError.message,

  });

}

const userMap =

  new Map(

    (allUsers || []).map(

      (u) => [

        String(
          u.mobile
        ),

        u

      ]

    )

  );

const today =
  new Date();

const finalData =

  (data || []).map(

    (company) => {

      let subscription_status =
        "NOT_SUBSCRIBED";

      if (
        company.license_from
      ) {

        subscription_status =
          "ACTIVE";

        if (
          company.license_till &&
          new Date(
            company.license_till
          ) < today
        ) {

          subscription_status =
            "EXPIRED";

        }

      }

     return {

  ...company,

  customer_name:
    userMap.get(
      String(company.mobile)
    )?.name || "",

  partner_name:
    userMap.get(
      String(
        company.partner_id || ""
      )
    )?.name || "",

  agent_name:
    userMap.get(
      String(
        company.agent_id || ""
      )
    )?.name || "",

  subscription_status

};

}

);

      return res.json({

        success: true,

        data:
  finalData,

      });

    } catch (error) {

      console.error(error);

      return res.json({

        success: false,

        error:
          "Something went wrong",

      });

    }

  }
);



app.post(
  "/getRecentActivities",

  async (req, res) => {

    try {

      const {
        mobile,
        role
      } = req.body || {};

      if (!mobile) {

        return res.json({

          success: false,

          message:
            "Mobile required"

        });

      }

      let userMappingQuery =

        supabase

          .from(
            "user_mapping"
          )

          .select("*");

      let licenseQuery =

        supabase

          .from(
            "license_master"
          )

          .select("*");

      const userRole =

        String(
          role || ""
        ).toUpperCase();

      if (
        userRole ===
        "PARTNER"
      ) {

        userMappingQuery =

          userMappingQuery.eq(
            "creator_mobile",
            mobile
          );

        licenseQuery =

          licenseQuery.eq(
            "partner_id",
            mobile
          );

      }

      else if (
        userRole ===
        "AGENT"
      ) {

        userMappingQuery =

          userMappingQuery.eq(
            "user_mobile",
            mobile
          );

        licenseQuery =

          licenseQuery.eq(
            "agent_id",
            mobile
          );

      }


      const usersResult =

  await supabase
    .from("usersheet")
    .select(
      "mobile,name"
    );

if (
  usersResult.error
) {
  throw usersResult.error;
}

    const userMap = {};

(usersResult.data || [])
  .forEach((u) => {

    userMap[
      u.mobile
    ] = u.name;

  });

      const [

        userResult,

        licenseResult

      ] = await Promise.all([

        userMappingQuery
          .order(
            "created_at",
            {
              ascending:
                false
            }
          )
          .limit(100),

        licenseQuery
          .order(
            "created_on",
            {
              ascending:
                false
            }
          )
          .limit(100)

      ]);

      if (
        userResult.error
      ) {

        throw userResult.error;

      }

      if (
        licenseResult.error
      ) {

        throw licenseResult.error;

      }

      const userActivities =

        (
          userResult.data || []
        ).map(

          (row) => ({

            activity_type:
              "USER_CREATED",

            activity:

              `Created ${row.user_role}`,

            user_mobile:
              row.user_mobile,

            user_role:
              row.user_role,

            creator_mobile:
              row.creator_mobile,

            creator_role:
              row.creator_role,

            creator_name:

  userMap[
    row.creator_mobile
  ] || "",  

           created_at:
  new Date(
    row.created_at
  ).toISOString()

          })

        );

      const licenseActivities =

        (
          licenseResult.data || []
        ).map(

          (row) => ({

            activity_type:
              "LICENSE",

            activity:

              `${row.selected_plan} Plan Activated`,

            company_code:
              row.company_code,

            selected_plan:
              row.selected_plan,

            amount:
              row.subscription_amt,

            created_by:
              row.created_by,

                payment_verified:
        row.payment_verified,

            partner_id:
              row.partner_id,

            agent_id:
              row.agent_id,

          creator_name:

  userMap[
    row.agent_id
  ] ||

  userMap[
    row.partner_id
  ] ||

  "",

agent_name:

  userMap[
    row.agent_id
  ] || "",

partner_name:

  userMap[
    row.partner_id
  ] || "",

           created_at:
  new Date(
    row.created_on
  ).toISOString()

          })

        );

      const activities =

        [

          ...userActivities,

          ...licenseActivities

        ]

          .sort(

            (a, b) =>

              new Date(
                b.created_at
              ) -

              new Date(
                a.created_at
              )

          )

          .slice(
            0,
            100
          );

      return res.json({

        success: true,

        count:
          activities.length,

        data:
          activities

      });

    } catch (error) {

      console.error(
        error
      );

      return res.json({

        success: false,

        message:
          error.message ||
          "Server Error"

      });

    }

  }
);


app.post(
  "/getAgentActivationReport",
  async (req, res) => {

    try {

     const {
  mobile,
  role,
  dateFilter,
  fromDate,
  toDate
} = req.body || {};

      if (!mobile) {

        return res.json({
          success: false,
          message: "Mobile required"
        });

      }

      let query = supabase
        .from("license_master")
        .select("*");

              let partnerAgentMap = {};

        const today =
  new Date();

if (
  dateFilter ===
  "TODAY"
) {

  const start =
    new Date();

  start.setHours(
    0, 0, 0, 0
  );

  query =
    query.gte(
      "created_on",
      start.toISOString()
    );

}
else if (
  dateFilter ===
  "THIS_WEEK"
) {

  const start =
    new Date();

  const day =
    start.getDay();

  const diff =
    day === 0
      ? 6
      : day - 1;

  start.setDate(
    start.getDate() -
    diff
  );

  start.setHours(
    0, 0, 0, 0
  );

  query =
    query.gte(
      "created_on",
      start.toISOString()
    );

}

else if (
  dateFilter ===
  "THIS_MONTH"
) {

  const start =
    new Date(
      today.getFullYear(),
      today.getMonth(),
      1
    );

  query =
    query.gte(
      "created_on",
      start.toISOString()
    );

}

else if (
  dateFilter ===
  "DATE_RANGE" &&
  fromDate &&
  toDate
) {

  query =
    query
      .gte(
        "created_on",
        fromDate
      )
      .lte(
        "created_on",
        toDate +
        "T23:59:59"
      );

}

      const userRole =
        String(role || "")
          .toUpperCase();

      if (
        userRole === "PARTNER"
      ) {

        query =
          query.eq(
            "partner_id",
            mobile
          );

      }

      else if (
        userRole === "AGENT"
      ) {

        query =
          query.eq(
            "agent_id",
            mobile
          );

      }

else if (
  userRole === "OWNER"
) {

  const {
    data: mappings,
    error: mappingError
  } = await supabase

    .from("user_mapping")

   .select(`
  user_mobile,
  user_role,
  creator_mobile,
  creator_role
`)
    .eq(
      "creator_mobile",
      mobile
    )

    .eq(
      "creator_role",
      "OWNER"
    )

  //  .eq(
 //     "user_role",
 //     "AGENT"
 //   );

  if (mappingError) {

    throw mappingError;

  }

  console.log(
  "OWNER MAPPINGS",
  mappings
);

const ownerPartners =

  (mappings || [])
    .filter(
      row =>
        row.user_role ===
        "PARTNER"
    );

console.log(
  "OWNER PARTNERS",
  ownerPartners
);


const partnerMobiles =

  ownerPartners.map(
    row => row.user_mobile
  );

console.log(
  "PARTNER MOBILES",
  partnerMobiles
);


const {
  data: partnerAgentMappings,
  error: partnerAgentError
} = await supabase

  .from("user_mapping")

  .select(`
    user_mobile,
    creator_mobile
  `)

  .in(
    "creator_mobile",
    partnerMobiles
  )

  .eq(
    "creator_role",
    "PARTNER"
  )

  .eq(
    "user_role",
    "AGENT"
  );

if (partnerAgentError) {

  throw partnerAgentError;

}

console.log(
  "PARTNER AGENTS",
  partnerAgentMappings
);



(partnerAgentMappings || [])
  .forEach(row => {

    partnerAgentMap[
      row.user_mobile
    ] = row.creator_mobile;

  });

const ownerDirectAgents =

  (mappings || [])

    .filter(
      row =>
        row.user_role ===
        "AGENT"
    )

    .map(
      row =>
        row.user_mobile
    );

const partnerAgents =

  (partnerAgentMappings || [])

    .map(
      row =>
        row.user_mobile
    );

const ownerAgents = [

  ...ownerDirectAgents,

  ...partnerAgents

];

console.log(
  "ALL OWNER AGENTS",
  ownerAgents
);

const {
  data: allMappings,
  error: allMappingsError
} = await supabase

  .from("user_mapping")

  .select(`
    user_mobile,
    user_role,
    creator_mobile,
    creator_role
  `);

if (allMappingsError) {

  throw allMappingsError;

}

console.log(
  "ALL MAPPINGS",
  allMappings
);

  query =
    query.in(
      "agent_id",
      ownerAgents
    );

}
      
      const {
        data: licenses,
        error: licenseError
      } = await query;

      if (licenseError) {
        throw licenseError;
      }

      const {
        data: users,
        error: usersError
      } = await supabase
        .from("usersheet")
       .select(
  "mobile,name,role"
);

      if (usersError) {
        throw usersError;
      }

      const userMap = {};

      const roleMap = {};



     (users || []).forEach(
  (u) => {

    userMap[
      String(u.mobile)
    ] = u.name;

    roleMap[
      String(u.mobile)
    ] = u.role;

  }
);
      const grouped = {};

      (licenses || []).forEach(
        (row) => {

          const agentMobile =
            String(
              row.agent_id || ""
            );

          if (!agentMobile) {
            return;
          }

          if (
            !grouped[
              agentMobile
            ]
          ) {

            grouped[
              agentMobile
            ] = {

              agent_mobile:
                agentMobile,

              agent_name:

                userMap[
                  agentMobile
                ] || "",

             parent_mobile:

              partnerAgentMap[
                agentMobile
              ] || mobile,

            parent_name:

              userMap[
                partnerAgentMap[
                  agentMobile
                ] || mobile
              ] || "",

            parent_role:

              partnerAgentMap[
                agentMobile
              ]
                ? "PARTNER"
                : "OWNER",

              activations: 0,

              collection: 0,

              details: []

            };

          }

          grouped[
            agentMobile
          ].activations += 1;

          grouped[
            agentMobile
          ].collection +=
            Number(
              row.subscription_amt || 0
            );

          grouped[
            agentMobile
          ].details.push({

            date:
              row.created_on,

            company:
              row.company_code,

            plan:
              row.selected_plan,

            amount:
              Number(
                row.subscription_amt || 0
              )

          });

        }
      );

      return res.json({

        success: true,

        data:

          Object.values(
            grouped
          )

      });

    } catch (error) {

      console.error(error);

      return res.json({

        success: false,

        message:
          error.message ||
          "Server Error"

      });

    }

  }
);


app.get(
  "/getActivePlans",
  async (req, res) => {

    try {

      const {
        data,
        error
      } = await supabase

        .from("license")

        .select("*")

        .eq(
          "launched",
          true
        )

        .order(
          "sort_order",
          {
            ascending: true
          }
        );

      if (error) {
        throw error;
      }

      return res.json({
        success: true,
        data
      });

    } catch (err) {

      console.error(err);

      return res.status(500)
        .json({
          success: false,
          message:
            err.message
        });

    }

  }
);


// =========================
// GET AGENTS
// =========================

app.post(
  "/getAgents",

  async (req, res) => {

    try {

      const mobile =

        String(
          req.body.mobile || ""
        ).trim();

        const role =

          String(
            req.body.role || ""
          )
            .toUpperCase();

            

      if (!mobile) {

        return res.json({

          success: false,

          error:
            "mobile missing"

        });

      }

      // =========================
      // GET PARTNER AGENTS
      // =========================

      const {

        data: mappings,

        error: mappingError

      } = await supabase

        .from("user_mapping")

        .select(`
          user_mobile
        `)

        .eq(
          "creator_mobile",
          mobile
        )

        .eq(
          "creator_role",
          role
        )

        .eq(
          "user_role",
          "AGENT"
        );

      if (mappingError) {

        return res.json({

          success: false,

          error:
            mappingError.message

        });

      }

      const agentMobiles =

        (mappings || []).map(
          row =>
            String(
              row.user_mobile || ""
            )
        );

      if (
        agentMobiles.length === 0
      ) {

        return res.json({

          success: true,

          data: []

        });

      }

      // =========================
      // GET AGENT DETAILS
      // =========================

      const {

        data,

        error

      } = await supabase

        .from("usersheet")

        .select(`
          id,
          name,
          mobile,
          is_active
        `)

        .in(
          "mobile",
          agentMobiles
        )

        .eq(
          "role",
          "AGENT"
        )

        .order(
          "name",
          {
            ascending: true
          }
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      return res.json({

        success: true,

        data:
          data || []

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


// =========================
// UPDATE AGENT STATUS
// =========================

app.post(
  "/updateAgentStatus",

  async (req, res) => {

    try {

     const {

  partner_mobile,
  agent_mobile,
  is_active,
  role

} = req.body;

const roleUpper =

  String(
    role || ""
  )
    .toUpperCase();

      if (
        !partner_mobile ||
        !agent_mobile
      ) {

        return res.json({

          success: false,

          message:
            "Missing required fields"

        });

      }

      // =========================
      // SECURITY CHECK
      // =========================

      const {

        data: mapping,

        error: mappingError

      } = await supabase

        .from("user_mapping")

        .select("id")

        .eq(
          "creator_mobile",
          partner_mobile
        )

       .eq(
  "creator_role",
  roleUpper
)

        .eq(
          "user_mobile",
          agent_mobile
        )

        .eq(
          "user_role",
          "AGENT"
        )

        .maybeSingle();

      if (
        mappingError
      ) {

        return res.json({

          success: false,

          message:
            mappingError.message

        });

      }

      if (
        !mapping
      ) {

        return res.json({

          success: false,

          message:
            "Unauthorized agent"

        });

      }

      // =========================
      // UPDATE STATUS
      // =========================

      const {

        error

      } = await supabase

        .from("usersheet")

        .update({

          is_active

        })

        .eq(
          "mobile",
          agent_mobile
        )

        .eq(
          "role",
          "AGENT"
        );

      if (
        error
      ) {

        return res.json({

          success: false,

          message:
            error.message

        });

      }

      return res.json({

        success: true,

        message:
          is_active
            ? "Agent activated successfully"
            : "Agent deactivated successfully"

      });

    }

    catch (err) {

      return res.json({

        success: false,

        message:
          err.message

      });

    }

  }
);

// =========================
// LICENSE DASHBOARD STATS
// =========================

app.get(
  "/getLicenseDashboardStats",

  async (req, res) => {

    try {

      const {
        data,
        error
      } = await supabase

        .from("company")

        .select(
          "license_till"
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      const today =
        new Date();

      today.setHours(
        0, 0, 0, 0
      );

      const next7Days =
        new Date(today);

      next7Days.setDate(
        next7Days.getDate() + 7
      );

      let activeLicenses = 0;

      let pendingRenewals = 0;

      let expiredLicenses = 0;

      (data || []).forEach(
        (row) => {

          if (
            !row.license_till
          ) return;

          const validTill =
            new Date(
              row.license_till
            );

          validTill.setHours(
            0, 0, 0, 0
          );

          // Expired

          if (
            validTill < today
          ) {

            expiredLicenses++;

            return;

          }

          // Active

          activeLicenses++;

          // Pending Renewal
          // (next 7 days)

          if (

            validTill >= today

            &&

            validTill <= next7Days

          ) {

            pendingRenewals++;

          }

        }
      );

      return res.json({

        success: true,

        data: {

          activeLicenses,

          pendingRenewals,

          expiredLicenses

        }

      });

    } catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


// =========================
// COLLECTION STATS
// =========================

app.get(
  "/getCollectionStats",

  async (req, res) => {

    try {

      const {
        data,
        error
      } = await supabase

        .from("license_master")

        .select(
          "subscription_amt, created_on"
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      const today =
        new Date();

      const todayStr =
        today
          .toISOString()
          .split("T")[0];

      const currentMonth =
        today.getMonth();

      const currentYear =
        today.getFullYear();

      let todayCollection = 0;

      let monthCollection = 0;

      (data || []).forEach(
        (row) => {

          const amount =
            Number(
              row.subscription_amt || 0
            );

          const createdDate =
            new Date(
              row.created_on
            );

          const rowDate =
            createdDate
              .toISOString()
              .split("T")[0];

          // Today

          if (
            rowDate ===
            todayStr
          ) {

            todayCollection +=
              amount;

          }

          // Month

          if (

            createdDate.getMonth()
              === currentMonth

            &&

            createdDate.getFullYear()
              === currentYear

          ) {

            monthCollection +=
              amount;

          }

        }
      );

      return res.json({

        success: true,

        data: {

          todayCollection,

          monthCollection

        }

      });

    } catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


// =========================
// PARTNER LICENSE STATS
// =========================

app.post(
  "/getPartnerLicenseStats",

  async (req, res) => {

    try {

      const { mobile } =
        req.body;

      const {
        data,
        error
      } = await supabase

        .from("company")

        .select(
          "license_till"
        )

        .eq(
          "partner_id",
          mobile
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      const today =
        new Date();

      today.setHours(
        0, 0, 0, 0
      );

      const next7Days =
        new Date(today);

      next7Days.setDate(
        next7Days.getDate() + 7
      );

      let activeLicenses = 0;

      let pendingRenewals = 0;

      let expiredLicenses = 0;

      (data || []).forEach(
        (row) => {

          if (
            !row.license_till
          ) return;

          const validTill =
            new Date(
              row.license_till
            );

          validTill.setHours(
            0, 0, 0, 0
          );

          if (
            validTill < today
          ) {

            expiredLicenses++;

            return;

          }

          activeLicenses++;

          if (

            validTill >= today

            &&

            validTill <= next7Days

          ) {

            pendingRenewals++;

          }

        }
      );

      return res.json({

        success: true,

        data: {

          activeLicenses,

          pendingRenewals,

          expiredLicenses

        }

      });

    } catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


// =========================
// PARTNER COLLECTION STATS
// =========================

app.post(
  "/getPartnerCollectionStats",

  async (req, res) => {

    try {

      const { mobile } =
        req.body;

      const {
        data,
        error
      } = await supabase

        .from("license_master")

        .select(
          "subscription_amt, created_on"
        )

        .eq(
          "partner_id",
          mobile
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      const today =
        new Date();

      const todayStr =
        today
          .toISOString()
          .split("T")[0];

      const currentMonth =
        today.getMonth();

      const currentYear =
        today.getFullYear();

      let todayCollection = 0;

      let monthCollection = 0;

      (data || []).forEach(
        (row) => {

          const amount =
            Number(
              row.subscription_amt || 0
            );

          const createdDate =
            new Date(
              row.created_on
            );

          const rowDate =
            createdDate
              .toISOString()
              .split("T")[0];

          if (
            rowDate ===
            todayStr
          ) {

            todayCollection +=
              amount;

          }

          if (

            createdDate.getMonth()
              === currentMonth

            &&

            createdDate.getFullYear()
              === currentYear

          ) {

            monthCollection +=
              amount;

          }

        }
      );

      return res.json({

        success: true,

        data: {

          todayCollection,

          monthCollection

        }

      });

    } catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


// =========================
// GET LICENSE PLANS
// =========================

app.get(
  "/getLicensePlans",

  async (req, res) => {

    try {

      const {
        data,
        error
      } = await supabase

        .from("license")

        .select("*")

        .eq(
          "launched",
          true
        )

        .order(
          "sort_order",
          {
            ascending: true
          }
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      const result =

        (data || []).map((row) => ({

          id:
            row.id,

          plan_name:
            row.plan_name || "",

          plan_code:
            row.plan_code || "",

          plan_amount:
            Number(
              row.plan_amount || 0
            ),

          validity_days:
            Number(
              row.validity_days || 0
            ),

          features:
            row.features || ""

        }));

      return res.json({

        success: true,

        data: result

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


app.post(

  "/getSubscriptionPaymentDetails",

  async (req, res) => {

    try {

      const {

        mobile,

        plan_code

      } = req.body;

      const {

        data: company,

        error: companyError

      } = await supabase

        .from("company")

        .select(`
          company_code,
          license_till,
          partner_id,
          agent_id,
          country
        `)

        .eq(
          "mobile",
          mobile
        )

        .single();

      if (

        companyError ||

        !company

      ) {

        return res.json({

          success: false,

          message:
            "Company not found"

        });

      }

      const {

        data: plan,

        error: planError

      } = await supabase

        .from("license")

        .select(`
          plan_name,
          plan_code,
          plan_amount,
          validity_days
        `)

        .eq(
          "plan_code",
          plan_code
        )

        .single();

      if (

        planError ||

        !plan

      ) {

        return res.json({

          success: false,

          message:
            "Plan not found"

        });

      }

      // 👇 YAHAN LAGA

console.log(
  "BEFORE CALCULATION"
);

console.log(
  "COMPANY DATA =",
  company
);

console.log(
  "PLAN DATA =",
  plan
);
      const details =

        calculateSubscriptionDetails(

          company,

          plan

        );

      return res.json({

        success: true,

        ...details

      });

    }

    catch (err) {

      console.log(err);

      return res.json({

        success: false,

        message:
          err.message

      });

    }

  }

);

app.post(

  "/submitLicensePayment",

  async (req, res) => {

    try {

      const {

        mobile,

        plan_code,

        payment_screenshot

      } = req.body;

      const {

        data: company,

        error: companyError

      } = await supabase

        .from("company")

        .select(`
          company_code,
          license_till,
          license_start,
          partner_id,
          agent_id,
          country
        `)

        .eq(
          "mobile",
          mobile
        )

        .single();

      if (

        companyError ||

        !company

      ) {

        return res.json({

          success: false,

          message:
            "Company not found"

        });

      }

      const {

        data: plan,

        error: planError

      } = await supabase

        .from("license")

        .select(`
          plan_name,
          plan_code,
          plan_amount,
          validity_days
        `)

        .eq(
          "plan_code",
          plan_code
        )

        .single();

      if (

        planError ||

        !plan

      ) {

        return res.json({

          success: false,

          message:
            "Plan not found"

        });

      }

      const details =

        calculateSubscriptionDetails(

          company,

          plan

        );

      const {

        error: insertError

      } = await supabase

        .from(
          "license_master"
        )

        .insert({

          company_code:
            company.company_code,

          valid_from:
            details.valid_from,

          valid_to:
            details.valid_to,

          status:
            "Active",

          created_by:
            "USER",

          collection_status:
            "UPI",

          created_on:
            new Date()
              .toISOString(),

          updated_on:
            new Date()
              .toISOString(),

          selected_plan:
            plan.plan_name,

          subscription_amt:
            details.amount,

          partner_id:
            company.partner_id,

          agent_id:
            company.agent_id,

          payment_verified:
            null,

          payment_screenshot

        });

      if (

        insertError

      ) {

        return res.json({

          success: false,

          message:
            insertError.message

        });

      }

      const updateData = {

  license_from:
    details.valid_from,

  license_till:
    details.valid_to

};

if (
  !company.license_start
) {

  updateData.license_start =
    details.valid_from;

}

await supabase

  .from("company")

  .update(updateData)

  .eq(
    "company_code",
    company.company_code
  );

      return res.json({

        success: true,

        ...details

      });

    }

    catch (err) {

      console.log(err);

      return res.json({

        success: false,

        message:
          err.message

      });

    }

  }

);

function calculateSubscriptionDetails(

  company,

  plan

) {

  console.log(
    "CALCULATION FUNCTION HIT"
  );


  // ===================================
  // TEST MODE
  // Change date here for testing
  // ===================================

  //const today =
    //new Date("2026-07-30");

  // ===================================
  // PRODUCTION MODE
  // Uncomment after testing
  // ===================================

   const today =
     new Date();

  today.setHours(
    0,
    0,
    0,
    0
  );

  const country =

  String(
    company.country || ""
  ).trim() ||

  "India";

  console.log(
  "TEST TODAY =",

 formatDateByCountry(
  today,
  country
)
);

  let validFrom;

  let cycles = 1;

  let isRenewal =
    false;

    console.log(
  "COMPANY =",
  company
);

console.log(
  "PLAN =",
  plan
);

  // =====================
  // FRESH SUBSCRIPTION
  // =====================

  if (

    !company.license_till

  ) {

    validFrom =
      new Date(
        today
      );

  }

  // =====================
  // RENEWAL
  // =====================

  else {

    isRenewal =
      true;

    validFrom =
      new Date(
        company.license_till
      );

    validFrom.setDate(
      validFrom.getDate() + 1
    );

    console.log(
  "LICENSE TILL =",
  company.license_till
);

console.log(
  "VALID FROM =",
  formatDateByCountry(
    validFrom,
    country
  )
);

    const diffMs =
      today -
      validFrom;

    const expiredDays =
      Math.max(

        0,

        Math.ceil(

          diffMs /

          (
            1000 *
            60 *
            60 *
            24
          )

        )

      );

    cycles =
      Math.max(

        1,

        Math.ceil(

          expiredDays /

          plan.validity_days

        )

      );

      console.log(
  "EXPIRED DAYS =",
  expiredDays
);

console.log(
  "CYCLES AFTER CALC =",
  cycles
);

  }


  let totalDays =
    cycles *
    plan.validity_days;

  let validTo =
    new Date(
      validFrom
    );

  validTo.setDate(

    validTo.getDate() +

    totalDays -

    1

  );

  while (

    validTo < today

  ) {

    cycles++;

    totalDays =
      cycles *
      plan.validity_days;

    validTo =
      new Date(
        validFrom
      );

    validTo.setDate(

      validTo.getDate() +

      totalDays -

      1

    );

  }

  const amount =

    cycles *

    Number(
      plan.plan_amount
    );


    console.log(
  "FINAL CYCLES =",
  cycles
);

console.log(
  "FINAL AMOUNT =",
  amount
);

console.log(
  "VALID TO =",
  validTo
    .toISOString()
    .split("T")[0]
);

  return {

    company_code:
      company.company_code,

    isRenewal,

    cycles,

    totalDays,

    amount,

valid_from:

  formatDateByCountry(
    validFrom,
    country
  ),

    

    valid_to:

      validTo

        .toISOString()

        .split("T")[0]

  };

}

app.post(

  "/getSubscriptionVerifyDetails",

  async (req, res) => {

    try {

      const {

        data,

        error

      } = await supabase

        .from(
          "license_master"
        )

        .select(`
          company_code,
          selected_plan,
          valid_from,
          valid_to,
          subscription_amt,
          payment_verified,
          partner_id,
          agent_id,
          payment_screenshot,
          created_on
        `)

        .is(
          "payment_verified",
          null
        )

        .eq(
  "created_by",
  "USER"
)

        .order(
          "created_on",
          {
            ascending: false
          }
        );

      if (error) {

        return res.json({

          success: false,

          message:
            error.message

        });

      }

      return res.json({

        success: true,

        data

      });

    }

    catch (err) {

      return res.json({

        success: false,

        message:
          err.message

      });

    }

  }

);

app.post(

  "/verifySubscriptionPayment",

  async (req, res) => {

    try {

      const company_code =

        String(
          req.body.company_code || ""
        ).trim();

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      const {

        data,

        error

      } = await supabase

        .from(
          "license_master"
        )

        .update({

          payment_verified:
            "TRUE"

        })

        .eq(

          "company_code",

          company_code

        )

        

        .select();

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

const { data: company } =
await supabase
  .from("company")
  .select("mobile")
  .eq("company_code", company_code)
  .single();

console.log(
  "COMPANY:",
  company
);

  if (!company?.mobile) {

  console.log(
    "COMPANY MOBILE NOT FOUND:",
    company_code
  );

}
else {

  const { data: matchedUser } =
await supabase
  .from("usersheet")
  .select("*")
  .eq("user_id", company.mobile);

console.log(
  "MATCHED USER:",
  matchedUser
);

  const {
    data: userUpdate,
    error: userError
  } = await supabase
    .from("usersheet")
    .update({
      role: "USER"
    })
    .eq("user_id", company.mobile)
    .select();

 console.log(
  "USER ROLE UPDATE:",
  userUpdate
);

console.log(
  "USER ROLE ERROR:",
  userError
);

}


      return res.json({

        success: true,

        message:
          "Payment verified successfully",

        data

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }

);

app.post(
  "/createConsolidatedShare",

  async (req, res) => {

    try {

      const {
        company_code,
        invoice_ids
      } = req.body;

      const token =
        crypto
          .randomBytes(16)
          .toString("hex");

      const {
        error
      } = await supabase

        .from(
          "consolidated_share"
        )

        .insert([{

          token,

          company_code,

          invoice_ids

        }]);

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      return res.json({

        success: true,

        token

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

app.post(
  "/sendConsolidatedShareEmail",
  async (req, res) => {

    try {

      const {
        token,
        email,
        companyName,
        companyEmail,
        periodText
      } = req.body;

      const shareUrl =
        `https://www.billey.in/share-consolidated/${token}`;

      const result =
        await resend.emails.send({

          from:
            "Billey <noreply@billey.in>",

          replyTo:
            companyEmail || undefined,

          to:
            email,

          subject:
            `${companyName || "Company"} - Consolidated Invoice Statement`,

          html: `
            <div style="
              font-family: Arial, sans-serif;
              max-width: 600px;
              margin: auto;
              line-height: 1.6;
              color: #333;
            ">

              <h2 style="
                margin-bottom: 10px;
              ">
                Consolidated Invoice Statement
              </h2>

              <p>
                Please find the consolidated invoice statement shared by
                <strong>
                  ${companyName || "Company"}
                </strong>.
              </p>

              ${
                periodText
                  ? `
                    <p>
                      Period:
                      <strong>
                        ${periodText}
                      </strong>
                    </p>
                  `
                  : ""
              }

              <p>
                This link contains all invoices included in the selected period.
              </p>

              <p style="
                margin: 25px 0;
              ">
                <a
                  href="${shareUrl}"
                  style="
                    background:#2563eb;
                    color:#ffffff;
                    padding:12px 20px;
                    text-decoration:none;
                    border-radius:6px;
                    display:inline-block;
                  "
                >
                  View Invoice Statement
                </a>
              </p>

              <p>
                If the button above does not work,
                copy and paste the following link into your browser:
              </p>

              <p>
                <a href="${shareUrl}">
                  ${shareUrl}
                </a>
              </p>

              <hr />

              <p>
                Regards,
                <br />
                <strong>
                  ${companyName || "Company"}
                </strong>
              </p>

              ${
                companyEmail
                  ? `
                    <p>
                      Contact:
                      ${companyEmail}
                    </p>
                  `
                  : ""
              }

              <p style="
                color:#666;
                font-size:12px;
              ">
                Sent securely via Billey.
              </p>

            </div>
          `

        });

        console.log(
  "RESEND RESULT:",
  JSON.stringify(
    result,
    null,
    2
  )
);

      if (result.error) {

        console.log(
          "RESEND ERROR:",
          result.error
        );

        return res.json({

          success: false,

          error:
            result.error

        });

      }

      return res.json({

        success: true

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

app.get(
  "/getConsolidatedShare/:token",

  async (req, res) => {

    try {

      const { token } =
        req.params;

      const {
        data,
        error
      } = await supabase

        .from(
          "consolidated_share"
        )

        .select("*")

        .eq(
          "token",
          token
        )

        .single();

      if (error || !data) {

        return res.json({

          success: false,

          error:
            "Invalid token"

        });

      }

      return res.json({

        success: true,

        data

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


app.get(
  "/getConsolidatedInvoices/:token",

  async (req, res) => {

    try {

      const { token } =
        req.params;

      // =====================
      // GET SHARE ROW
      // =====================

      const {
        data: shareRow,
        error: shareError
      } = await supabase

        .from(
          "consolidated_share"
        )

        .select("*")

        .eq(
          "token",
          token
        )

        .single();

      if (
        shareError ||
        !shareRow
      ) {

        return res.json({

          success: false,

          error:
            "Invalid token"

        });

      }

      const invoiceIds =
        shareRow.invoice_ids || [];

      // =====================
      // GET INVOICES
      // =====================

      const {
        data: invoices,
        error: invoiceError
      } = await supabase

        .from("invoices")

        .select("*")

        .in(
          "invoice_id",
          invoiceIds
        );

      if (invoiceError) {

        return res.json({

          success: false,

          error:
            invoiceError.message

        });

      }

const {
  data: company,
  error: companyError
} = await supabase

  .from("company")

  .select("*")

  .eq(
    "company_code",
    shareRow.company_code
  )

  .single();

  if (companyError) {

  return res.json({

    success: false,

    error:
      companyError.message

  });

}

  
      

// =====================
// GET INVOICE ITEMS
// =====================

const {

  data: invoiceItems,

  error: itemsError

} = await supabase

  .from("invoice_items")

  .select("*")

  .in(

    "invoice_id",

    invoiceIds

  );

if (itemsError) {

  return res.json({

    success: false,

    error:
      itemsError.message

  });

}

const invoicesWithItems =

  (invoices || []).map(

    (invoice) => ({

      ...invoice,

      items:

        (invoiceItems || []).filter(

          item =>

            item.invoice_id ===

            invoice.invoice_id

        )

    })

  );

    return res.json({

  success: true,

  company,

  company_code:
    shareRow.company_code,

  invoices:
    invoicesWithItems

});
    }
    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);

// =========================
// GET TUTORIALS
// =========================

app.get(
  "/getTutorials",
  async (req, res) => {

    try {

      const {
        data,
        error
      } = await supabase

        .from("tutorials")

        .select("*")

        .eq(
          "is_active",
          true
        )

        .order(
          "sort_order",
          {
            ascending: true
          }
        );

      if (error) {

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      return res.json({

        success: true,

        data: data || []

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


app.post(
  "/saveTutorial",
  async (req, res) => {

    try {

      const tutorial =
        req.body;

      let result;

      if (tutorial.id) {

        // UPDATE

        result =
          await supabase

            .from("tutorials")

            .update({

              sort_order:
                tutorial.sort_order,

              title:
                tutorial.title,

              description:
                tutorial.description,

              youtube_url:
                tutorial.youtube_url,

              thumbnail_url:
                tutorial.thumbnail_url,

              duration_minutes:
                tutorial.duration_minutes,

              category:
                tutorial.category,

              is_active:
                tutorial.is_active

            })

            .eq(
              "id",
              tutorial.id
            )

            .select();

      } else {

        // INSERT

        result =
          await supabase

            .from("tutorials")

            .insert([{

              sort_order:
                tutorial.sort_order,

              title:
                tutorial.title,

              description:
                tutorial.description,

              youtube_url:
                tutorial.youtube_url,

              thumbnail_url:
                tutorial.thumbnail_url,

              duration_minutes:
                tutorial.duration_minutes,

              category:
                tutorial.category,

              is_active:
                tutorial.is_active ?? true

            }])

            .select();

      }

      if (result.error) {

        return res.json({

          success: false,

          error:
            result.error.message

        });

      }

      return res.json({

        success: true,

        data:
          result.data

      });

    }

    catch (err) {

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


// =========================
// GET CLIENT
// =========================

app.get(
  "/getClientMobile",
  async (req, res) => {

    try {

      const company_code =

        String(
          req.query.company_code || ""
        ).trim();

      const client_code =

        String(
          req.query.client_code || ""
        ).trim();

      console.log(
        "GET CLIENT API HIT"
      );

      console.log(
        "COMPANY CODE:",
        company_code
      );

      console.log(
        "CLIENT CODE:",
        client_code
      );

      if (!company_code) {

        return res.json({

          success: false,

          error:
            "company_code missing"

        });

      }

      if (!client_code) {

        return res.json({

          success: false,

          error:
            "client_code missing"

        });

      }

      // =========================
      // FETCH CLIENT
      // =========================

      const {
        data,
        error
      } = await supabase

        .from("client")

        .select("*")

        .eq(
          "company_code",
          company_code
        )

        .eq(
          "client_code",
          client_code
        )

        .single();

      console.log(
        "RAW CLIENT DATA:",
        data
      );

      if (error) {

        console.log(
          "GET CLIENT ERROR:",
          error
        );

        return res.json({

          success: false,

          error:
            error.message

        });

      }

      // =========================
      // FINAL RESPONSE
      // =========================

      const result = {

        company_code:
          String(
            data.company_code || ""
          ),

        client_code:
          String(
            data.client_code || ""
          ),

        client_name:
          data.client_name || "",

        gstin:
          data.gstin || "",

        billing_address:
          data.billing_address || "",

        bill_state:
          data.bill_state || "",

        bill_stateCode:
          String(
            data.bill_statecode || ""
          ),

        bill_pincode:
          String(
            data.bill_pincode || ""
          ),

        mobile:
          String(
            data.mobile || ""
          ),

        email:
          data.email || "",

        same_address:
          data.same_address || "",

        shipping_address:
          data.shipping_address || "",

        shipping_state:
          data.shipping_state || "",

        shipping_stateCode:
          String(
            data.shipping_statecode || ""
          ),

        shipping_pincode:
          String(
            data.shipping_pincode || ""
          ),

        createdAt:
          data.created_at || "",

        updatedAt:
          data.updated_at || "",

        is_active:
          data.is_active,

        contact_person:
          data.contact_person || "",

        credit_limit:
          data.credit_limit || 0,

        opening_balance:
          data.opening_balance || 0,

        remarks:
          data.remarks || "",

        credit_period:
          data.credit_period || 0

      };

      console.log(
        "FINAL CLIENT RESPONSE:",
        result
      );

      return res.json({

        success: true,

        data: result

      });

    }

    catch (err) {

      console.log(
        "GET CLIENT CATCH ERROR:",
        err
      );

      return res.json({

        success: false,

        error:
          err.message

      });

    }

  }
);


app.listen(
  process.env.PORT,
  () => {

    console.log(
      `Server running on port ${process.env.PORT}`
    );

  }
);