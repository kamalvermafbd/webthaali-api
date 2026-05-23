require("dotenv").config();

const express = require("express");
const cors = require("cors");

const {
  createClient
} = require("@supabase/supabase-js");

const app = express();

app.use(cors());

app.use(express.json());

const supabase =
  createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
  );

app.get("/", (req, res) => {

  res.json({
    success: true,
    message: "WebThaali API Running"
  });

});

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

  const invoiceId =

  invoice.doc_type === "quotation"

    ? `QT-${companyCode}-${clientCode}-${fy}-${sequence}`

    : `${companyCode}-${clientCode}-${fy}-${sequence}`;

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

  await supabase

    .from("invoices")

    .update({

     invoice_number:
  invoice.invoiceNumber || "",

invoice_date:
  invoice.invoiceDate || "",

customer_name:
  invoice.customer?.name || "",

customer_mobile:
  invoice.customer?.mobile || "",

customer_gst:
  invoice.customer?.gstin || "",

      billingAddress:
        invoice.customer?.billingAddress || "",

      shippingAddress:
        invoice.customer?.shippingAddress || "",

      state:
        invoice.customer?.state || "",

      stateCode:
        invoice.customer?.stateCode || "",

      subtotal:
  invoice.totals?.taxableAmount || 0,

cgst:
  invoice.totals?.cgstTotal || 0,

sgst:
  invoice.totals?.sgstTotal || 0,

igst:
  invoice.totals?.igstTotal || 0,

  gstPercent:
  invoice.totals?.gstPercent || 0,

grandTotal:
  invoice.totals?.rounded || 0,

 paymentMode:
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

      shipping_stateCode:
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

      Quote_No:
        invoice.Quote_No || "",

      Quote_Dt:
        invoice.Quote_Dt || ""

    })

 .eq(
  "invoiceId",
      invoiceId
    );

} else {

  const {
  error: invoiceError
} = await supabase

    .from("invoices")

    .insert([{

invoice_id:
  invoiceId,

invoice_number:
  invoice.invoiceNumber || "",

invoice_date:
  invoice.invoiceDate || "",

customer_name:
  invoice.customer?.name || "",

customer_mobile:
  invoice.customer?.mobile || "",

customer_gst:
  invoice.customer?.gstin || "",

      billingAddress:
        invoice.customer?.billingAddress || "",

      shippingAddress:
        invoice.customer?.shippingAddress || "",

      state:
        invoice.customer?.state || "",

      stateCode:
        invoice.customer?.stateCode || "",

      subtotal:
  invoice.totals?.taxableAmount || 0,

cgst:
  invoice.totals?.cgstTotal || 0,

sgst:
  invoice.totals?.sgstTotal || 0,

igst:
  invoice.totals?.igstTotal || 0,

  gstPercent:
  invoice.totals?.gstPercent || 0,

grandTotal:
  invoice.totals?.rounded || 0,

  paymentMode:
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

      shipping_stateCode:
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

      Quote_No:
        invoice.Quote_No || "",

      Quote_Dt:
        invoice.Quote_Dt || ""

    }]);

    if (invoiceError) {

  console.log(
    "INVOICE ERROR:",
    invoiceError
  );

  return res.status(400).json({

    success: false,

    error:
      invoiceError.message

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

app.listen(
  process.env.PORT,
  () => {

    console.log(
      `Server running on port ${process.env.PORT}`
    );

  }
);