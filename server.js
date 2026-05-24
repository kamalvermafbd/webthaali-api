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
  invoice.invoiceNumber || "",

invoice_date:
  invoice.invoiceDate || "",

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
  invoice.Quote_No || "",

quote_dt:
  invoice.Quote_Dt || null,

  

    })

 .eq(
  "invoice_id",
  invoiceId
);

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
  invoice.Quote_No || "",

quote_dt:
  invoice.Quote_Dt || null


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
  company?.pincode || ""  

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
  row.eway_bill_no

        }));

      return res.json({

        success: true,

        data: result

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
            row.credit_period || 0

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

      const duplicateGST =

        (allClients || []).find((row) => {

          return (

            String(
              row.gstin || ""
            )
              .trim()
              .toUpperCase()

            ===

            String(
              body.gstin || ""
            )
              .trim()
              .toUpperCase()

          );

        });

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
          )

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

app.listen(
  process.env.PORT,
  () => {

    console.log(
      `Server running on port ${process.env.PORT}`
    );

  }
);