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
  invoice.Quote_Dt || null

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
    company?.statecode || ""

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
            row.quote_dt

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


app.listen(
  process.env.PORT,
  () => {

    console.log(
      `Server running on port ${process.env.PORT}`
    );

  }
);