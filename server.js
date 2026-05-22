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

      // =====================
      // INSERT INVOICE
      // =====================

      const {
        data: invoiceData,
        error: invoiceError
      } = await supabase

        .from("invoices")

        .insert([{

          invoice_id:
            invoice.invoiceId,

          invoice_number:
            invoice.invoiceNumber,

          invoice_date:
            invoice.invoiceDate,

          customer_name:
            invoice.customerName,

          customer_mobile:
            invoice.customerMobile,

          customer_gst:
            invoice.customerGST,

          billing_address:
            invoice.billingAddress,

          shipping_address:
            invoice.shippingAddress,

          state:
            invoice.state,

          state_code:
            invoice.stateCode,

          subtotal:
            invoice.subtotal,

          cgst:
            invoice.cgst,

          sgst:
            invoice.sgst,

          igst:
            invoice.igst,

          gst_percent:
            invoice.gstPercent,

          grand_total:
            invoice.grandTotal,

          payment_mode:
            invoice.paymentMode,

          notes:
            invoice.notes,

          terms:
            invoice.terms,

          company_code:
            invoice.company_code,

          client_code:
            invoice.client_code,

          billing_pincode:
            invoice.billing_pincode,

          shipping_state:
            invoice.shipping_state,

          shipping_state_code:
            invoice.shipping_stateCode,

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

          quote_no:
            invoice.Quote_No,

          quote_dt:
            invoice.Quote_Dt

        }])

        .select();

      if (invoiceError) {

        return res.status(400).json({

          success: false,

          error:
            invoiceError.message

        });

      }

      // =====================
      // INSERT ITEMS
      // =====================

      if (
        invoice.items &&
        invoice.items.length > 0
      ) {

        const itemsData =
          invoice.items.map(
            (item) => ({

              invoice_id:
                invoice.invoiceId,

              item_name:
                item.itemName,

              hsn:
                item.hsn,

              qty:
                item.qty,

              rate:
                item.rate,

              gst_percent:
                item.gstPercent,

              taxable_amount:
                item.taxableAmount,

              cgst:
                item.cgst,

              sgst:
                item.sgst,

              igst:
                item.igst,

              total:
                item.total,

              disc_percent:
                item.Disc_Percent,

              disc_amt:
                item.Disc_Amt,

              source:
                item.source

            })
          );

        const {
          error: itemsError
        } = await supabase

          .from("invoice_items")

          .insert(itemsData);

        if (itemsError) {

          return res.status(400).json({

            success: false,

            error:
              itemsError.message

          });

        }

      }

      return res.json({

        success: true,

        data: invoiceData

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