module.exports = ({
  company,
  stockName,
  unit,
  hsn,
  gstRate
}) => {

  const halfRate =
    Number(gstRate) / 2;

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

          <STOCKITEM NAME="${stockName}" ACTION="Create">

            <NAME>${stockName}</NAME>

            <PARENT></PARENT>

            <CATEGORY>&#4; Not Applicable</CATEGORY>

            <GSTAPPLICABLE>&#4; Applicable</GSTAPPLICABLE>

            <GSTTYPEOFSUPPLY>Goods</GSTTYPEOFSUPPLY>

            <BASEUNITS>${unit}</BASEUNITS>

            <VATBASEUNIT>${unit}</VATBASEUNIT>

            <GSTDETAILS.LIST>

              <APPLICABLEFROM>20260401</APPLICABLEFROM>

              <TAXABILITY>Taxable</TAXABILITY>

              <SRCOFGSTDETAILS>
                Specify Details Here
              </SRCOFGSTDETAILS>

              <STATEWISEDETAILS.LIST>

                <STATENAME>&#4; Any</STATENAME>

                <RATEDETAILS.LIST>

                  <GSTRATEDUTYHEAD>CGST</GSTRATEDUTYHEAD>

                  <GSTRATEVALUATIONTYPE>
                    Based on Value
                  </GSTRATEVALUATIONTYPE>

                  <GSTRATE>${halfRate}</GSTRATE>

                </RATEDETAILS.LIST>

                <RATEDETAILS.LIST>

                  <GSTRATEDUTYHEAD>
                    SGST/UTGST
                  </GSTRATEDUTYHEAD>

                  <GSTRATEVALUATIONTYPE>
                    Based on Value
                  </GSTRATEVALUATIONTYPE>

                  <GSTRATE>${halfRate}</GSTRATE>

                </RATEDETAILS.LIST>

                <RATEDETAILS.LIST>

                  <GSTRATEDUTYHEAD>IGST</GSTRATEDUTYHEAD>

                  <GSTRATEVALUATIONTYPE>
                    Based on Value
                  </GSTRATEVALUATIONTYPE>

                  <GSTRATE>${gstRate}</GSTRATE>

                </RATEDETAILS.LIST>

              </STATEWISEDETAILS.LIST>

            </GSTDETAILS.LIST>

            <HSNDETAILS.LIST>

              <APPLICABLEFROM>20260401</APPLICABLEFROM>

              <HSNCODE>${hsn}</HSNCODE>

              <SRCOFHSNDETAILS>
                Specify Details Here
              </SRCOFHSNDETAILS>

            </HSNDETAILS.LIST>

            <LANGUAGENAME.LIST>

              <NAME.LIST TYPE="String">

                <NAME>${stockName}</NAME>

              </NAME.LIST>

            </LANGUAGENAME.LIST>

          </STOCKITEM>

        </TALLYMESSAGE>

      </REQUESTDATA>

    </IMPORTDATA>

  </BODY>

</ENVELOPE>

`;
};