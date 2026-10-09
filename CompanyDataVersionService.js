
const incrementCompanyDataVersion = async ({
  supabase,
  company_code,
  tally_owner,
}) => {
  const companyCode = String(company_code || "").trim();
  const tallyOwner = String(tally_owner || "").trim().toUpperCase();

  if (!companyCode) {
    throw new Error("company_code is required for version increment");
  }

  if (!["USER", "CA"].includes(tallyOwner)) {
    throw new Error(`Invalid tally_owner: ${tallyOwner}`);
  }

  const { data, error } = await supabase.rpc(
    "increment_company_data_version",
    {
      p_company_code: companyCode,
      p_tally_owner: tallyOwner,
    }
  );

  if (error) {
    throw new Error(
      `Company data version increment failed: ${error.message}`
    );
  }

  return data;
};

module.exports = {
  incrementCompanyDataVersion,
};
