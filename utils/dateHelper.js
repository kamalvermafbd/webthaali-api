function formatDateByCountry(date, country) {

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


function getDateNowByCountry(country) {

  return formatDateByCountry(
    new Date(),
    country
  );

}


module.exports = {
  formatDateByCountry,
  getDateNowByCountry
};