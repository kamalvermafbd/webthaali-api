const { google } = require("googleapis");

async function sendGmailEmail({
  gmail_email,
  gmail_refresh_token,
  to,
  subject,
  message,
}) {
  if (!gmail_email) {
    throw new Error("Gmail sender email is missing");
  }

  if (!gmail_refresh_token) {
    throw new Error("Gmail refresh token is missing");
  }

  if (!to) {
    throw new Error("Recipient email is required");
  }

  const oauth2Client = new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    process.env.GOOGLE_REDIRECT_URI
  );

  oauth2Client.setCredentials({
    refresh_token: gmail_refresh_token,
  });

  const gmail = google.gmail({
    version: "v1",
    auth: oauth2Client,
  });

  const rawMessage = [
    `From: ${gmail_email}`,
    `To: ${to}`,
    `Subject: ${subject}`,
    "MIME-Version: 1.0",
    "Content-Type: text/plain; charset=UTF-8",
    "",
    message,
  ].join("\r\n");

  const encodedMessage = Buffer.from(rawMessage)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");

  const result = await gmail.users.messages.send({
    userId: "me",
    requestBody: {
      raw: encodedMessage,
    },
  });

  return {
    success: true,
    message_id: result.data.id,
  };
}

module.exports = {
  sendGmailEmail,
};