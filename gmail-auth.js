/**
 * Run this ONCE to authorize Billington to access Hargo's Gmail.
 * Usage: node gmail-auth.js
 *
 * Prerequisites:
 *   1. Gmail API enabled in GCP Console (raylo-production project)
 *   2. OAuth 2.0 client ID created (type: Desktop app)
 *   3. GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET added to .env
 */

require("dotenv").config({ path: require("path").join(__dirname, ".env"), override: true });

const { google } = require("googleapis");
const readline = require("readline");
const fs = require("fs");
const path = require("path");

const CLIENT_ID = process.env.GMAIL_CLIENT_ID;
const CLIENT_SECRET = process.env.GMAIL_CLIENT_SECRET;

if (!CLIENT_ID || !CLIENT_SECRET) {
  console.error("❌  GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET must be set in .env first.");
  process.exit(1);
}

const SCOPES = [
  "https://www.googleapis.com/auth/gmail.readonly",
];

const auth = new google.auth.OAuth2(CLIENT_ID, CLIENT_SECRET, "urn:ietf:wg:oauth:2.0:oob");

const authUrl = auth.generateAuthUrl({
  access_type: "offline",
  scope: SCOPES,
  prompt: "consent",
});

console.log("\n📧  Billington Gmail Authorization\n");
console.log("1. Open this URL in your browser (make sure you're signed in as hargobind@raylo.com):\n");
console.log("   " + authUrl);
console.log("\n2. Authorize the app, then copy the code shown.\n");

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

rl.question("Paste the code here: ", async (code) => {
  rl.close();
  try {
    const { tokens } = await auth.getToken(code.trim());

    if (!tokens.refresh_token) {
      console.error("\n❌  No refresh token received. Try revoking access at https://myaccount.google.com/permissions and run this script again.");
      process.exit(1);
    }

    // Append to .env
    const envPath = path.join(__dirname, ".env");
    const envContents = fs.readFileSync(envPath, "utf-8");
    const updated = envContents.includes("GMAIL_REFRESH_TOKEN=")
      ? envContents.replace(/GMAIL_REFRESH_TOKEN=.*/g, `GMAIL_REFRESH_TOKEN=${tokens.refresh_token}`)
      : envContents + `\nGMAIL_REFRESH_TOKEN=${tokens.refresh_token}\n`;
    fs.writeFileSync(envPath, updated);

    console.log("\n✅  Done! GMAIL_REFRESH_TOKEN saved to .env.");
    console.log("    Now restart Billington: npx pm2 restart bill-ling --update-env");
  } catch (err) {
    console.error("\n❌  Error exchanging code:", err.message);
    process.exit(1);
  }
});
