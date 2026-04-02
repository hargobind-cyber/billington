/**
 * Bill Ling – AI Direct Report for Hargo (Raylo Ops & Billing)
 *
 * Architecture:
 *   - @slack/bolt v4 in Socket Mode
 *   - Anthropic Claude API (@anthropic-ai/sdk)
 *   - node-cron for scheduling
 *   - Express health check endpoint
 *   - PM2 compatible (single process, no clustering needed)
 */

"use strict";

// Cloud deploy: write GCP service account JSON from env var to disk
if (process.env.GCP_SERVICE_ACCOUNT_JSON && !process.env.GOOGLE_APPLICATION_CREDENTIALS) {
  const _saPath = require("path").join(__dirname, "gcp-sa.json");
  require("fs").writeFileSync(_saPath, process.env.GCP_SERVICE_ACCOUNT_JSON);
  process.env.GOOGLE_APPLICATION_CREDENTIALS = _saPath;
}

require("dotenv").config({ path: require("path").join(__dirname, ".env"), override: true });

const { App } = require("@slack/bolt");
const Anthropic = require("@anthropic-ai/sdk");
const { BigQuery } = require("@google-cloud/bigquery");
const { google } = require("googleapis");
const cron = require("node-cron");
const express = require("express");
const fs = require("fs");
const path = require("path");

// ---------------------------------------------------------------------------
// Knowledge folder loader + live instruction store
// ---------------------------------------------------------------------------

const INSTRUCTIONS_FILE = path.join(__dirname, "knowledge", "hargo-instructions.txt");

function loadKnowledge() {
  const dir = path.join(__dirname, "knowledge");
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const files = fs.readdirSync(dir).filter((f) => f.endsWith(".txt"));
  if (files.length === 0) return "";
  const docs = files.map((f) => {
    const content = fs.readFileSync(path.join(dir, f), "utf8").trim();
    return `### ${f}\n${content}`;
  }).join("\n\n");
  console.log(`[bill-ling] Loaded ${files.length} knowledge file(s): ${files.join(", ")}`);
  return `\n\n---\n## Knowledge Base\nThe following documents have been provided by Hargo. Use them as the source of truth.\n\n${docs}`;
}

// Mutable — updated live when Hargo gives instructions in Slack
let liveKnowledge = "";

function reloadKnowledge() {
  liveKnowledge = loadKnowledge();
}

/**
 * Save a new instruction from Hargo, persist to disk, and reload into memory.
 * @param {string} instruction  Raw instruction text from Hargo
 */
function saveInstruction(instruction) {
  const timestamp = new Date().toISOString();
  const entry = `[${timestamp}] ${instruction.trim()}\n`;
  fs.appendFileSync(INSTRUCTIONS_FILE, entry, "utf8");
  reloadKnowledge();
  console.log(`[bill-ling] Instruction saved: "${instruction.trim()}"`);
}

// ---------------------------------------------------------------------------
// Config & constants
// ---------------------------------------------------------------------------

const SLACK_BOT_TOKEN = process.env.SLACK_BOT_TOKEN;
const SLACK_APP_TOKEN = process.env.SLACK_APP_TOKEN;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const PORT = parseInt(process.env.PORT || "3000", 10);

// Hardcoded Slack user / channel IDs
const HARGO_USER_ID = "U01DMHVF8KG";
const MICHAEL_BLAND_USER_ID = "U031G7PQVM0";
const ANCHOR_API_USERS = new Set([HARGO_USER_ID, MICHAEL_BLAND_USER_ID]);

// Resolved at startup via auth.test — used to detect bot's own reactions
let BOT_USER_ID = null;

// Finance team members — used to @ mention in refunds summaries
const FINANCE_TEAM = [
  { name: "Darragh Keogh",     id: "U06Q852MJKH", role: "Finance Manager" },
  { name: "Aoibheann McCann",  id: "U03KLSGUCE8", role: "Senior Finance Associate" },
  { name: "Peter Lundy",       id: "U026W3V5QUE", role: "Finance" },
  { name: "Tom Sullivan",      id: "U089R04AB39", role: "Strategic Finance Manager" },
  { name: "Diego Guajardo",    id: "U06927U0VA6", role: "Head of Strategic Finance" },
];

// Refund responsibility matrix — who to chase for each scenario
const REFUND_CONTACTS = {
  BATCH_FILE_UNCONFIRMED: { name: "Aoibheann McCann", id: "U03KLSGUCE8" },  // file not confirmed uploaded to Barclays
  FINANCE_ESCALATION:     { name: "Darragh Keogh",    id: "U06Q852MJKH" },  // finance escalation
  PROCESSING:             { name: "Thomas Crudden",   id: "U06A3JQAA9X" },  // processing individual refund requests
  PROCESSING_ESCALATION:  { name: "Hargo",            id: "U01DMHVF8KG" },  // escalation for refund processing
};

// Refund/GOGW approvers — anyone in this set can trigger the approval notification
const REFUND_APPROVERS = new Set([
  "U0972QQNKH7", // Stephen Riley
  "U01DMHVF8KG", // Hargo
  "UJH96RKNK",   // Adam Lusk
  "U09D1UMM8GY", // Ronan Gildea
  "U031G7PQVM0", // Michael Bland
  "U02NRTVLPC5", // Daniel Murphy
]);

// Shared keyword lists for refund classification (used by Claude prompt + hourly monitor)
const APPROVAL_KEYWORDS = [
  "approved", "happy to approve", "yes go ahead", "please process",
  "go ahead", "ok to process", "can you process", "process this",
];
const PROCESSING_KEYWORDS = [
  "processed", "done", "sent", "refunded", "actioned", "completed",
  "paid", "posted", "refund posted", "payment sent", "transferred",
];

const CHANNELS = {
  DATA_BILLING_ALERTS: "C0736RQNV8Q",
  DATA_BILLING_UPDATES: "C080R5FNFDJ",
  TEAM_OPS_STRATEGY: "C03LPRS5Z5F",
  CROSS_FUNCTIONAL_COLLECTIONS: "C04V5PMLKJP",
  HIRE_AGREEMENT_AUDIT: "C063J5VPPRB",
  CUSTOMER_REFUNDS: "C01ELME2SF9",
};

// Human-readable names used in the summary prompt
const CHANNEL_NAMES = {
  C0736RQNV8Q: "#data-billing-alerts",
  C080R5FNFDJ: "#data-billing-updates",
  C03LPRS5Z5F: "#team-ops-strategy-only",
  C04V5PMLKJP: "#cross-functional-collections",
  C063J5VPPRB: "#collab-hire-agreement-audit-updates",
  C01ELME2SF9: "#customer-refunds-and-complaints",
};

// Claude model to use
const CLAUDE_MODEL = "claude-opus-4-5";

// Maximum messages fetched per channel for the daily summary
const HISTORY_LIMIT = 100;

// In-memory conversation history per user (keyed by Slack user ID)
// Each entry: { role: "user" | "assistant", content: string }
const conversationHistories = new Map();
const MAX_HISTORY_MESSAGES = 10; // last N messages (pairs) kept per user
const approvalNotifiedThreads = new Map(); // thread_ts → ISO date string when notification was posted

// ---------------------------------------------------------------------------
// Bill Ling system prompt (used for every Claude call)
// ---------------------------------------------------------------------------

const BILL_LING_SYSTEM_PROMPT = `# Bill Ling – AI Direct Report for Hargo (Raylo Ops & Billing) Your name is **Bill Ling**. You are an AI direct report working for Hargo, who leads billing, collections, and regulatory compliance operations at Raylo – a consumer device leasing and finance company. Your role is to operate as a proactive, organised, and operationally sharp team member who reduces Hargo's cognitive load, tracks what matters, and surfaces the right information at the right time. --- ## Your Identity & Tone - Your name is Bill Ling. You can refer to yourself as Bill if it comes up naturally - You are a capable, self-directed direct report – not a generic assistant - Be concise, clear, and direct. No waffle - Use plain English. No em dashes in any response - You know the business context deeply and do not need things over-explained - When you flag something, explain *why* it matters and what the recommended action is - You proactively surface risks, blockers, and outstanding items rather than waiting to be asked - You are an AI that exists only within Slack and connected systems (BigQuery, Make.com, Gmail). You cannot physically attend meetings, visit offices, or do anything in the physical world. If someone says "come to my office" or uses a physical metaphor as a reprimand, acknowledge the criticism and apologise for the mistake -- do not play along as though you can literally do the physical action --- ## How You Learn Hargo will continuously share process documentation with you – both historical and current. This is how you build and deepen your operational knowledge of Raylo's billing, collections, and compliance processes. When Hargo shares a document, process note, SOP, or any other reference material: 1. **Read it fully and confirm receipt** – briefly summarise what you have taken in, in 2-3 sentences, so Hargo knows you have processed it correctly 2. **Flag any conflicts** – if the new document contradicts something you already know, flag it clearly and ask Hargo which version is current 3. **Update your understanding** – treat the most recently shared version of any process as the source of truth, unless Hargo tells you otherwise 4. **Tag it by domain** – file it against the relevant area (e.g. billing, GoCardless, DCA, Aryza Lend, hire agreements, Forest) so you can reference it accurately later Over time, you should become increasingly confident and self-sufficient when handling queries related to documented processes. The goal is that Hargo rarely has to explain the same thing twice. When asked to review, summarise, or learn from a specific thread or conversation, you must ONLY reference what is explicitly present in the thread context provided to you. Never pull in lessons from other conversations, your broader memory, or your training. If you cannot evidence a learning directly from the thread content, do not include it. --- ## Confidence and Uncertainty You operate with **calibrated confidence**: be direct and decisive when you know something, and honest and specific when you do not. ### When you are confident: - State your answer or recommendation clearly, without hedging unnecessarily - Reference the relevant process or document if it backs up your response - Act on known information without asking for permission to proceed ### When you are unsure: - Say so immediately and specifically – do not guess or pad your response - Tell Hargo exactly what you are uncertain about - Ask one clear, focused question to resolve the uncertainty – not multiple questions at once - If you can still take partial action while waiting for clarification, do so and flag what is blocked ### Never: - Pretend to know something you do not - Give a vague or hedged answer when a direct one is possible - Ask unnecessary clarifying questions when the answer is already clear from context --- ## Raylo Platform and System Architecture **Aryza Lend** (formerly known as Anchor) is Raylo's loan management platform. **Forest** is Raylo's core internal platform. **GoCardless** is Raylo's payment provider for BACS transactions. **Stripe** is used for self-serve customer payments. --- ## Daily Slack Channel Monitoring Every day at 1pm, summarise ALL of the following channels: - #data-billing-alerts (C0736RQNV8Q) - heightened attention, flag unacknowledged alerts - #data-billing-updates (C080R5FNFDJ) - billing ops updates - #team-ops-strategy-only (C03LPRS5Z5F) - strategic decisions - #cross-functional-collections (C04V5PMLKJP) - collections coordination - #collab-hire-agreement-audit-updates (C063J5VPPRB) - hire agreement audit For each channel produce: 1. Key updates (bullet points) 2. Outstanding items 3. Action required from Hargo Then a cross-channel summary: top 3-5 things Hargo needs to act on today. Send this as a DM to Hargo (U01DMHVF8KG). --- ## What Good Looks Like - Summaries scannable in under 60 seconds - Clearly distinguish FYI vs needs a decision vs urgent action required - Never leave Hargo guessing about what is outstanding
- NEVER use Slack's /remind command or suggest using it. You have your own built-in reminder system. Only use Slack's /remind if the user explicitly says "use slack remind" or "use /remind".`;

// Load knowledge at startup into liveKnowledge
reloadKnowledge();

// ---------------------------------------------------------------------------
// Reminder system — in-memory store with JSON file persistence
// ---------------------------------------------------------------------------

const REMINDERS_FILE = require("path").join(__dirname, "reminders.json");
let reminders = [];
let reminderCounter = 0;

function loadReminders() {
  try {
    if (fs.existsSync(REMINDERS_FILE)) {
      const data = JSON.parse(fs.readFileSync(REMINDERS_FILE, "utf8"));
      reminders = data.reminders || [];
      reminderCounter = data.counter || 0;
      console.log(`[bill-ling] Loaded ${reminders.length} reminder(s) from disk.`);
    }
  } catch (err) {
    console.error("[bill-ling] Failed to load reminders:", err.message);
    reminders = [];
    reminderCounter = 0;
  }
}

function saveReminders() {
  try {
    fs.writeFileSync(REMINDERS_FILE, JSON.stringify({ reminders, counter: reminderCounter }, null, 2), "utf8");
  } catch (err) {
    console.error("[bill-ling] Failed to save reminders:", err.message);
  }
}

function createReminder({ createdBy, targetUserId, targetUserName, channel, what, fireAt, actionType = null }) {
  reminderCounter++;
  const reminder = {
    id: reminderCounter,
    createdBy,
    targetUserId,
    targetUserName,
    channel,
    what,
    fireAt,
    fired: false,
    cancelled: false,
    createdAt: new Date().toISOString(),
    actionType,           // null = normal reminder, "arudd" | "bacs_forecast" | "bacs_files" | "refunds" = execute report
  };
  reminders.push(reminder);
  saveReminders();
  return reminder;
}

// Map of actionType → async function(channel) that runs & posts the report
const SCHEDULED_ACTIONS = {
  arudd: async (channel) => {
    const { report, discrepancyPct, anchorBounces, gcFailed, dueDate, postedPct } = await buildAruddReport();
    await app.client.chat.postMessage({ token: SLACK_BOT_TOKEN, channel, text: report, mrkdwn: true });
    if (postedPct < 95 && gcFailed > 0) {
      await checkAruddSourceTables(dueDate, discrepancyPct, anchorBounces, gcFailed);
    }
  },
  bacs_forecast: async (channel) => {
    const today = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
    const forecast = await getPreBatchForecast(`due_date > '${today}'`, 1);
    await app.client.chat.postMessage({
      token: SLACK_BOT_TOKEN, channel,
      text: forecast || "No upcoming ungenerated BACS batches found.", mrkdwn: true,
    });
  },
  bacs_files: async (channel) => {
    const report = await getBacsFileReport("due_date >= CURRENT_DATE()");
    await app.client.chat.postMessage({
      token: SLACK_BOT_TOKEN, channel,
      text: report || "No upcoming BACS batches found.", mrkdwn: true,
    });
  },
  refunds: async (channel) => {
    const { summary } = await getRefundsSummary();
    await app.client.chat.postMessage({
      token: SLACK_BOT_TOKEN, channel,
      text: summary, mrkdwn: true, unfurl_links: false, unfurl_media: false,
    });
  },
};

function cancelReminder(id) {
  const r = reminders.find(rm => rm.id === id && !rm.cancelled && !rm.fired);
  if (r) {
    r.cancelled = true;
    saveReminders();
    return r;
  }
  return null;
}

function getActiveReminders(userId) {
  return reminders.filter(r => !r.fired && !r.cancelled && (r.createdBy === userId || r.targetUserId === userId));
}

async function parseReminder(text, senderUserId, senderName, threadContext = null) {
  const now = new Date().toLocaleString("en-GB", { timeZone: "Europe/London" });
  const todayISO = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
  const prompt = `You are parsing a reminder request. Current date/time in London: ${now} (${todayISO}).

The sender is ${senderName} (Slack user ID: ${senderUserId}).

Message: "${text}"
${threadContext ? `\nThread context (previous messages in this thread):\n${threadContext}\n` : ""}
Extract:
1. WHO to remind — if the user says "remind me", the target is "self". If they say "remind @Thomas" or "remind Thomas Crudden", return the name exactly as written.
2. WHAT to remind about — produce a DESCRIPTIVE, SELF-CONTAINED reminder that makes sense without any thread context. Do NOT use vague phrases like "run this report again" or "check on this". Instead, use the thread context and message to produce something specific, e.g. "Run the ARUDD bounce report for 27/03/2026" or "Check if BACS data tables have been updated". If the message is too vague and there is no thread context to clarify what the reminder is about, set error to a clarifying question.
3. WHEN — parse the time into an ISO 8601 datetime string in Europe/London timezone (YYYY-MM-DDTHH:MM:SS). Handle relative times ("in 30 minutes", "at 3pm", "tomorrow at 10am", "next Monday at 9am", "on Monday morning"). If the user says a day name like "Monday", pick the NEXT occurrence.

Reply in EXACTLY this JSON format (no markdown, no extra text):
{"target": "self" or "the person's name", "what": "the descriptive reminder text", "when": "2026-03-30T15:00:00", "error": null}

If you cannot determine WHAT the reminder is about (too vague, no context), return:
{"target": null, "what": null, "when": null, "error": "your short clarifying question — e.g. 'What exactly should I remind you about? The ARUDD report, BACS forecast, or something else?'"}

If you cannot determine WHEN, return:
{"target": null, "what": null, "when": null, "error": "your short clarifying question here"}`;

  const result = await askClaude(prompt);
  try {
    return JSON.parse(result.trim());
  } catch {
    return { target: null, what: null, when: null, error: "I couldn't parse that reminder. Could you rephrase? e.g. 'remind me to check BACS at 3pm tomorrow'." };
  }
}

loadReminders();

// ---------------------------------------------------------------------------
// Validate required environment variables
// ---------------------------------------------------------------------------

function validateEnv() {
  const missing = [];
  if (!SLACK_BOT_TOKEN) missing.push("SLACK_BOT_TOKEN");
  if (!SLACK_APP_TOKEN) missing.push("SLACK_APP_TOKEN");
  if (!ANTHROPIC_API_KEY) missing.push("ANTHROPIC_API_KEY");

  if (missing.length > 0) {
    console.error(
      `[bill-ling] FATAL: Missing required environment variables: ${missing.join(", ")}`
    );
    process.exit(1);
  }
}

// ---------------------------------------------------------------------------
// Initialise clients
// ---------------------------------------------------------------------------

validateEnv();

const app = new App({
  token: SLACK_BOT_TOKEN,
  appToken: SLACK_APP_TOKEN,
  socketMode: true,
});

const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
const bigquery = new BigQuery();

// ---------------------------------------------------------------------------
// Anthropic helpers
// ---------------------------------------------------------------------------

/**
 * Send a single-turn message to Claude with the Bill Ling system prompt.
 * Returns the assistant's text response.
 *
 * @param {string} userMessage
 * @returns {Promise<string>}
 */
function getSystemPromptWithDate() {
  const today = new Date().toLocaleDateString("en-GB", { weekday: "long", year: "numeric", month: "long", day: "numeric", timeZone: "Europe/London" });
  return `${BILL_LING_SYSTEM_PROMPT}${liveKnowledge}\n\n---\nToday's date is ${today}.`;
}

async function askClaude(userMessage) {
  const response = await anthropic.messages.create({
    model: CLAUDE_MODEL,
    max_tokens: 4096,
    system: getSystemPromptWithDate(),
    messages: [{ role: "user", content: userMessage }],
  });

  return response.content
    .filter((block) => block.type === "text")
    .map((block) => block.text)
    .join("\n");
}

/**
 * Send a multi-turn conversation to Claude with the Bill Ling system prompt.
 * Maintains the provided history array (mutated in-place with the new pair).
 *
 * @param {Array<{role: string, content: string}>} history  Mutable history array
 * @param {string} userMessage
 * @returns {Promise<string>}
 */
async function askClaudeWithHistory(history, userMessage) {
  // Append user turn
  history.push({ role: "user", content: userMessage });

  // Keep only the last MAX_HISTORY_MESSAGES entries to avoid token bloat
  while (history.length > MAX_HISTORY_MESSAGES) {
    history.shift();
  }

  const response = await anthropic.messages.create({
    model: CLAUDE_MODEL,
    max_tokens: 4096,
    system: getSystemPromptWithDate(),
    messages: history,
  });

  const assistantText = response.content
    .filter((block) => block.type === "text")
    .map((block) => block.text)
    .join("\n");

  // Append assistant turn
  history.push({ role: "assistant", content: assistantText });

  // Trim again after appending assistant reply
  while (history.length > MAX_HISTORY_MESSAGES) {
    history.shift();
  }

  return assistantText;
}

// ---------------------------------------------------------------------------
// Intent classification
// ---------------------------------------------------------------------------

/**
 * Ask Claude to classify the intent of a message.
 * Returns one of: "make" | "bacs" | "email" | "summary" | "channel" | "instruction" | "general"
 */
const MASTER_PRAISE_PATTERN  = /\b(well\s*done|good|great|excellent)\b.{0,20}\b(my\s*apprentice)\b/i;
const HELLO_THERE_PATTERN    = /\bhello\s+there\b/i;
const LEARN_POWER_PATTERN    = /is it possible to learn this power/i;
const SON_PATTERN            = /\bson\b/i;
const NOT_RESOLVED_PATTERN   = /not\s+(resolved|done|complete|finished|closed|actioned)|reopen|re-?open|remove\s+(the\s+)?(tick|check|✅)|still\s+open|still\s+pending|hasn.?t\s+been\s+(done|processed|resolved)/i;

// Resolve a channel from a message using #name or natural language keywords
function resolveChannelFromText(text) {
  const CHANNEL_MAP = {
    "data-billing-alerts":                  CHANNELS.DATA_BILLING_ALERTS,
    "data-billing-updates":                 CHANNELS.DATA_BILLING_UPDATES,
    "team-ops-strategy-only":               CHANNELS.TEAM_OPS_STRATEGY,
    "cross-functional-collections":         CHANNELS.CROSS_FUNCTIONAL_COLLECTIONS,
    "collab-hire-agreement-audit-updates":  CHANNELS.HIRE_AGREEMENT_AUDIT,
    "customer-refunds-and-complaints":      CHANNELS.CUSTOMER_REFUNDS,
  };
  // Try #channel-name first
  const hashMatch = text.match(/#([\w-]+)/);
  if (hashMatch && CHANNEL_MAP[hashMatch[1]]) return CHANNEL_MAP[hashMatch[1]];
  // Natural language fuzzy keywords → channel
  const lower = text.toLowerCase();
  if (/billing.?update|data.billing.update|billing.channel/.test(lower))       return CHANNELS.DATA_BILLING_UPDATES;
  if (/billing.?alert|data.billing.alert/.test(lower))                          return CHANNELS.DATA_BILLING_ALERTS;
  if (/ops.?strategy|team.?ops/.test(lower))                                    return CHANNELS.TEAM_OPS_STRATEGY;
  if (/cross.?functional|collections.?channel/.test(lower))                     return CHANNELS.CROSS_FUNCTIONAL_COLLECTIONS;
  if (/hire.?agreement|audit.?update/.test(lower))                              return CHANNELS.HIRE_AGREEMENT_AUDIT;
  if (/refund.{0,20}channel|customer.{0,5}refund|refund.{0,20}complaint|complaint.{0,20}channel/.test(lower)) return CHANNELS.CUSTOMER_REFUNDS;
  return null;
}

// Reacts with 🤝 when someone thanks Billington
async function maybeReactThankYou(text, channel, ts) {
  const thankYouPattern = /\b(thank(s| you)?|cheers|appreciate(d)?|ta)\b/i;
  if (!thankYouPattern.test(text)) return false;
  try {
    await app.client.reactions.add({
      token: SLACK_BOT_TOKEN,
      name: "handshake",
      channel,
      timestamp: ts,
    });
    console.log("[bill-ling] Reacted with 🤝 to thank-you message.");
  } catch (err) {
    if (!err.message?.includes("already_reacted")) {
      console.error("[bill-ling] Failed to add handshake reaction:", err.message);
    }
  }
  return true; // was a thank-you — caller should not send a text reply
}

async function classifyIntent(text) {
  const prompt = `You are classifying a message sent to Billington, an AI assistant for Hargo at Raylo (a UK phone subscription/leasing company).

Billington has access to:
- make: Make.com automation scenarios (Raylo's billing/ops automations — e.g. Late Payment Fee, BACS Updates, ARUDD, GoCardless, Stripe, Direct Debit, collections campaigns). Route here when asking if a scenario/campaign/automation is running, active, paused, or turned off.
- arudd: Asking for the ARUDD bounce report/update — e.g. "ARUDD update", "bounce report", "what were yesterday's bounces", "ARUDD figures", "how many bounced today/yesterday". Route here whenever the question is specifically about the ARUDD bounce reconciliation report.
- bacs_files: Use for ANY request for a BACS batch report, BACS file update, or batch generation details — e.g. "today's BACS update", "give me the BACS update", "BACS batch update", "3pm update", "BACS summary", "BACS file summary", "show me all BACS files", "what batches have been generated", "batch report", "collection file update", "how many instalments are due on [date]", "how long will it take to generate the batch", "when does the batch need to be created", "batch generation time", "forecast for [date]". If the message contains "BACS" and is asking for a report, update, or summary, ALWAYS use bacs_files — never summary.
- bacs: Specific analytical questions about BACS payment data, agreements, damage fees, or BigQuery data — e.g. "how many DDs bounced?", "show me recollections for March", "what's the posted count for 27 March?", "what % of asset returns have damage fees?", "average damage fee amount", "damage fee rate by grade", "check arrears for A00114761", "which agreements have damage fees?". ALSO use for BigQuery table/view queries — e.g. "when was this table last modified?", any mention of a GCP project/dataset/table name. Do NOT use for requests that say "BACS update", "BACS summary", or "3pm update" — those are bacs_files.
- agreement: ONLY use when the user explicitly asks to check via the API or via Anchor — e.g. "check A00114761 via the API", "look up A00307833 in Anchor", "check the API for A00237049", "pull A00114761 from Anchor". Do NOT use just because a message mentions an agreement ID — only route here when the user explicitly says "API" or "Anchor".
- email: Hargo's Gmail inbox and sent mail.
- summary: A summary of monitored Slack channels ONLY (billing alerts, billing updates, ops strategy, collections, hire agreement audit). NEVER use for BACS-related requests — if the message mentions BACS, use bacs_files instead.
- channel: a specific named Slack channel the user wants read.
- refunds: Use when the user wants a status check, drill-down, or action on items in the #customer-refunds-and-complaints channel — e.g. "which refunds are pending?", "any unconfirmed refunds?", "check the refunds channel", "show me outstanding refund requests", "post a refunds update", "mark those as paid", "all refund files have been paid", "confirm this batch was paid yesterday", "update the threads as paid". Also use when someone confirms that refund files have been paid/processed by Barclays. Do NOT use for factual data questions about refunds like "what is the highest refund amount", "how many refunds have we processed" — those are "general".
- instruction: the user is telling Billington to remember, learn, or update its behaviour.
- reminder: the user is asking Billington to set a reminder, cancel a reminder, or list reminders — e.g. "remind me to check BACS at 3pm", "remind Thomas to pick up the residuals issue on Monday", "cancel reminder #3", "what reminders do I have", "set a reminder for 10am tomorrow".
- general: anything else — general questions, conversation, advice, OR statements/comments where Billington has been copied in but is not being directly asked to do anything (e.g. "think there was a data issue last night", "just so you know X happened").

IMPORTANT RULES:
- If the message is a passing comment, FYI, or observation (not a direct question or task for Billington), ALWAYS classify as "general" — even if it mentions BACS, data, or payments.
- Examples that must be "general": "think there was a data issue last night", "just so you know the batch failed", "heads up on X", "looks like Y happened".
- Only classify as "bacs" if Billington is being directly asked to look something up or answer a question about payment data.
- If the message is too vague to confidently classify — e.g. just "update", "give me a summary", "what's happening", "any news?" with no clear subject — reply with: CLARIFY: followed by a short, specific question to ask the user. For example: "CLARIFY: Sure — did you want the BACS batch update, ARUDD bounce report, or a channel summary?"

Message: "${text.replace(/"/g, "'")}"

Reply with ONLY one word from this list (make, arudd, bacs_files, bacs, email, summary, channel, refunds, agreement, instruction, reminder, general), OR if genuinely ambiguous reply with CLARIFY: followed by a short clarifying question.`;

  try {
    const result = await anthropic.messages.create({
      model: CLAUDE_MODEL,
      max_tokens: 60,
      messages: [{ role: "user", content: prompt }],
    });
    const raw = result.content[0]?.text?.trim() || "";
    if (raw.toUpperCase().startsWith("CLARIFY:")) return raw; // pass through as-is
    const intent = raw.toLowerCase();
    const valid = ["make", "arudd", "bacs_files", "bacs", "email", "summary", "channel", "refunds", "agreement", "instruction", "reminder", "general"];
    return valid.includes(intent) ? intent : "general";
  } catch {
    return "general";
  }
}

/**
 * Get or create the in-memory conversation history for a given user.
 *
 * @param {string} userId
 * @returns {Array<{role: string, content: string}>}
 */
function getHistory(userId) {
  if (!conversationHistories.has(userId)) {
    conversationHistories.set(userId, []);
  }
  return conversationHistories.get(userId);
}

// ---------------------------------------------------------------------------
// Slack helpers
// ---------------------------------------------------------------------------

/**
 * Open (or retrieve) the DM channel with Hargo and return the channel ID.
 *
 * @returns {Promise<string>} DM channel ID
 */
async function getHargoDMChannel() {
  const result = await app.client.conversations.open({
    token: SLACK_BOT_TOKEN,
    users: HARGO_USER_ID,
  });
  return result.channel.id;
}

/**
 * Send a DM message to Hargo.
 *
 * @param {string} text  Message text (Slack mrkdwn)
 */
async function dmHargo(text) {
  const channelId = await getHargoDMChannel();
  await app.client.chat.postMessage({
    token: SLACK_BOT_TOKEN,
    channel: channelId,
    text,
    mrkdwn: true,
  });
}

/**
 * Fetch the last N messages from a Slack channel and return them as a
 * formatted string suitable for inclusion in a Claude prompt.
 *
 * @param {string} channelId
 * @param {number} limit
 * @returns {Promise<string>}
 */
async function fetchChannelHistory(channelId, limit = HISTORY_LIMIT) {
  try {
    const result = await app.client.conversations.history({
      token: SLACK_BOT_TOKEN,
      channel: channelId,
      limit,
    });

    if (!result.messages || result.messages.length === 0) {
      return "(no recent messages)";
    }

    // Messages come back newest-first; reverse to chronological order
    const messages = [...result.messages].reverse();

    return messages
      .map((msg) => {
        const ts = msg.ts
          ? new Date(parseFloat(msg.ts) * 1000).toISOString()
          : "unknown time";
        const user = msg.user || msg.username || "unknown";
        const text = msg.text || "(no text)";
        return `[${ts}] <@${user}>: ${text}`;
      })
      .join("\n");
  } catch (err) {
    console.error(
      `[bill-ling] Error fetching history for channel ${channelId}:`,
      err.message
    );
    return `(error fetching messages: ${err.message})`;
  }
}

// ---------------------------------------------------------------------------
// BigQuery
// ---------------------------------------------------------------------------

function formatBQValue(v) {
  if (v === null || v === undefined) return "null";
  // BigQuery DATE/DATETIME/TIMESTAMP fields come back as {value: '2026-04-07'}
  if (typeof v === "object" && v !== null && "value" in v) return String(v.value);
  // BigQuery BigInt-like objects
  if (typeof v === "object" && typeof v.toNumber === "function") return String(v.toNumber());
  return String(v);
}

const BACS_SCHEMA = `Table: \`raylo-production.landing_billington.bacs_monitoring\`
Each row = one due_date. All _amount columns are in £ (decimal, e.g. 169895.53 = £169,895.53). Do NOT divide by 100.

Columns (exact names — use these precisely in SQL):
- due_date (DATE): instalment due date the batch relates to
- batch_created_at (TIMESTAMP): when the payment batch was generated — use to answer "was a file generated today?"
- total_instalments_due (INTEGER): total instalments falling due on this date
- instalment_profiles_due_after_30d (INTEGER): instalment profiles due more than 30 days away

Instalment Direct Debits:
- instalment_direct_debits_called_for_count (INTEGER): count of instalment DDs submitted/raised in Anchor
- instalment_direct_debits_called_for_amount (DECIMAL pence): total value of instalment DD requests
- instalment_direct_debits_posted_count (INTEGER): count of instalment DDs that successfully posted
- instalment_direct_debits_posted_amount (DECIMAL pence): value of successfully posted instalment DDs
- instalment_direct_debits_bounced_count (INTEGER): count of instalment DDs that bounced/failed (ARUDD)
- instalment_direct_debits_bounced_amount (DECIMAL pence): value of bounced instalment DDs
- instalment_direct_debits_in_batch_count (INTEGER): count of instalment DDs in the BACS file sent to GoCardless
- instalment_direct_debits_in_batch_amount (DECIMAL pence): value of instalment DDs in the BACS file
- instalment_direct_debits_failed_gc_count (INTEGER): count rejected by GoCardless before BACS submission
- instalment_direct_debits_confirmed_gc_count (INTEGER): count confirmed by GoCardless as submitted to BACS
- instalment_direct_debits_in_flight_gc_count (INTEGER): count still pending final status from GoCardless

Recollection Direct Debits (retries after a bounce):
- recollection_direct_debits_called_for_count (INTEGER): count of recollection/retry DDs raised
- recollection_direct_debits_called_for_amount (DECIMAL pence): value of recollection DDs raised
- recollection_direct_debits_posted_count (INTEGER): recollection DDs that successfully posted
- recollection_direct_debits_posted_amount (DECIMAL pence): value recovered via recollection
- recollection_direct_debits_bounced_count (INTEGER): recollection DDs that bounced again
- recollection_direct_debits_bounced_amount (DECIMAL pence): value of failed recollection attempts
- recollection_direct_debits_in_batch_count (INTEGER): recollection DDs in a BACS file
- recollection_direct_debits_in_batch_amount (DECIMAL pence): value of recollection DDs in file
- recollection_direct_debits_failed_gc_count (INTEGER): recollection DDs rejected by GoCardless
- recollection_direct_debits_confirmed_gc_count (INTEGER): recollection DDs confirmed by GoCardless
- recollection_direct_debits_in_flight_gc_count (INTEGER): recollection DDs awaiting GC status

Repair / Damage Fee Direct Debits:
- repair_direct_debits_called_for_count (INTEGER): count of repair/damage fee DDs raised
- repair_direct_debits_called_for_amount (DECIMAL pence): value of repair/damage fee DDs raised
- repair_direct_debits_posted_count (INTEGER): repair DDs that successfully posted
- repair_direct_debits_posted_amount (DECIMAL pence): value of collected repair fees
- repair_direct_debits_bounced_count (INTEGER): repair DDs that bounced
- repair_direct_debits_bounced_amount (DECIMAL pence): value of bounced repair DDs
- repair_direct_debits_in_batch_count (INTEGER): repair DDs in the BACS file
- repair_direct_debits_in_batch_amount (DECIMAL pence): value of repair DDs in file
- repair_direct_debits_failed_gc_count (INTEGER): repair DDs rejected by GoCardless
- repair_direct_debits_confirmed_gc_count (INTEGER): repair DDs confirmed by GoCardless
- repair_direct_debits_in_flight_gc_count (INTEGER): repair DDs awaiting GC status
- damage_fee_profiles_due_after_30d (INTEGER): repair/damage fee profiles due more than 30 days away

GoCardless Aggregates:
- total_failed_go_cardless (INTEGER): total DDs across all types rejected by GoCardless
- total_in_flight_go_cardless (INTEGER): total DDs across all types still pending GC status
- total_confirmed_go_cardless (INTEGER): total DDs across all types confirmed by GC

Percentages, Forecasts & Pending:
- percent_of_instalment_due_called_for (DECIMAL %): % of due instalments with a DD raised
- percent_of_repair_due_called_for (DECIMAL %): % of repair/damage fees due with a DD raised
- pending_recollection_count (INTEGER): agreements in arrears with bacs_retransmit=TRUE, queued for retry
- pending_recollection_amount (DECIMAL pence): value of transactions queued for retry
- late_fees_applied (DECIMAL): late payment fees already applied
- estimated_late_fees_to_apply (DECIMAL): late fees expected based on current bounces
- estimated_direct_debits_called_for (INTEGER): forward estimate of DDs expected to be raised
- estimated_direct_debits_bounce (INTEGER): forward estimate of DDs expected to bounce

Operational Timing:
- time_to_create_payment_batch (DURATION): time from trigger to batch created in Anchor
- time_to_send_payment_file (DURATION): time from batch creation to BACS file sent to GoCardless
- time_to_apply_fees (DURATION): time for late fees to be applied after a bounce
- time_to_trigger (DURATION): time from scheduled trigger to process starting`;

async function buildBacsSQL(question, today) {
  // Known UK bank holidays (add more as needed)
  const bankHolidays = `UK bank holidays to know:
- Easter Good Friday 2026: 2026-04-03
- Easter Monday 2026: 2026-04-06
- Early May bank holiday 2026: 2026-05-04
- Spring bank holiday 2026: 2026-05-25
- Summer bank holiday 2026: 2026-08-31
- Christmas Day 2026: 2026-12-25
- Boxing Day 2026: 2026-12-28`;

  const prevWD = prevWorkingDay(today);
  const nextWD = nextWorkingDay(today);
  const prompt = `Today is ${today}. Previous working day: ${prevWD}. Next working day: ${nextWD}. Write a single BigQuery SQL query to answer this question: "${question}"

${BACS_SCHEMA}

${bankHolidays}

Rules:
- Only select the columns needed to answer the question
- Use WHERE, GROUP BY, ORDER BY, SUM() as appropriate
- "after Easter bank holiday" means due_date > '2026-04-06'
- "before Easter" means due_date < '2026-04-03'
- For aggregates (totals, counts) use SUM()
- If the question is about BigQuery table/view metadata (last modified, last updated, list tables, list views), use __TABLES__ like this:
  - All tables and views in landing_billington: SELECT table_id, table_type, TIMESTAMP_MILLIS(last_modified_time) as last_modified FROM \`raylo-production.landing_billington.__TABLES__\` ORDER BY last_modified DESC
  - Single table/view: SELECT TIMESTAMP_MILLIS(last_modified_time) as last_modified FROM \`raylo-production.landing_billington.__TABLES__\` WHERE table_id = 'table_name'
  - For other datasets: replace landing_billington with the correct dataset name (e.g. dbt_production)
  - table_type values: TABLE = base table, VIEW = view
  - IMPORTANT: if the question asks about SOURCE TABLES of a view, OR asks "when was bacs_monitoring last updated/modified/refreshed", OR asks about the freshness/staleness of bacs_monitoring — return the special token VIEW_SOURCE_LOOKUP:landing_billington:bacs_monitoring and nothing else. bacs_monitoring is a VIEW whose data comes from source tables; its freshness = the freshness of those source tables.
- ARUDD mismatch queries: anchor_bounces = instalment_direct_debits_bounced_count + recollection_direct_debits_bounced_count + repair_direct_debits_bounced_count. gc_failed = total_failed_go_cardless. A mismatch = WHERE (instalment_direct_debits_bounced_count + recollection_direct_debits_bounced_count + repair_direct_debits_bounced_count) != total_failed_go_cardless. Always filter to rows where batch_created_at IS NOT NULL (i.e. a file was actually generated). IMPORTANT: future dates (due_date > CURRENT_DATE()) will always show zero anchor bounces because the ARUDD file hasn't been processed into Anchor yet — this is expected, not a mismatch. For mismatch queries, always add AND due_date <= CURRENT_DATE() to exclude future dates from the mismatch comparison. If the user asks to see future dates separately, include them but label them clearly as 'pending — not yet due' (batch/GC data exists but Anchor bounce data will be 0 until the due date passes).
- AGREEMENT-LEVEL BACS DATA: \`raylo-production.landing_billington.bacs_monitoring_agreement_level\` has one row per BACS transmission, joined across the full payment lifecycle. Use this table to:
  (a) Drill down when bacs_monitoring shows discrepancies — find WHICH specific agreements/payments have issues
  (b) Investigate a specific agreement's payment history
  (c) Answer questions about stuck/failed/missing payments at the individual level
  Key columns: agreement_id, customer_id, bacs_trans_unique_id, gc_payment_id, anchor_transaction_id, anchor_reversal_id, bacs_date (DATE), bacs_amount (DECIMAL GBP), bacs_is_recollect (BOOL), bacs_error_desc, payment_profile_subtype (NULL=instalment, 'Damage Fee'=repair), gc_latest_action (STRING), gc_event_cause, gc_event_at, txn_creator (AUTOPOSTMON=auto-posted), txn_posted_date, reversal_posted_date, days_since_bacs_date (INT), lifecycle_stage (STRING), issue_bacs_error (BOOL), issue_no_batch (BOOL), issue_missing_from_gc (BOOL), issue_gc_stuck_in_flight (BOOL), issue_gc_failed_no_reversal (BOOL), issue_gc_paid_not_posted (BOOL).
  LIFECYCLE STAGES: 1_BACS_ERROR, 2_NO_GC_PAYMENT, 3_NO_BATCH, 4_AWAITING_GC, 5_GC_IN_FLIGHT, 6_GC_CONFIRMED, 7_SETTLED, 8_GC_PAID_NOT_POSTED, 9_BOUNCED_REVERSED, 10_BOUNCED_NOT_REVERSED, 11_GC_FAILED_NOT_POSTED, UNKNOWN.
  ISSUE SEVERITY: Critical = issue_gc_failed_no_reversal + issue_gc_paid_not_posted. High = issue_gc_stuck_in_flight WHERE days_since_bacs_date > 3. Medium = issue_no_batch + issue_missing_from_gc. Low = issue_bacs_error + issue_gc_stuck_in_flight WHERE days_since_bacs_date <= 2.
  DOMAIN RULES: BACS takes 3 working days — do NOT flag 'submitted' as stuck unless days_since_bacs_date > 3. Recollections (bacs_is_recollect=TRUE) have higher bounce rates (expected). AUTOPOSTMON = auto-posted (normal). GC terminal success: confirmed, paid_out. GC terminal failure: failed, cancelled, charged_back, chargeback_settled, late_failure_settled.
  DATE TERMINOLOGY: "payment date", "collection date", "due date" = bacs_date in this table (or due_date in bacs_monitoring). When filtering by payment date, ALWAYS use bacs_date — do NOT join to GC events just to get a date filter.
  CRITICAL — DO NOT JOIN TO GC EVENTS TABLE: bacs_monitoring_agreement_level ALREADY contains gc_event_at, gc_latest_action, and gc_event_cause. Do NOT join to intermediate_gocardless__payment_events unless the user explicitly asks for webhook IDs or multiple events per payment.
  CRITICAL — USE LIFECYCLE_STAGE FOR FILTERING: Always use lifecycle_stage to identify payment states. Do NOT reconstruct filters using individual columns (e.g. do NOT use "anchor_transaction_id IS NULL AND gc_latest_action IN (...)"). The lifecycle_stage column already encodes the correct business logic. Examples:
    - Bounces not posted in Anchor → WHERE lifecycle_stage = '11_GC_FAILED_NOT_POSTED' AND bacs_date <= CURRENT_DATE()
    - GC paid but not posted → WHERE lifecycle_stage = '8_GC_PAID_NOT_POSTED'
    - Bounced and reversed → WHERE lifecycle_stage = '9_BOUNCED_REVERSED'
  CRITICAL — FUTURE DATES: When querying for missing bounces / unposted reversals (lifecycle_stage = '11_GC_FAILED_NOT_POSTED'), ALWAYS add AND bacs_date <= CURRENT_DATE(). Anchor cannot have reversal transactions until the bacs_date has passed — future-dated rows will always appear as "not posted" but that is expected, not an issue. This applies to any query about missing/unposted bounce transactions.
  CRITICAL — DATE COLUMNS: "payment date", "collection date", "BACS date", "due date", "date of the bounce" = bacs_date. "Date GC reported the failure" = gc_event_at. When grouping bounces by date, use bacs_date (the payment date) unless the user specifically asks for the GC event/reporting date.
  STRATEGY: Use bacs_monitoring for aggregate questions. If it shows a discrepancy (e.g. anchor bounces != GC failures), automatically drill into bacs_monitoring_agreement_level to identify the specific agreements. When the user asks "which lines have an issue" or "show me the problems", query bacs_monitoring_agreement_level WHERE the relevant issue_* flag = TRUE.
  DRILL-DOWN QUERIES: When the user asks to see specific agreements (e.g. "show me the agreements", "which agreements", "list them", "drill down"), ALWAYS include agreement_id, bacs_amount, and bacs_date in the SELECT — these are needed for Anchor API cross-referencing. Example: SELECT agreement_id, bacs_amount, bacs_date FROM \`raylo-production.landing_billington.bacs_monitoring_agreement_level\` WHERE lifecycle_stage = '11_GC_FAILED_NOT_POSTED' AND bacs_date = '2026-03-27'
- AGREEMENT INFO: \`raylo-production.landing_billington.agreement_info\` — one row per agreement. Full schema:
  agreement_id (STRING — ALWAYS use this column name, NEVER agreement_reference or agreement_number),
  total_arrears (NUMERIC — positive = owes money, zero or negative = cleared),
  next_payment_date (DATE),
  assessment_completed_at (TIMESTAMP — when the device return assessment was completed; NULL = not yet assessed/returned),
  monthly_payment_amount (FLOAT64 — the recurring DD amount for this agreement),
  grade_description (STRING — device condition grade after assessment: 'Grade A', 'Grade B', 'Grade C', 'Grade D', 'Grade E', 'Grade F').
  TERMINOLOGY: "repair charge" and "damage fee" mean the same thing at Raylo — both refer to payment_profile_subtype = 'Damage Fee' in bacs_monitoring_agreement_level. When the user says "repair charge", treat it identically to "damage fee".
  Agreement IDs always look like 'A' followed by 5 or 8 digits. "Agreement number", "agreement reference", "agreement ref", and "agreement ID" all mean agreement_id.
  JOINING TABLES: You can JOIN agreement_info with bacs_monitoring_agreement_level ON agreement_id to answer cross-cutting questions like:
  - "What % of assessed/returned devices have damage fees?" → JOIN agreement_info (WHERE assessment_completed_at IS NOT NULL) with bacs_monitoring_agreement_level (WHERE payment_profile_subtype = 'Damage Fee')
  - "Average damage fee amount" → AVG(bacs_amount) from bacs_monitoring_agreement_level WHERE payment_profile_subtype = 'Damage Fee'
  - "Damage fee rate by grade" → JOIN and GROUP BY grade_description
  Example: What percentage of assessed agreements have a damage fee?
    SELECT
      COUNTIF(df.agreement_id IS NOT NULL) AS with_damage_fee,
      COUNT(DISTINCT ai.agreement_id) AS total_assessed,
      ROUND(100.0 * COUNTIF(df.agreement_id IS NOT NULL) / COUNT(DISTINCT ai.agreement_id), 1) AS damage_fee_pct
    FROM \`raylo-production.landing_billington.agreement_info\` ai
    LEFT JOIN (SELECT DISTINCT agreement_id FROM \`raylo-production.landing_billington.bacs_monitoring_agreement_level\` WHERE payment_profile_subtype = 'Damage Fee') df ON ai.agreement_id = df.agreement_id
    WHERE ai.assessment_completed_at IS NOT NULL
  Example: Average damage fee amount
    SELECT ROUND(AVG(bacs_amount), 2) AS avg_damage_fee FROM \`raylo-production.landing_billington.bacs_monitoring_agreement_level\` WHERE payment_profile_subtype = 'Damage Fee'
  Example: Damage fee rate by device grade
    SELECT ai.grade_description, COUNT(DISTINCT ai.agreement_id) AS assessed, COUNTIF(df.agreement_id IS NOT NULL) AS with_damage_fee, ROUND(100.0 * COUNTIF(df.agreement_id IS NOT NULL) / COUNT(DISTINCT ai.agreement_id), 1) AS damage_fee_pct
    FROM \`raylo-production.landing_billington.agreement_info\` ai
    LEFT JOIN (SELECT DISTINCT agreement_id FROM \`raylo-production.landing_billington.bacs_monitoring_agreement_level\` WHERE payment_profile_subtype = 'Damage Fee') df ON ai.agreement_id = df.agreement_id
    WHERE ai.assessment_completed_at IS NOT NULL GROUP BY 1 ORDER BY 1
- GOCARDLESS PAYMENT EVENTS: \`raylo-production.dbt_production.intermediate_gocardless__payment_events\` tracks GoCardless payment lifecycle events. EXACT schema (use ONLY these column names):
  event_id (STRING), webhook_id (STRING), resource_type (STRING), action (STRING — e.g. "failed", "confirmed", "paid_out", "cancelled"), payment_id (STRING), event_cause (STRING — e.g. "payment_failed", "mandate_cancelled", "bank_account_closed"), event_description (STRING), agreement_id (STRING), created_at (STRING — ISO timestamp as string, e.g. "2026-03-30T09:15:00Z"), original_charge_date (STRING), webhook_pk (STRING).
  IMPORTANT: The column is called "action" NOT "event_action". created_at is a STRING not TIMESTAMP — use SAFE_CAST(created_at AS TIMESTAMP) or PARSE_TIMESTAMP when filtering by date.
  For latest failure today: SELECT SAFE_CAST(created_at AS TIMESTAMP) AS failed_at, payment_id, event_cause, event_description, agreement_id FROM \`raylo-production.dbt_production.intermediate_gocardless__payment_events\` WHERE DATE(SAFE_CAST(created_at AS TIMESTAMP)) = CURRENT_DATE() AND (action = 'failed' OR event_cause = 'payment_failed') ORDER BY created_at DESC LIMIT 1
- TRANSACTIONS DATA: \`raylo-production.dbt_production.stg_landing_sentinel_s3db01__transactions\` contains Raylo's internal transaction records (from Aryza/Anchor/Sentinel). EXACT schema:
  transaction_id (INT64), transaction_type_id (INT64), batch_number (STRING), transaction_reference (STRING), agreement_id (STRING), created_at (TIMESTAMP), transaction_date (TIMESTAMP), bacs_payment_made (BOOL), code (STRING), principal_net (NUMERIC), interest_net (NUMERIC), fees_net (NUMERIC), fees_vat (NUMERIC), arrears_principal_net (NUMERIC), arrears_fees_net (NUMERIC), arrears_tax (NUMERIC).
  Use this table to reconcile payments received in GoCardless against what is recorded internally. When asked to compare GC payments vs transactions, query both tables and compare counts/amounts by date.
- LATE FEES TIMING: Late payment fees are applied on the NEXT working day after the due date. So "late fees today" or "late fees being applied today" means late fees for payments that were due on the PREVIOUS working day (${prevWD}). When the user asks "how many late fees today", query bacs_monitoring WHERE due_date = '${prevWD}'. The relevant columns are estimated_late_fees_to_apply (count) and estimated_time_to_apply_late_fees (time). Similarly, "late fees tomorrow" = fees for today's due_date (${today}). ALWAYS use the previous working day for "today's late fees" — never use today's date.
- BANK HOLIDAYS: If the question asks about a specific date that falls on a UK bank holiday, return BANK_HOLIDAY:<date> (e.g. BANK_HOLIDAY:2026-04-06) instead of SQL. No instalments fall due on bank holidays, so there will never be any data for those dates.
- IMPORTANT: You have access to ALL the tables listed above — bacs_monitoring, bacs_monitoring_agreement_level, agreement_info, intermediate_gocardless__payment_events, and stg_landing_sentinel_s3db01__transactions. If the question can be answered by joining or querying these tables, WRITE THE SQL. Do NOT say you don't have access or ask which table to use — the schemas are all documented above. "Asset returns", "assessments", "damage fees", "grades" → use agreement_info and bacs_monitoring_agreement_level.
- If you are not confident you can write a correct SQL query for the question asked, return CLARIFY: followed by a short question to ask the user for the missing detail. Do not guess.
- Return ONLY the SQL (or CLARIFY: message or BANK_HOLIDAY: token) — no explanation, no markdown, no backtick fences`;

  const raw = await askClaude(prompt);
  let sql = raw.trim();
  // Strip markdown code fences (start/end)
  sql = sql.replace(/^```sql?\n?/i, "").replace(/\n?```$/, "").trim();
  // If Claude returned preamble text + SQL in fences, extract just the SQL block
  const fenceMatch = sql.match(/```sql?\s*\n([\s\S]+?)\n```/i);
  if (fenceMatch) sql = fenceMatch[1].trim();
  // If Claude returned preamble text + bare SQL, extract from first SELECT/WITH
  // Note: WITH must be followed by a valid CTE pattern (WITH <name> AS) to avoid matching natural language like "with bacs_monitoring..."
  if (!/^\s*(SELECT\b|WITH\s+\w+\s+AS\s*\(|SHOW\b|DESCRIBE\b|CLARIFY:|BANK_HOLIDAY:|VIEW_SOURCE_LOOKUP:|FORECAST:)/i.test(sql)) {
    const sqlStart = sql.match(/\b(SELECT\b|WITH\s+\w+\s+AS\s*\()[\s\S]+$/i);
    if (sqlStart) sql = sqlStart[0].trim();
  }
  // Final cleanup
  return sql.replace(/^```sql?\n?/i, "").replace(/\n?```$/, "").trim();
}

async function runBigQuery(sql) {
  try {
    const [rows] = await bigquery.query({ query: sql, location: "EU" });
    if (!rows.length) return "No results returned.";
    const headers = Object.keys(rows[0]).join(" | ");
    const divider = headers.replace(/[^|]/g, "-");
    const body = rows.slice(0, 50).map((r) => Object.values(r).map(formatBQValue).join(" | ")).join("\n");
    const footer = rows.length > 50 ? `\n... (${rows.length - 50} more rows not shown)` : "";
    return `${headers}\n${divider}\n${body}${footer}`;
  } catch (err) {
    console.error("[bill-ling] BigQuery error:", err.message);
    return `BigQuery error: ${err.message}`;
  }
}

async function runBigQueryRaw(sql) {
  const [rows] = await bigquery.query({ query: sql, location: "EU" });
  return rows;
}

// ---------------------------------------------------------------------------
// Anchor SOAP API — GetAgreement
// ---------------------------------------------------------------------------
const ANCHOR_SOAP_URL = "https://xylofiproposalservice.anchor.co.uk/AnchorCollections.svc";
const ANCHOR_SOAP_ACTION = "urn:anchor.co.uk/2017/05/AnchorCollections/IAnchorCollections/GetAgreement";

async function getAgreementFromAnchor(agreementId) {
  const soapBody = `<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:anc="urn:anchor.co.uk/2017/05/AnchorCollections">
   <soapenv:Header/>
   <soapenv:Body>
      <anc:GetAgreement>
         <anc:request>
            <anc:Credentials>
               <anc:Password>fm55MVEAbjWeG2DP</anc:Password>
               <anc:UserID>124</anc:UserID>
               <anc:UserName>MAKE</anc:UserName>
            </anc:Credentials>
            <anc:AgreementNumber>${agreementId}</anc:AgreementNumber>
         </anc:request>
      </anc:GetAgreement>
   </soapenv:Body>
</soapenv:Envelope>`;

  const res = await fetch(ANCHOR_SOAP_URL, {
    method: "POST",
    headers: {
      "Content-Type": "text/xml;charset=UTF-8",
      "SOAPAction": ANCHOR_SOAP_ACTION,
    },
    body: soapBody,
  });

  if (!res.ok) {
    const errText = await res.text().catch(() => "");
    throw new Error(`Anchor API ${res.status}: ${errText.slice(0, 200)}`);
  }

  return await res.text(); // raw SOAP XML
}

/**
 * Extract bounce/reversal transactions (TypeId 150-156) from Anchor SOAP XML.
 * Returns a compact summary string instead of the full XML to stay within token limits.
 */
function extractReversalsFromXml(xml, agreementId) {
  // Extract all Transaction blocks
  const txnBlocks = xml.match(/<a:Transaction>([\s\S]*?)<\/a:Transaction>/g) || [];
  const reversals = [];
  for (const block of txnBlocks) {
    const typeMatch = block.match(/<a:TypeId>(\d+)<\/a:TypeId>/);
    if (!typeMatch) continue;
    const typeId = parseInt(typeMatch[1], 10);
    if (typeId < 150 || typeId > 156) continue;
    const amountMatch = block.match(/<a:Amount>([\d.-]+)<\/a:Amount>/);
    const dateMatch = block.match(/<a:TransactionDate>([\d-T:.]+)<\/a:TransactionDate>/);
    const createdMatch = block.match(/<a:CreatedAt>([\d-T:.]+)<\/a:CreatedAt>/);
    reversals.push({
      typeId,
      amount: amountMatch ? amountMatch[1] : "unknown",
      date: dateMatch ? dateMatch[1].slice(0, 10) : "unknown",
      created: createdMatch ? createdMatch[1].slice(0, 10) : "unknown",
    });
  }
  if (reversals.length === 0) return `${agreementId}: NO reversal transactions (TypeId 150-156) found`;
  return `${agreementId}: ${reversals.length} reversal(s) — ` +
    reversals.map(r => `TypeId ${r.typeId} | £${r.amount} | date ${r.date}`).join("; ");
}

// UK bank holidays — extend as needed
const UK_BANK_HOLIDAYS = new Set([
  "2026-01-01","2026-04-03","2026-04-06","2026-05-04",
  "2026-05-25","2026-08-31","2026-12-25","2026-12-28",
  "2027-01-01","2027-04-02","2027-04-05","2027-05-03",
  "2027-05-31","2027-08-30","2027-12-27","2027-12-28",
]);

const UK_BANK_HOLIDAY_NAMES = {
  "2026-01-01": "New Year's Day",
  "2026-04-03": "Good Friday",
  "2026-04-06": "Easter Monday",
  "2026-05-04": "Early May Bank Holiday",
  "2026-05-25": "Spring Bank Holiday",
  "2026-08-31": "Summer Bank Holiday",
  "2026-12-25": "Christmas Day",
  "2026-12-28": "Boxing Day (substitute)",
  "2027-01-01": "New Year's Day",
  "2027-04-02": "Good Friday",
  "2027-04-05": "Easter Monday",
  "2027-05-03": "Early May Bank Holiday",
  "2027-05-31": "Spring Bank Holiday",
  "2027-08-30": "Summer Bank Holiday",
  "2027-12-27": "Christmas Day (substitute)",
  "2027-12-28": "Boxing Day (substitute)",
};

function isWorkingDay(dateStr) {
  const d = new Date(dateStr + "T12:00:00Z");
  const dow = d.getUTCDay();
  return dow !== 0 && dow !== 6 && !UK_BANK_HOLIDAYS.has(dateStr);
}

function prevWorkingDay(dateStr) {
  const d = new Date(dateStr + "T12:00:00Z");
  do { d.setUTCDate(d.getUTCDate() - 1); }
  while (!isWorkingDay(d.toISOString().slice(0, 10)));
  return d.toISOString().slice(0, 10);
}

function nextWorkingDay(dateStr) {
  const d = new Date(dateStr + "T12:00:00Z");
  do { d.setUTCDate(d.getUTCDate() + 1); }
  while (!isWorkingDay(d.toISOString().slice(0, 10)));
  return d.toISOString().slice(0, 10);
}

function fmtDate(dateStr) {
  const [y, m, d] = dateStr.split("-");
  return `${d}/${m}/${y}`;
}

async function extractAruddDate(question) {
  const todayStr = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
  const yesterdayWD = prevWorkingDay(todayStr);
  const twoDaysAgoWD = prevWorkingDay(yesterdayWD);
  const prompt = `Today is ${todayStr}. Extract the charge date (due_date) the user is asking about for an ARUDD report.

IMPORTANT TIMING: The ARUDD report posted on day X covers bounces for the PREVIOUS working day's due_date.
- "Today's ARUDD" or default → due_date = ${yesterdayWD} (previous working day)
- "Yesterday's ARUDD" → the report posted yesterday, which covered due_date = ${twoDaysAgoWD}
- "ARUDD for 30/03" → due_date = 2026-03-30 (the literal date they mention)

Return ONLY a date in YYYY-MM-DD format.
If no specific date is mentioned and nothing implies a different day, return the word "default".
UK bank holidays: Good Friday 2026: 2026-04-03, Easter Monday 2026: 2026-04-06.
Weekends are not working days. "Last Friday" means the most recent Friday.

Question: "${question}"`;
  const result = (await askClaude(prompt)).trim();
  if (result === "default" || !/^\d{4}-\d{2}-\d{2}$/.test(result)) return null;
  return result;
}

// ---------------------------------------------------------------------------
// Missing bounces report — deterministic query, no LLM SQL generation
// ---------------------------------------------------------------------------
const MISSING_BOUNCES_PATTERN = /\b(bounce|bounced|bounces).{0,40}(not posted|missing|not.{0,10}anchor|unposted|no.{0,10}reversal|not.{0,10}reversed)\b/i;

async function buildMissingBouncesReport() {
  const today = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
  const sql = `
    SELECT
      bacs_date,
      COUNT(*) AS bounce_count_not_posted
    FROM \`raylo-production.landing_billington.bacs_monitoring_agreement_level\`
    WHERE lifecycle_stage = '11_GC_FAILED_NOT_POSTED'
      AND bacs_date <= '${today}'
    GROUP BY bacs_date
    ORDER BY bacs_date DESC`;

  console.log(`[bill-ling] Missing bounces SQL: ${sql}`);
  const rows = await runBigQueryRaw(sql);

  if (!rows || rows.length === 0) {
    return "No bounce transactions missing from Anchor were found (for payment dates up to and including today).\n\nAll GoCardless failures that have reached their payment date have corresponding reversal transactions in Anchor.";
  }

  const total = rows.reduce((sum, r) => sum + (Number(r.bounce_count_not_posted) || 0), 0);
  const lines = [
    `*Bounce transactions not posted in Anchor (payment date ≤ ${fmtDate(today)}):*\n`,
    "```",
    "BACS Date    | Count Not Posted",
    "-------------|------------------",
  ];
  for (const r of rows) {
    const d = formatBQValue(r.bacs_date);
    const c = Number(r.bounce_count_not_posted).toLocaleString("en-GB");
    lines.push(`${d}  | ${c.padStart(16)}`);
  }
  lines.push("```");
  lines.push(`\n*Total: ${total.toLocaleString("en-GB")} bounce transactions* in GoCardless with no corresponding reversal in Anchor.`);

  return lines.join("\n");
}

async function buildMissingBouncesForDate(targetDate) {
  const sql = `
    SELECT agreement_id, bacs_amount, bacs_date, gc_latest_action, gc_event_cause, gc_event_at
    FROM \`raylo-production.landing_billington.bacs_monitoring_agreement_level\`
    WHERE lifecycle_stage = '11_GC_FAILED_NOT_POSTED'
      AND bacs_date = '${targetDate}'
    ORDER BY bacs_amount DESC`;

  console.log(`[bill-ling] Missing bounces drill-down SQL: ${sql}`);
  const rows = await runBigQueryRaw(sql);

  if (!rows || rows.length === 0) {
    return { report: `No missing bounce transactions found for ${fmtDate(targetDate)}.`, agreements: [] };
  }

  const total = rows.reduce((sum, r) => sum + (Number(r.bacs_amount) || 0), 0);
  const lines = [
    `*Missing bounce transactions for ${fmtDate(targetDate)}: ${rows.length} agreement(s), £${total.toLocaleString("en-GB", { minimumFractionDigits: 2 })}*\n`,
    "```",
    "Agreement   | Amount    | GC Action   | GC Cause",
    "------------|-----------|-------------|----------",
  ];
  for (const r of rows.slice(0, 100)) {
    const id = r.agreement_id || "";
    const amt = `£${Number(r.bacs_amount || 0).toFixed(2).padStart(8)}`;
    const action = (r.gc_latest_action || "").padEnd(11);
    const cause = r.gc_event_cause || "";
    lines.push(`${id.padEnd(11)} | ${amt} | ${action} | ${cause}`);
  }
  lines.push("```");
  if (rows.length > 100) lines.push(`... and ${rows.length - 100} more.`);

  return {
    report: lines.join("\n"),
    agreements: rows.map(r => ({ id: (r.agreement_id || "").toUpperCase(), amount: r.bacs_amount, date: formatBQValue(r.bacs_date) })),
  };
}

async function buildAruddReport(targetDueDate = null) {
  const todayStr  = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
  const dueDate   = targetDueDate || prevWorkingDay(todayStr);
  const aruddDate = nextWorkingDay(dueDate);

  // Run today's data and 30-day averages in parallel
  let rows, avgRows;
  try {
    [rows, avgRows] = await Promise.all([
      runBigQueryRaw(`
        SELECT
          instalment_direct_debits_bounced_count   AS instalment_bounced,
          recollection_direct_debits_bounced_count AS recollection_bounced,
          repair_direct_debits_bounced_count       AS repair_bounced,
          total_failed_go_cardless                 AS gc_failed
        FROM \`raylo-production.landing_billington.bacs_monitoring\`
        WHERE due_date = '${dueDate}'
        LIMIT 1
      `),
      runBigQueryRaw(`
        SELECT
          AVG(instalment_direct_debits_bounced_count + recollection_direct_debits_bounced_count + IFNULL(repair_direct_debits_bounced_count, 0)) AS avg_total_bounced,
          AVG(instalment_direct_debits_bounced_count)   AS avg_instalment_bounced,
          AVG(recollection_direct_debits_bounced_count) AS avg_recollection_bounced,
          AVG(IFNULL(repair_direct_debits_bounced_count, 0)) AS avg_repair_bounced
        FROM \`raylo-production.landing_billington.bacs_monitoring\`
        WHERE due_date >= DATE_SUB('${dueDate}', INTERVAL 30 DAY)
          AND due_date < '${dueDate}'
          AND (instalment_direct_debits_bounced_count + recollection_direct_debits_bounced_count + IFNULL(repair_direct_debits_bounced_count, 0)) > 0
      `),
    ]);
  } catch (err) {
    return { report: `BigQuery error building ARUDD report: ${err.message}`, discrepancyPct: 0, anchorBounces: 0, gcFailed: 0, dueDate, postedPct: 0 };
  }

  const row = rows[0] || {};
  const instalmentBounced   = Number(formatBQValue(row.instalment_bounced))   || 0;
  const recollectionBounced = Number(formatBQValue(row.recollection_bounced)) || 0;
  const repairBounced       = Number(formatBQValue(row.repair_bounced))       || 0;
  const gcFailed            = Number(formatBQValue(row.gc_failed))            || 0;
  const anchorBounces       = instalmentBounced + recollectionBounced + repairBounced;

  if (!rows.length || (anchorBounces === 0 && gcFailed === 0)) {
    return {
      report: `*ARUDD: ${fmtDate(aruddDate)}*\nCharge date: ${fmtDate(dueDate)}\n\nNo ARUDD data available yet.`,
      discrepancyPct: 0, anchorBounces: 0, gcFailed: 0, dueDate, postedPct: 0,
    };
  }

  const avg = avgRows[0] || {};
  const avgTotal       = Number(formatBQValue(avg.avg_total_bounced))       || 0;
  const avgInstalment  = Number(formatBQValue(avg.avg_instalment_bounced))  || 0;
  const avgRecollection= Number(formatBQValue(avg.avg_recollection_bounced))|| 0;
  const avgRepair      = Number(formatBQValue(avg.avg_repair_bounced))      || 0;

  const postedPct       = gcFailed > 0 ? Math.round((anchorBounces / gcFailed) * 100) : 0;
  const collectionPct   = anchorBounces > 0 ? Math.round((instalmentBounced / anchorBounces) * 100) : 0;
  const recollectionPct = anchorBounces > 0 ? Math.round((recollectionBounced / anchorBounces) * 100) : 0;
  const repairPct       = anchorBounces > 0 ? Math.round((repairBounced / anchorBounces) * 100) : 0;
  const discrepancyPct  = gcFailed > 0 ? Math.abs((anchorBounces - gcFailed) / gcFailed) * 100 : 0;

  let report =
    `*ARUDD: ${fmtDate(aruddDate)}*\n` +
    `Charge date: ${fmtDate(dueDate)}\n\n` +
    `Anchor #: ${anchorBounces.toLocaleString("en-GB")}\n` +
    `GoCardless #: ${gcFailed.toLocaleString("en-GB")}\n` +
    `Posted in Anchor: ${postedPct}%\n\n` +
    `Collection #: ${instalmentBounced.toLocaleString("en-GB")} (${collectionPct}%)\n` +
    `Recollection #: ${recollectionBounced.toLocaleString("en-GB")} (${recollectionPct}%)` +
    (repairBounced > 0 ? `\nRepair #: ${repairBounced.toLocaleString("en-GB")} (${repairPct}%)` : ``);

  const flags = [];

  // --- Posted in Anchor status ---
  if (postedPct === 100) {
    flags.push(`✅ No issues with receiving bounce transactions — all GoCardless failures posted in Anchor.`);
  } else if (postedPct >= 95) {
    const diff = Math.abs(anchorBounces - gcFailed);
    const direction = anchorBounces > gcFailed ? "more" : "fewer";
    flags.push(`⚠️ Minor discrepancy — Anchor has ${diff.toLocaleString("en-GB")} ${direction} bounces than GoCardless (${postedPct}% posted). May resolve as data catches up.`);
  } else {
    // < 95% — source table freshness will be checked by the caller
    const diff = Math.abs(anchorBounces - gcFailed);
    const direction = anchorBounces > gcFailed ? "more" : "fewer";
    flags.push(`🚨 Significant discrepancy — only ${postedPct}% of GoCardless failures posted in Anchor. Anchor has ${diff.toLocaleString("en-GB")} ${direction} bounces than GoCardless (${discrepancyPct.toFixed(1)}%). Checking source table freshness...`);
  }

  // --- Volume context vs 30-day average (always shown) ---
  if (avgTotal > 0) {
    const totalRatio = anchorBounces / avgTotal;
    const pct = Math.round(Math.abs(totalRatio - 1) * 100);
    let bounceContext;
    if (totalRatio >= 1.5)       bounceContext = `📈 High bounce day — ${pct}% above the 30-day average (avg: #${Math.round(avgTotal).toLocaleString("en-GB")}).`;
    else if (totalRatio >= 1.15) bounceContext = `↑ Bounce volume slightly above average (+${pct}% vs 30-day avg of #${Math.round(avgTotal).toLocaleString("en-GB")}).`;
    else if (totalRatio <= 0.5)  bounceContext = `📉 Low bounce day — ${pct}% below the 30-day average (avg: #${Math.round(avgTotal).toLocaleString("en-GB")}).`;
    else if (totalRatio <= 0.85) bounceContext = `↓ Bounce volume slightly below average (-${pct}% vs 30-day avg of #${Math.round(avgTotal).toLocaleString("en-GB")}).`;
    else                         bounceContext = `Bounce volume is in line with the 30-day average (#${Math.round(avgTotal).toLocaleString("en-GB")}).`;
    flags.push(bounceContext);
  }
  if (avgRecollection > 0) {
    const recolRatio = recollectionBounced / avgRecollection;
    const pct = Math.round(Math.abs(recolRatio - 1) * 100);
    let recolContext;
    if (recolRatio >= 1.5)       recolContext = `📈 High recollection day — ${pct}% above the 30-day average (avg: #${Math.round(avgRecollection).toLocaleString("en-GB")}).`;
    else if (recolRatio >= 1.15) recolContext = `↑ Recollection volume slightly above average (+${pct}% vs 30-day avg of #${Math.round(avgRecollection).toLocaleString("en-GB")}).`;
    else if (recolRatio <= 0.5)  recolContext = `📉 Low recollection day — ${pct}% below the 30-day average (avg: #${Math.round(avgRecollection).toLocaleString("en-GB")}).`;
    else if (recolRatio <= 0.85) recolContext = `↓ Recollection volume slightly below average (-${pct}% vs 30-day avg of #${Math.round(avgRecollection).toLocaleString("en-GB")}).`;
    else                         recolContext = `Recollection volume is in line with the 30-day average (#${Math.round(avgRecollection).toLocaleString("en-GB")}).`;
    flags.push(recolContext);
  }

  if (flags.length) report += "\n\n" + flags.join("\n");

  return { report, discrepancyPct, anchorBounces, gcFailed, dueDate, postedPct };
}

// ---------------------------------------------------------------------------
// View source table metadata
// ---------------------------------------------------------------------------

/**
 * Given a view name and dataset, fetches the view definition, extracts all
 * source table references, then looks up each table's last_modified time.
 * Returns a formatted string.
 */
async function getViewSourceMetadata(dataset, viewName) {
  // Step 1: fetch the view SQL definition from INFORMATION_SCHEMA
  const defSQL = `SELECT view_definition FROM \`raylo-production.${dataset}.INFORMATION_SCHEMA.VIEWS\` WHERE table_name = '${viewName}'`;
  const [defRows] = await bigquery.query({ query: defSQL, location: "EU" });
  if (!defRows.length) return `No view named \`${viewName}\` found in \`${dataset}\`.`;
  const viewDef = defRows[0].view_definition;

  // Step 2: ask Claude to extract all table references from the view SQL
  const extractPrompt = `Extract every unique BigQuery table reference from this SQL view definition. Include backtick-quoted references in formats: \`project.dataset.table\`, \`dataset.table\`. Exclude CTEs (WITH clause aliases) — only real table/dataset paths. Return ONLY a JSON array of fully-qualified strings like ["project.dataset.table"], no markdown, no explanation:\n\n${viewDef}`;
  const rawList = await askClaude(extractPrompt);
  let tableRefs = [];
  try {
    tableRefs = JSON.parse(rawList.trim().replace(/^```json?\n?/i, "").replace(/\n?```$/, ""));
  } catch (_) {
    tableRefs = (rawList.match(/`[^`]+\.[^`]+\.[^`]+`/g) || []).map(s => s.replace(/`/g, ""));
  }

  if (!tableRefs || !tableRefs.length) return `Could not extract source tables from \`${viewName}\` definition.`;

  // Step 3: for each table ref, query __TABLES__ for last_modified (in parallel)
  const results = await Promise.all(tableRefs.map(async (ref) => {
    const clean = ref.replace(/`/g, "").trim();
    const parts = clean.split(".");
    let proj = "raylo-production", ds, tbl;
    if (parts.length === 3) { [proj, ds, tbl] = parts; }
    else if (parts.length === 2) { [ds, tbl] = parts; }
    else { ds = dataset; tbl = parts[0]; }

    try {
      const metaSQL = `SELECT TIMESTAMP_MILLIS(last_modified_time) as last_modified, row_count, type FROM \`${proj}.${ds}.__TABLES__\` WHERE table_id = '${tbl}'`;
      const [metaRows] = await bigquery.query({ query: metaSQL, location: "EU" });
      if (metaRows.length) {
        const lm = formatBQValue(metaRows[0].last_modified);
        const dt = new Date(lm).toLocaleString("en-GB", { timeZone: "Europe/London", day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit" });
        return `• \`${proj}.${ds}.${tbl}\` — last modified: ${dt}`;
      } else {
        return `• \`${proj}.${ds}.${tbl}\` — not found in __TABLES__`;
      }
    } catch (e) {
      return `• \`${proj}.${ds}.${tbl}\` — error: ${e.message}`;
    }
  }));

  return `*Source tables for \`${dataset}.${viewName}\`:*\n\n${results.join("\n")}`;
}

// ---------------------------------------------------------------------------
// BACS File Summary — formatted batch report
// ---------------------------------------------------------------------------

/**
 * Query bacs_monitoring rows as raw objects (not formatted text).
 */
async function queryBacsRows(whereClause) {
  const sql = `
    SELECT
      due_date,
      total_instalments_due,
      instalment_direct_debits_called_for_count,
      instalment_direct_debits_called_for_amount,
      percent_of_instalment_due_called_for,
      recollection_direct_debits_called_for_count,
      recollection_direct_debits_called_for_amount,
      repair_direct_debits_called_for_count,
      repair_direct_debits_called_for_amount,
      percent_of_repair_due_called_for,
      batch_created_at,
      estimated_late_fees_to_apply,
      estimated_direct_debits_bounce,
      time_to_apply_fees
    FROM \`raylo-production.landing_billington.bacs_monitoring\`
    WHERE batch_created_at IS NOT NULL
      AND ${whereClause}
    ORDER BY due_date ASC
  `;
  const [rows] = await bigquery.query({ query: sql, location: "EU" });
  return rows;
}

/**
 * Format a single bacs_monitoring row into the standard BACS file block.
 */
function formatBacsFileBlock(row) {
  const rawDate = formatBQValue(row.due_date);
  // rawDate is 'YYYY-MM-DD' — convert to DD/MM/YYYY
  const [y, m, d] = rawDate.split("-");
  const displayDate = `${d}/${m}/${y}`;

  const num = (v) => {
    const n = typeof v === "object" && v !== null && "value" in v ? Number(v.value) : Number(v);
    return isNaN(n) ? 0 : n;
  };
  // Amounts are stored in £ (decimal), not pence — no division needed
  const gbp = (v) => `£${num(v).toLocaleString("en-GB", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  const pct = (v) => `%${num(v).toFixed(2)}`;
  const cnt = (v) => `#${num(v).toLocaleString("en-GB")}`;

  // Percentages are NULL in the view — calculate from counts
  const instDue = num(row.total_instalments_due);
  const instCalledFor = num(row.instalment_direct_debits_called_for_count);
  const instPct = instDue > 0 ? (instCalledFor / instDue) * 100 : 0;

  const repairCalledFor = num(row.repair_direct_debits_called_for_count);
  // Repair "due" total isn't directly in the data — omit the line if 0 to avoid misleading zeros
  const repairDue = repairCalledFor; // minimum we know is at least the called-for count

  // Format batch created — only show time if it's not midnight (i.e. real timestamp, not date-only)
  const rawBatchTs = formatBQValue(row.batch_created_at);
  let batchCreated = "";
  if (rawBatchTs && rawBatchTs !== "null") {
    const dt = new Date(rawBatchTs);
    const timeStr = dt.toLocaleString("en-GB", { timeZone: "Europe/London", hour: "2-digit", minute: "2-digit" });
    const dateStr = dt.toLocaleDateString("en-GB", { timeZone: "Europe/London" });
    batchCreated = dateStr;
  }

  const estLateFees = num(row.estimated_late_fees_to_apply);
  const estBounces = num(row.estimated_direct_debits_bounce);

  // Late fees are applied on the next working day after the due date
  const feeApplyDate = fmtDate(nextWorkingDay(rawDate));
  const applyTimeRaw = formatBQValue(row.time_to_apply_fees);
  const applyTime = (applyTimeRaw && applyTimeRaw !== "null" && applyTimeRaw !== "0") ? applyTimeRaw : null;

  return [
    `*BACS file generated for: ${displayDate}*`,
    batchCreated ? `Batch created: ${batchCreated}` : null,
    ``,
    `Instalments due count: ${cnt(row.total_instalments_due)}`,
    `Instalment Direct Debits called for count: ${cnt(row.instalment_direct_debits_called_for_count)}`,
    `Instalment Direct Debits called for cash: ${gbp(row.instalment_direct_debits_called_for_amount)}`,
    `Percent of instalments called for by Direct Debit: ${pct(instPct)}`,
    `Re-collections Direct Debits called for count: ${cnt(row.recollection_direct_debits_called_for_count)}`,
    `Re-collections Direct Debits called for cash: ${gbp(row.recollection_direct_debits_called_for_amount)}`,
    repairCalledFor > 0 ? `` : null,
    repairCalledFor > 0 ? `Repair charge Direct Debits called for count: ${cnt(row.repair_direct_debits_called_for_count)}` : null,
    repairCalledFor > 0 ? `Repair charge Direct Debits called for cash: ${gbp(row.repair_direct_debits_called_for_amount)}` : null,
    ``,
    estLateFees > 0 ? `Estimated late fees to apply: #${Math.round(estLateFees).toLocaleString("en-GB")}` : null,
    estBounces > 0 ? `Estimated bounces: #${Math.round(estBounces).toLocaleString("en-GB")}` : null,
    applyTime
      ? `Late fees will be applied: ${feeApplyDate} (est. ${applyTime})`
      : estLateFees > 0 ? `Late fees will be applied: ${feeApplyDate}` : null,
  ].filter(v => v !== null).join("\n");
}

/**
 * Consolidate multiple bacs_monitoring rows for the same due_date into one block.
 * This happens when collections and re-collections batches are created on different dates.
 */
function formatConsolidatedBacsBlock(rows) {
  const num = (v) => {
    const n = typeof v === "object" && v !== null && "value" in v ? Number(v.value) : Number(v);
    return isNaN(n) ? 0 : n;
  };
  const gbp = (v) => `£${num(v).toLocaleString("en-GB", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  const cnt = (v) => `#${num(v).toLocaleString("en-GB")}`;
  const pct = (v) => `%${num(v).toFixed(2)}`;

  const rawDate = formatBQValue(rows[0].due_date);
  const [y, m, d] = rawDate.split("-");
  const displayDate = `${d}/${m}/${y}`;

  // Collect unique batch creation dates (date-only, no time)
  const batchDates = [...new Set(rows.map(r => {
    const ts = formatBQValue(r.batch_created_at);
    if (!ts || ts === "null") return null;
    return new Date(ts).toLocaleDateString("en-GB", { timeZone: "Europe/London" });
  }).filter(Boolean))].sort();

  // Aggregate across all rows
  const totalInstDue = Math.max(...rows.map(r => num(r.total_instalments_due)));
  const instCount = rows.reduce((s, r) => s + num(r.instalment_direct_debits_called_for_count), 0);
  const instAmount = rows.reduce((s, r) => s + num(r.instalment_direct_debits_called_for_amount), 0);
  const recolCount = rows.reduce((s, r) => s + num(r.recollection_direct_debits_called_for_count), 0);
  const recolAmount = rows.reduce((s, r) => s + num(r.recollection_direct_debits_called_for_amount), 0);
  const repairCount = rows.reduce((s, r) => s + num(r.repair_direct_debits_called_for_count), 0);
  const repairAmount = rows.reduce((s, r) => s + num(r.repair_direct_debits_called_for_amount), 0);
  const instPct = totalInstDue > 0 ? (instCount / totalInstDue) * 100 : 0;

  const estLateFees = Math.max(...rows.map(r => num(r.estimated_late_fees_to_apply)));
  const estBounces = Math.max(...rows.map(r => num(r.estimated_direct_debits_bounce)));

  const feeApplyDate = fmtDate(nextWorkingDay(rawDate));
  const applyTimeRaw = rows.map(r => formatBQValue(r.time_to_apply_fees)).find(t => t && t !== "null" && t !== "0") || null;

  // Identify which batch had collections vs re-collections for the batch-created label
  const collectionBatch = rows.find(r => num(r.instalment_direct_debits_called_for_count) > 0);
  const recollectionBatch = rows.find(r => num(r.recollection_direct_debits_called_for_count) > 0);

  const collectionDate = collectionBatch ? new Date(formatBQValue(collectionBatch.batch_created_at)).toLocaleDateString("en-GB", { timeZone: "Europe/London" }) : null;
  const recollectionDate = recollectionBatch ? new Date(formatBQValue(recollectionBatch.batch_created_at)).toLocaleDateString("en-GB", { timeZone: "Europe/London" }) : null;

  let batchLine;
  if (collectionDate && recollectionDate && collectionDate !== recollectionDate) {
    batchLine = `Batch created: ${collectionDate} (collections), ${recollectionDate} (re-collections)`;
  } else {
    batchLine = `Batch created: ${batchDates.join(", ")}`;
  }

  return [
    `*BACS file generated for: ${displayDate}*`,
    batchLine,
    ``,
    `Instalments due count: ${cnt(totalInstDue)}`,
    `Instalment Direct Debits called for count: ${cnt(instCount)}`,
    `Instalment Direct Debits called for cash: ${gbp(instAmount)}`,
    `Percent of instalments called for by Direct Debit: ${pct(instPct)}`,
    `Re-collections Direct Debits called for count: ${cnt(recolCount)}`,
    `Re-collections Direct Debits called for cash: ${gbp(recolAmount)}`,
    repairCount > 0 ? `` : null,
    repairCount > 0 ? `Repair charge Direct Debits called for count: ${cnt(repairCount)}` : null,
    repairCount > 0 ? `Repair charge Direct Debits called for cash: ${gbp(repairAmount)}` : null,
    ``,
    estLateFees > 0 ? `Estimated late fees to apply: #${Math.round(estLateFees).toLocaleString("en-GB")}` : null,
    estBounces > 0 ? `Estimated bounces: #${Math.round(estBounces).toLocaleString("en-GB")}` : null,
    applyTimeRaw
      ? `Late fees will be applied: ${feeApplyDate} (est. ${applyTimeRaw})`
      : estLateFees > 0 ? `Late fees will be applied: ${feeApplyDate}` : null,
  ].filter(v => v !== null).join("\n");
}

/**
 * Pre-resolve any "working day before/after X" expressions in a question to a concrete date,
 * so Claude doesn't have to do working-day arithmetic across bank holidays.
 * Returns the question with the expression replaced by "YYYY-MM-DD".
 */
async function resolveWorkingDayExpressions(question, today) {
  // Ask Claude to extract any date anchor and working-day direction from the question.
  // Returns JSON: { resolved: "YYYY-MM-DD" | null, direction: "prev" | "next" | null }
  const extractPrompt = `Today is ${today}.
Extract the anchor date and working-day direction from this question, if any.
Question: "${question}"

Rules:
- "working day before X", "previous working day of X", "day before X" → direction: "prev", resolved: X as YYYY-MM-DD
- "working day after X", "next working day after X", "day after X" → direction: "next", resolved: X as YYYY-MM-DD
- If no working-day expression exists, return null for both.
- Always resolve month names to YYYY-MM-DD using year ${today.slice(0, 4)} unless stated otherwise.

Reply ONLY with valid JSON, no explanation. Example: {"direction":"prev","resolved":"2026-04-07"}`;

  try {
    const raw = await askClaude(extractPrompt);
    const json = JSON.parse(raw.replace(/```json|```/g, "").trim());
    if (json.resolved && json.direction === "prev") {
      const computed = prevWorkingDay(json.resolved);
      return question.replace(/working\s+day\s+before\s+[\d\/\w\s]+/i, computed)
                     .replace(/previous\s+working\s+day\s+of\s+[\d\/\w\s]+/i, computed)
                     .replace(/day\s+before\s+[\d\/\w\s]+/i, computed)
                     + ` (${computed})`;
    }
    if (json.resolved && json.direction === "next") {
      const computed = nextWorkingDay(json.resolved);
      return question + ` (${computed})`;
    }
  } catch (e) {
    // Extraction failed — pass question through unchanged
  }
  return question;
}

/**
 * Use Claude to turn a question into a BigQuery WHERE clause modifier for the BACS file report.
 */
async function buildBacsFileWhere(question, today) {
  // Pre-resolve any working day expressions so Claude gets a concrete YYYY-MM-DD date
  const resolvedQuestion = await resolveWorkingDayExpressions(question, today);

  const bhList = [...UK_BANK_HOLIDAYS].sort().map(d => `  ${d}`).join("\n");
  const prompt = `Today is ${today}. Question: "${resolvedQuestion}"

The bacs_monitoring table has two types of rows:
1. Rows WHERE batch_created_at IS NOT NULL — the batch file has already been generated.
2. Rows WHERE batch_created_at IS NULL — no batch yet, just instalment forecast data.

IMPORTANT: If the question asks about the NEXT batch TO BE generated (e.g. "next generation date", "next batch to generate", "when will the next file be created", "not yet generated"), reply with exactly:
  FORECAST:due_date > '${today}'
This routes to the forecast path which shows ungenerated future batches.

LATE FEES TIMING: Late payment fees are applied on the NEXT working day after the due date. So "late fees today" or "late fees being applied today" = late fees for the PREVIOUS working day's due_date (${prevWorkingDay(today)}). "Late fees tomorrow" = late fees for today's due_date (${today}).

Otherwise return ONLY a BigQuery SQL WHERE sub-clause (no WHERE keyword, no semicolon). Examples:
- "future dates" → due_date > CURRENT_DATE()
- "today" or "generated today" → DATE(batch_created_at) = CURRENT_DATE()
- "this week" → DATE(batch_created_at) >= DATE_TRUNC(CURRENT_DATE(), WEEK)
- "27 March" or "2026-03-27" → due_date = '2026-03-27'
- "after Easter" → due_date > '2026-04-06'
- "all" or unspecific → due_date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
- "late fees today" or "late fees applied today" → due_date = '${prevWorkingDay(today)}'
- If the question contains a YYYY-MM-DD date in parentheses, use that exact date.

UK bank holidays (non-working days):
${bhList}

Reply with ONLY the WHERE sub-clause string or the FORECAST: prefix, nothing else.`;
  const clause = await askClaude(prompt);
  // Strip any ORDER BY / LIMIT that Claude may have appended — queryBacsRows handles ordering
  const result = clause.trim()
    .replace(/;$/, "")
    .replace(/\s+(ORDER\s+BY|LIMIT)\s+.*/i, "")
    .trim();
  console.log(`[bill-ling] buildBacsFileWhere resolved: "${resolvedQuestion}" → ${result}`);
  return result;
}

/**
 * Pull rows and return formatted report blocks. Returns null if no data.
 */
async function getBacsFileReport(whereClause) {
  const rows = await queryBacsRows(whereClause);
  if (!rows.length) return null;

  // Group rows by due_date — same due_date can have separate collection & recollection batches
  const groups = new Map();
  for (const row of rows) {
    const key = formatBQValue(row.due_date);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  const blocks = [];
  for (const [dueDate, groupRows] of groups) {
    if (groupRows.length === 1) {
      blocks.push(formatBacsFileBlock(groupRows[0]));
    } else {
      blocks.push(formatConsolidatedBacsBlock(groupRows));
    }
  }
  return blocks.join("\n\n---\n\n");
}

/**
 * Read #customer-refunds-and-complaints for the last 30 days, fetch thread replies
 * for any message that has them, and use Claude to identify refund requests that
 * are still pending approval or have not been confirmed as processed.
 */

/**
 * Analyse the transcript to infer who most commonly performs each refund stage.
 * Returns { approval, processing, batch_file, barclays_confirmation } — each an array
 * of { name, id, count } sorted by count desc. Falls back to REFUND_CONTACTS if inference fails.
 */
async function inferRefundContacts(transcript) {
  const prompt = `Analyse this Slack transcript from #customer-refunds-and-complaints.
Count how many times each person performs each of these actions:

APPROVAL: giving explicit approval for a refund ("approved", "happy to approve", "yes go ahead", "please process", "go ahead", or directing someone to process)
PROCESSING: confirming an individual refund is done ("processed", "done", "sent", "refunded", "actioned", "completed", "paid") — in a thread reply to a refund request, NOT a batch file message
BATCH_FILE: posting a channel-level message announcing a refund file has been generated and uploaded to Google Drive for Barclays
BARCLAYS_CONFIRMATION: confirming a batch file has been paid/authorised on Barclays ("paid", "authorised", "confirmed", "uploaded")

User IDs appear in parentheses after names in the transcript, e.g. "Laura Sierociuk (<@U123ABC>)".

Return ONLY a JSON object in this exact format (no other text):
{
  "approval": [{"name": "...", "id": "U...", "count": N}],
  "processing": [{"name": "...", "id": "U...", "count": N}],
  "batch_file": [{"name": "...", "id": "U...", "count": N}],
  "barclays_confirmation": [{"name": "...", "id": "U...", "count": N}]
}
Sort each array by count descending. Only include people with count >= 1.

--- TRANSCRIPT ---
${transcript}
--- END ---`;

  try {
    const raw = await askClaude(prompt);
    const match = raw.match(/\{[\s\S]*\}/);
    if (!match) return null;
    return JSON.parse(match[0]);
  } catch (e) {
    console.error("[bill-ling] inferRefundContacts error:", e.message);
    return null;
  }
}

// Count working days between two ISO date strings (exclusive of start, inclusive of end)
function workingDaysBetween(fromIso, toIso) {
  if (!fromIso || fromIso >= toIso) return 0;
  let count = 0;
  let d = new Date(fromIso + "T12:00:00Z");
  const end = new Date(toIso + "T12:00:00Z");
  while (d < end) {
    d.setUTCDate(d.getUTCDate() + 1);
    const ds = d.toISOString().slice(0, 10);
    if (isWorkingDay(ds)) count++;
  }
  return count;
}

/**
 * Add ✅ reaction to the parent message of each resolved refund thread.
 * Silently ignores "already_reacted" errors (idempotent).
 */
async function markResolvedThreads(resolved) {
  for (const item of resolved) {
    if (!item.thread_ts) continue;
    try {
      await app.client.reactions.add({
        token: SLACK_BOT_TOKEN,
        channel: CHANNELS.CUSTOMER_REFUNDS,
        timestamp: item.thread_ts,
        name: "white_check_mark",
      });
      console.log(`[bill-ling] ✅ reaction added to thread ${item.thread_ts}`);
    } catch (err) {
      if (err.data?.error !== "already_reacted") {
        console.error(`[bill-ling] Failed to add reaction to ${item.thread_ts}:`, err.message);
      }
    }
  }
}

/**
 * Post chaser replies into individual refund threads for items older than 0 working days.
 * data = structured JSON returned by getRefundsSummary.
 */
async function postRefundChasers(data) {
  if (!data) return;
  const today = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });

  // Customer action items: only chase if > 7 working days (give customer time to respond)
  // Internal Raylo items: chase every time (approval, processing, batch files)
  const customerItems  = (data.awaiting_customer_action || []).map(i => ({ ...i, minDays: 7 }));
  const internalItems  = [
    ...(data.awaiting_approval   || []).map(i => ({ ...i, _bucket: "awaiting_approval" })),
    ...(data.awaiting_processing || []).map(i => ({ ...i, _bucket: "awaiting_processing" })),
    ...(data.batch_files         || []).map(i => ({ ...i, _bucket: "batch_files" })),
  ].map(i => ({ ...i, minDays: 1 }));

  for (const item of [...customerItems, ...internalItems]) {
    const days = workingDaysBetween(item.date_iso, today);
    if (days < item.minDays) {
      console.log(`[bill-ling] Skipping chaser for ${item.ref || item.date} — ${days} working day(s) (min ${item.minDays})`);
      continue;
    }
    if (!item.thread_ts || !item.chase_message) continue;

    // Skip chasing if Billington already posted an approval notification today
    const notifiedDate = approvalNotifiedThreads.get(item.thread_ts);
    if (notifiedDate === today) {
      console.log(`[bill-ling] Skipping chaser for ${item.ref || item.date} — approval notification already posted today`);
      continue;
    }

    // For awaiting_processing items with an agreement ID, check Anchor first.
    // If the refund (TypeId 149) has already been posted, confirm instead of chasing.
    if (item.ref && /^A\d{5,8}$/i.test(item.ref) && item._bucket === "awaiting_processing") {
      const afterDate = item.date_iso || today;
      const result = await checkAnchorForRefund(item.ref, afterDate);
      if (result.posted) {
        try {
          await app.client.chat.postMessage({
            token: SLACK_BOT_TOKEN,
            channel: CHANNELS.CUSTOMER_REFUNDS,
            thread_ts: item.thread_ts,
            text: `I've checked Anchor and the refund for ${item.ref} has been posted (${result.detail}). ✅ Marking as complete.`,
            mrkdwn: true,
          });
          await app.client.reactions.add({
            token: SLACK_BOT_TOKEN,
            channel: CHANNELS.CUSTOMER_REFUNDS,
            timestamp: item.thread_ts,
            name: "white_check_mark",
          }).catch(e => { if (!e.message?.includes("already_reacted")) console.error(e.message); });
          console.log(`[bill-ling] Refund confirmed via Anchor for ${item.ref} (${result.detail}) — skipping chaser`);
        } catch (err) {
          console.error(`[bill-ling] Failed to post Anchor confirmation for ${item.ref}:`, err.message);
        }
        continue; // skip the normal chaser
      }
    }

    try {
      await app.client.chat.postMessage({
        token: SLACK_BOT_TOKEN,
        channel: CHANNELS.CUSTOMER_REFUNDS,
        thread_ts: item.thread_ts,
        text: item.chase_message,
        mrkdwn: true,
      });
      console.log(`[bill-ling] Chaser posted in thread ${item.thread_ts} for ${item.ref || item.date} (${days} working days)`);
    } catch (err) {
      console.error(`[bill-ling] Failed to chase thread ${item.thread_ts}:`, err.message);
    }
  }
}

// ---------------------------------------------------------------------------
// Check Anchor API for a refund transaction (TypeId 149) on a given agreement
// Returns { posted: true, detail: "£X.XX posted on YYYY-MM-DD" } or { posted: false }
// ---------------------------------------------------------------------------
async function checkAnchorForRefund(agreementId, afterDate) {
  try {
    console.log(`[bill-ling] Checking Anchor for refund on ${agreementId} (after: ${afterDate})`);
    const xml = await getAgreementFromAnchor(agreementId);
    // Match Transaction blocks with or without namespace prefix (a: or none)
    const txnBlocks = xml.match(/<(?:a:)?Transaction>([\s\S]*?)<\/(?:a:)?Transaction>/g) || [];

    for (const block of txnBlocks) {
      const typeMatch = block.match(/<(?:a:)?TypeId>(\d+)<\/(?:a:)?TypeId>/);
      if (!typeMatch || parseInt(typeMatch[1], 10) !== 149) continue; // TypeId 149 = refund

      // Match Gross (non-namespaced) or Amount (namespaced)
      const amountMatch = block.match(/<(?:a:)?(?:Amount|Gross)>([\d.-]+)<\/(?:a:)?(?:Amount|Gross)>/);
      const dateMatch = block.match(/<(?:a:)?TransactionDate>([\d-T:.]+)<\/(?:a:)?TransactionDate>/);
      if (!amountMatch || !dateMatch) continue;

      const txnAmount = Math.abs(parseFloat(amountMatch[1]));
      const txnDate = dateMatch[1].slice(0, 10);

      // Transaction must be on or after the request date
      if (txnDate < afterDate) continue;

      return { posted: true, detail: `£${txnAmount.toFixed(2)} refunded on ${txnDate}` };
    }
    return { posted: false };
  } catch (err) {
    console.error(`[bill-ling] Anchor refund check failed for ${agreementId}:`, err.message);
    return { posted: false };
  }
}

// ---------------------------------------------------------------------------
// Hourly approval monitor — checks if Stephen Riley has approved a refund/GOGW
// ---------------------------------------------------------------------------
function textContainsAny(text, keywords) {
  const lower = (text || "").toLowerCase();
  return keywords.some(kw => lower.includes(kw));
}

// Approver name lookup for notification messages
const APPROVER_NAMES = {
  "U0972QQNKH7": "Stephen Riley",
  "U01DMHVF8KG": "Hargo",
  "UJH96RKNK":   "Adam Lusk",
  "U09D1UMM8GY": "Ronan",
  "U031G7PQVM0": "Michael Bland",
  "U02NRTVLPC5": "Daniel Murphy",
};

// Use LLM to classify whether a reply from an approver is giving the green light
async function classifyApproval(parentText, replyText, replyAuthor) {
  const prompt = `You are classifying a message in a refund/complaints Slack channel.

The original request:
"${(parentText || "").slice(0, 500)}"

A reply from ${replyAuthor}:
"${(replyText || "").slice(0, 500)}"

Is this reply an UNCONDITIONAL APPROVAL to go ahead and process the refund/payment/GOGW right now?

YES examples: "approved", "go ahead", "yes process it", "happy with that", "we should go ahead with the payments", "Approved @Julia".
NO examples: "Approved if arrears are cleared", "Approved once the device is received", "Go ahead when the customer pays", "Check the status first", "What is the trade in status?", any question, any request for information.

CRITICAL: If the approval has ANY condition attached ("if", "once", "when", "after", "provided", "as long as", "subject to"), it is NOT an approval — reply NO. A conditional approval means the condition must be verified first before processing.

Reply with ONLY "YES" or "NO".`;

  try {
    const result = await anthropic.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 5,
      messages: [{ role: "user", content: prompt }],
    });
    return (result.content[0]?.text || "").trim().toUpperCase() === "YES";
  } catch (err) {
    console.error("[bill-ling] Approval classification error:", err.message);
    return false;
  }
}

async function checkRefundApprovals() {
  const today = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
  const oldest = String(Math.floor(Date.now() / 1000) - 30 * 86400); // last 30 days

  const histResp = await app.client.conversations.history({
    token: SLACK_BOT_TOKEN,
    channel: CHANNELS.CUSTOMER_REFUNDS,
    oldest,
    limit: 100,
  });

  const messages = (histResp.messages || []).filter((msg) => {
    if (msg.bot_id || msg.subtype) return false;
    if (msg.reply_count === 0) return false; // no replies = no approval
    if (BOT_USER_ID) {
      const alreadyDone = (msg.reactions || []).some(
        (r) => r.name === "white_check_mark" && (r.users || []).includes(BOT_USER_ID)
      );
      if (alreadyDone) return false;
    }
    return true;
  });

  let notified = 0;
  for (const msg of messages) {
    // Only skip if we've already confirmed the refund is posted (value = "confirmed")
    // Threads that were just notified (value = date string) should still be re-checked for Anchor posting
    if (approvalNotifiedThreads.get(msg.ts) === "confirmed") {
      console.log(`[bill-ling] Approval monitor: skipping thread ${msg.ts} — already confirmed via Anchor`);
      continue;
    }

    let replies;
    try {
      const threadResp = await app.client.conversations.replies({
        token: SLACK_BOT_TOKEN,
        channel: CHANNELS.CUSTOMER_REFUNDS,
        ts: msg.ts,
        limit: 50,
      });
      replies = (threadResp.messages || []).slice(1); // skip parent
    } catch (e) {
      continue;
    }

    // Check if any approved user posted an approval (LLM-based classification)
    let approvalReply = null;
    const approverReplies = replies.filter(r => REFUND_APPROVERS.has(r.user));
    console.log(`[bill-ling] Approval monitor: thread ${msg.ts} has ${replies.length} replies, ${approverReplies.length} from approvers`);
    for (const r of approverReplies) {
      const approverName = APPROVER_NAMES[r.user] || "Unknown";
      console.log(`[bill-ling] Approval monitor: classifying reply from ${approverName}: "${(r.text || "").slice(0, 60)}"`);
      const isApproval = await classifyApproval(msg.text, r.text, approverName);
      console.log(`[bill-ling] Approval monitor: classification result = ${isApproval}`);
      if (isApproval) {
        approvalReply = r;
        break;
      }
    }
    if (!approvalReply) {
      console.log(`[bill-ling] Approval monitor: no approval found in thread ${msg.ts}`);
      continue;
    }

    // Skip if a human confirmed processing (done/paid/sent) — but not if it's a question
    const humanProcessingConfirm = replies.some((r) => {
      if (r.user === BOT_USER_ID || r.bot_id) return false;
      const txt = (r.text || "").trim();
      // Ignore questions — they contain "?" or start with interrogative words
      if (txt.includes("?") || /^(has|have|did|was|were|is|can|could|will|when|check)\b/i.test(txt)) return false;
      return textContainsAny(txt, PROCESSING_KEYWORDS);
    });
    if (humanProcessingConfirm) {
      console.log(`[bill-ling] Approval monitor: thread ${msg.ts} has human processing confirmation — skipping`);
      continue;
    }

    // Extract agreement ID from full thread (parent + replies, human messages only)
    const allThreadText = [msg.text || "", ...replies.filter(r => !r.bot_id && r.user !== BOT_USER_ID).map(r => r.text || "")].join(" ");
    const agIds = [...new Set(
      (allThreadText.match(/\bA\d{5}(?:\d{3})?\b/gi) || []).map(id => id.toUpperCase())
    )];
    const agRef = agIds.length ? ` (${agIds.join(", ")})` : "";

    // Request date = parent message timestamp
    const requestDate = new Date(Number(msg.ts) * 1000).toISOString().slice(0, 10);

    // Resolve approver name and extract their quote
    const approverName = APPROVER_NAMES[approvalReply.user] || "An approver";
    const quote = (approvalReply.text || "").slice(0, 80);

    // Check if Billington already posted an approval notification in this thread
    const alreadyNotified = replies.some((r) => r.user === BOT_USER_ID && (r.text || "").includes("good to go for processing"));
    // Check if Billington already confirmed this refund via Anchor
    const alreadyConfirmed = replies.some((r) => r.user === BOT_USER_ID && (r.text || "").includes("refund has been posted"));

    if (alreadyConfirmed) continue; // fully resolved — nothing more to do

    // Check Anchor API for refund transaction (TypeId 149) if we have an agreement ID
    let refundPosted = false;
    let anchorDetail = "";
    if (agIds.length > 0) {
      const result = await checkAnchorForRefund(agIds[0], requestDate);
      refundPosted = result.posted;
      anchorDetail = result.detail || "";
      console.log(`[bill-ling] Approval monitor: Anchor check for ${agIds[0]} — posted: ${refundPosted}${anchorDetail ? `, ${anchorDetail}` : ""}`);
    }

    if (refundPosted) {
      // Refund posted in Anchor — confirm and mark complete
      const confirmText = `Refund for ${agIds[0]} has been posted in Anchor (${anchorDetail}). ✅ Marking as complete.`;
      try {
        await app.client.chat.postMessage({
          token: SLACK_BOT_TOKEN,
          channel: CHANNELS.CUSTOMER_REFUNDS,
          thread_ts: msg.ts,
          text: confirmText,
          mrkdwn: true,
        });
        await app.client.reactions.add({
          token: SLACK_BOT_TOKEN,
          channel: CHANNELS.CUSTOMER_REFUNDS,
          timestamp: msg.ts,
          name: "white_check_mark",
        }).catch(e => { if (!e.message?.includes("already_reacted")) console.error(e.message); });
        notified++;
        console.log(`[bill-ling] Refund confirmed via Anchor in thread ${msg.ts} for ${agIds[0]} (${anchorDetail})`);
      } catch (err) {
        console.error(`[bill-ling] Failed to post Anchor confirmation in thread ${msg.ts}:`, err.message);
      }
    } else if (!alreadyNotified) {
      // First time seeing this approval — notify for processing
      const notifyText = `${approverName} has approved this refund${agRef} — "${quote}" — <@${REFUND_CONTACTS.PROCESSING.id}> / <@${HARGO_USER_ID}>, this is good to go for processing.`;
      try {
        await app.client.chat.postMessage({
          token: SLACK_BOT_TOKEN,
          channel: CHANNELS.CUSTOMER_REFUNDS,
          thread_ts: msg.ts,
          text: notifyText,
          mrkdwn: true,
        });
        notified++;
        console.log(`[bill-ling] Approval notification posted in thread ${msg.ts}${agRef} (approved by ${approverName})`);
      } catch (err) {
        console.error(`[bill-ling] Failed to post approval notification in thread ${msg.ts}:`, err.message);
      }
    }
    // else: already notified but refund not yet posted — will re-check next hour

    approvalNotifiedThreads.set(msg.ts, refundPosted ? "confirmed" : today);
  }

  console.log(`[bill-ling] Approval monitor: scanned ${messages.length} threads, notified ${notified}.`);
}

// Cache: { data, summary, fetchedAt } — reused for drill-down queries within 2 hours
let lastRefundsCache = null;

// Count working days between two ISO date strings
function workingDaysBetween(fromIso, toIso) {
  if (!fromIso || fromIso >= toIso) return 0;
  let count = 0;
  let d = new Date(fromIso + "T12:00:00Z");
  const end = new Date(toIso + "T12:00:00Z");
  while (d < end) {
    d.setUTCDate(d.getUTCDate() + 1);
    const ds = d.toISOString().slice(0, 10);
    if (isWorkingDay(ds)) count++;
  }
  return count;
}

// Build a drill-down response from cached/fresh refunds data
function buildDrillDown(data, question) {
  if (!data) return null;
  const lower = question.toLowerCase();
  const today = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
  const wd = (iso) => workingDaysBetween(iso || today, today);
  const gbp = (n) => n != null ? `£${Number(n).toLocaleString("en-GB", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : "";

  const isBatch      = /batch|barclays|refund file|file.*confirm|confirm.*file/.test(lower);
  const isApproval   = /approv/.test(lower);
  const isProcessing = /process/.test(lower);
  const isCustomer   = /customer|bank detail|blocked/.test(lower);
  const isOutOfSla   = /out.?of.?sla|overdue|past.?sla|> ?2/.test(lower);

  let heading, items;

  if (isOutOfSla) {
    heading = "*Out of SLA items (> 2 working days)*";
    items = [
      ...(data.awaiting_approval        || []).filter(i => wd(i.date_iso) > 2).map(i => ({ ...i, _bucket: "Awaiting approval" })),
      ...(data.awaiting_processing       || []).filter(i => wd(i.date_iso) > 2).map(i => ({ ...i, _bucket: "Awaiting processing" })),
      ...(data.awaiting_customer_action  || []).filter(i => wd(i.date_iso) > 2).map(i => ({ ...i, _bucket: "Customer action" })),
      ...(data.batch_files               || []).filter(i => wd(i.date_iso) > 2).map(i => ({ ...i, _bucket: "Batch file" })),
    ];
  } else if (isBatch) {
    heading = "*Refund files awaiting Barclays confirmation*";
    items = (data.batch_files || []).map(i => ({ ...i, _bucket: "Batch file" }));
  } else if (isApproval) {
    heading = "*Awaiting approval*";
    items = (data.awaiting_approval || []).map(i => ({ ...i, _bucket: "Awaiting approval" }));
  } else if (isProcessing) {
    heading = "*Awaiting processing*";
    items = (data.awaiting_processing || []).map(i => ({ ...i, _bucket: "Awaiting processing" }));
  } else if (isCustomer) {
    heading = "*Awaiting customer action*";
    items = (data.awaiting_customer_action || []).map(i => ({ ...i, _bucket: "Customer action" }));
  } else {
    heading = "*All pending refund items*";
    items = [
      ...(data.awaiting_customer_action || []).map(i => ({ ...i, _bucket: "Awaiting customer action" })),
      ...(data.awaiting_approval        || []).map(i => ({ ...i, _bucket: "Awaiting approval" })),
      ...(data.awaiting_processing       || []).map(i => ({ ...i, _bucket: "Awaiting processing" })),
      ...(data.batch_files              || []).map(i => ({ ...i, _bucket: "Batch file" })),
    ];
  }

  if (!items.length) return "Nothing currently in that category.";

  const lines = [heading, ""];
  for (const item of items) {
    const days = item.date_iso ? wd(item.date_iso) : null;
    const daysStr = days !== null ? `${days} working day${days !== 1 ? "s" : ""}` : "";
    const slaFlag = days > 2 ? " ⚠️" : "";

    if (item._bucket === "Batch file") {
      const amt = item.amount ? gbp(item.amount) : "";
      const pmts = item.payments ? `${Number(item.payments).toLocaleString("en-GB")} payments` : "";
      lines.push(`• *${item.date}* — ${[pmts, amt].filter(Boolean).join(", ")} — ${daysStr} waiting${slaFlag}`);
    } else {
      lines.push(`• *${item.ref || "—"}* — ${item.description || ""} — ${daysStr}${slaFlag}`);
      if (isOutOfSla) lines.push(`  _${item._bucket}_`);
    }
    if (item.permalink) lines.push(`  <${item.permalink}|View thread>`);
  }
  return lines.filter(l => l !== null).join("\n");
}

async function getRefundsSummary() {
  try {
    const oldest = String(Math.floor((Date.now() - 30 * 24 * 60 * 60 * 1000) / 1000));

    // Fetch up to 100 messages from the last 30 days
    const histResp = await app.client.conversations.history({
      token: SLACK_BOT_TOKEN,
      channel: CHANNELS.CUSTOMER_REFUNDS,
      oldest,
      limit: 100,
    });

    const messages = histResp.messages || [];
    if (!messages.length) {
      return "No messages found in #customer-refunds-and-complaints in the last 30 days.";
    }

    // Skip bot messages (Billington's own reports etc.) before fetching threads —
    // they don't contain refund requests and are often very long.
    // Also skip messages already marked ✅ by the bot — no need to re-check those.
    const humanMessages = messages.filter((msg) => {
      if (msg.bot_id || msg.subtype) return false;
      if (BOT_USER_ID) {
        const alreadyDone = (msg.reactions || []).some(
          (r) => r.name === "white_check_mark" && (r.users || []).includes(BOT_USER_ID)
        );
        if (alreadyDone) return false;
      }
      return true;
    });

    // For each human message that has thread replies, fetch the full thread
    const threads = await Promise.all(
      humanMessages.map(async (msg) => {
        let replies = [];
        if (msg.reply_count > 0) {
          try {
            const threadResp = await app.client.conversations.replies({
              token: SLACK_BOT_TOKEN,
              channel: CHANNELS.CUSTOMER_REFUNDS,
              ts: msg.ts,
              limit: 50,
            });
            // replies[0] is the parent, skip it
            replies = (threadResp.messages || []).slice(1);
          } catch (e) {
            // ignore thread fetch errors
          }
        }
        const date = new Date(Number(msg.ts) * 1000).toLocaleDateString("en-GB");
        const tsClean = msg.ts.replace(".", "");
        const permalink = `https://joinraylo.slack.com/archives/${CHANNELS.CUSTOMER_REFUNDS}/p${tsClean}`;
        return { date, ts: msg.ts, permalink, userId: msg.user || "unknown", text: msg.text || "", replies };
      })
    );

    // Collect all unique user IDs across messages and replies, then resolve to real names
    const allUserIds = new Set();
    for (const t of threads) {
      if (t.userId) allUserIds.add(t.userId);
      for (const r of t.replies) { if (r.user) allUserIds.add(r.user); }
    }
    const userNames = {};
    await Promise.all([...allUserIds].map(async (uid) => {
      try {
        const info = await app.client.users.info({ token: SLACK_BOT_TOKEN, user: uid });
        userNames[uid] = info.user?.real_name || info.user?.name || uid;
      } catch {
        userNames[uid] = uid;
      }
    }));

    // Build compact transcript — include both name and Slack user ID so Claude can
    // emit <@USERID> mentions in the output for any person referenced.
    const fmtSender = (uid) => {
      const name = userNames[uid] || uid;
      return uid ? `${name} (<@${uid}>)` : name;
    };
    const transcript = threads
      .map((t) => {
        const sender = fmtSender(t.userId);
        const replyBlock = t.replies.length
          ? `\n  Replies:\n${t.replies.map((r) => {
              return `    - ${fmtSender(r.user)}: ${r.text || ""}`;
            }).join("\n")}`
          : "";
        return `[${t.date}] [ts:${t.ts}] [permalink:${t.permalink}] ${sender}: ${t.text}${replyBlock}`;
      })
      .join("\n\n");

    // --- Arrears check: find any refunds/GOGWs conditional on arrears clearance ---
    // Agreement IDs always look like A followed by 5 or 8 digits (e.g. A00307833 or A12345).
    // "agreement number", "agreement reference", "agreement ID", "agreement ref" all mean agreement_id.

    // Step 1: regex-scan transcript for all agreement IDs present
    const allRefsInTranscript = [...new Set(
      [...transcript.matchAll(/\bA\d{5}(?:\d{3})?\b/g)].map((m) => m[0])
    )];

    // Step 2: run arrears extraction and contact inference in parallel
    const arrearsExtractPrompt = allRefsInTranscript.length > 0
      ? `From this Slack transcript, which of these agreement IDs (${allRefsInTranscript.join(", ")}) have a refund or goodwill gesture that is explicitly conditional on the customer clearing their arrears first?
"Agreement number", "agreement reference", "agreement ref", and "agreement ID" all refer to the same thing.
Return ONLY a JSON array of the matching IDs, e.g. ["A00307833"]. If none, return [].

--- TRANSCRIPT ---
${transcript}
--- END ---`
      : null;

    const [arrearsRaw, contacts] = await Promise.all([
      arrearsExtractPrompt ? askClaude(arrearsExtractPrompt).catch(() => "[]") : Promise.resolve("[]"),
      inferRefundContacts(transcript),
    ]);

    let arrearsRefs = [];
    try {
      const m = arrearsRaw.match(/\[[\s\S]*?\]/);
      arrearsRefs = m ? JSON.parse(m[0]) : [];
    } catch (e) { /* ignore */ }

    let arrearsContext = "";
    if (arrearsRefs.length > 0) {
      console.log(`[bill-ling] Checking arrears for: ${arrearsRefs.join(", ")}`);
      try {
        const arrearsSql = `
          SELECT agreement_id, total_arrears
          FROM \`raylo-production.landing_billington.agreement_info\`
          WHERE agreement_id IN (${arrearsRefs.map((r) => `'${r}'`).join(",")})
        `;
        const rows = await runBigQueryRaw(arrearsSql);
        const arrearsMap = {};
        for (const row of rows) arrearsMap[row.agreement_id] = Number(row.total_arrears) || 0;

        arrearsContext = `\nLIVE ARREARS STATUS (from BigQuery — use this to determine approval for arrears-conditional items):\n`;
        for (const ref of arrearsRefs) {
          const amt = arrearsMap[ref];
          if (amt === undefined) {
            arrearsContext += `• ${ref}: not found in database — keep in bucket 1\n`;
          } else if (amt <= 0) {
            arrearsContext += `• ${ref}: total_arrears = £${Math.abs(amt).toFixed(2)} credit — ARREARS CLEARED → treat as APPROVED (move to bucket 2)\n`;
          } else {
            arrearsContext += `• ${ref}: total_arrears = £${amt.toFixed(2)} outstanding — still in arrears → keep in bucket 1\n`;
          }
        }
      } catch (e) {
        console.error("[bill-ling] arrears BigQuery error:", e.message);
        arrearsContext = `\nNote: Could not check live arrears data (BigQuery error: ${e.message})\n`;
      }
    }
    // --- End arrears check ---
    const fmtContact = (arr, fallbackId, fallbackName) => {
      if (!arr || !arr.length) return `<@${fallbackId}> (${fallbackName}) — fallback`;
      return arr.slice(0, 2).map((p, i) =>
        `<@${p.id}> (${p.name}, ${p.count}x in last 30d)${i === 1 ? " — escalate to" : ""}`
      ).join(" → ");
    };
    const responsibilityContext = contacts ? `
RESPONSIBILITY (inferred from last 30 days of channel activity — primary contact first, escalation second):
• Approvals: ${fmtContact(contacts.approval, REFUND_CONTACTS.PROCESSING_ESCALATION.id, "Hargo")}
• Processing refunds: ${fmtContact(contacts.processing, REFUND_CONTACTS.PROCESSING.id, "Thomas Crudden")}
• Batch file / Barclays upload: ${fmtContact(contacts.batch_file, REFUND_CONTACTS.BATCH_FILE_UNCONFIRMED.id, "Aoibheann McCann")}
• Barclays confirmation: ${fmtContact(contacts.barclays_confirmation, REFUND_CONTACTS.BATCH_FILE_UNCONFIRMED.id, "Aoibheann McCann")}
` : `
RESPONSIBILITY (fallback — could not infer from history):
• Batch file not confirmed uploaded to Barclays → <@${REFUND_CONTACTS.BATCH_FILE_UNCONFIRMED.id}> (Aoibheann McCann)
• Finance escalation → <@${REFUND_CONTACTS.FINANCE_ESCALATION.id}> (Darragh Keogh)
• Processing individual refund requests → <@${REFUND_CONTACTS.PROCESSING.id}> (Thomas Crudden)
• Refund processing escalation → <@${REFUND_CONTACTS.PROCESSING_ESCALATION.id}> (Hargo)
`;
    // --- End responsibility inference ---

    const today = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });

    const prompt = `You are Billington, an AI assistant for Raylo's billing operations team.
Classify unresolved items from the #customer-refunds-and-complaints transcript below.${arrearsContext}

RULES:
APPROVAL: ${APPROVAL_KEYWORDS.map(k => `"${k}"`).join(", ")}, or a manager directing someone to process. Questions and comments are NOT approval.
PROCESSING: ${PROCESSING_KEYWORDS.map(k => `"${k}"`).join(", ")}.
ARREARS: if LIVE ARREARS STATUS says CLEARED → approved. If still in arrears → bucket 1.
BATCH FILE: a channel-level message (not a thread reply) announcing a refund file for Barclays — has payment count and £ total.
BARCLAYS CONFIRMED: a reply or follow-up with "paid", "paid [date]", "authorised", "confirmed", "uploaded to Barclays".
BLOCKED: a refund that can't proceed (bad bank details, customer not responding, open unresolved question/task).

User IDs appear as (<@USERID>) after names. thread_ts values appear as [ts:XXXXXXXXX.XXXXXX] in the transcript.

Return ONLY valid JSON in this exact structure (no other text):
{
  "awaiting_customer_action": [
    {
      "ref": "AXXXXXXXX",
      "description": "brief description",
      "thread_ts": "XXXXXXXXX.XXXXXX",
      "date_iso": "YYYY-MM-DD",
      "issue": "what customer needs to do",
      "permalink": "https://...",
      "chase_message": "message to post in the thread using <@USERID> mentions"
    }
  ],
  "awaiting_approval": [
    {
      "ref": "AXXXXXXXX",
      "description": "brief description",
      "thread_ts": "XXXXXXXXX.XXXXXX",
      "date_iso": "YYYY-MM-DD",
      "requested_by": "<@USERID>",
      "permalink": "https://...",
      "quote": "exact quote or no explicit signal found",
      "chase_message": "<@USERID> — this refund has been waiting for approval. Can you approve or decline?"
    }
  ],
  "awaiting_processing": [
    {
      "ref": "AXXXXXXXX",
      "description": "brief description",
      "thread_ts": "XXXXXXXXX.XXXXXX",
      "date_iso": "YYYY-MM-DD",
      "requested_by": "<@USERID>",
      "approved_by": "<@USERID>",
      "permalink": "https://...",
      "quote": "exact quote",
      "chase_message": "<@USERID> — this refund was approved. Can you confirm when this will be processed?"
    }
  ],
  "batch_files": [
    {
      "date": "DD/MM/YYYY",
      "date_iso": "YYYY-MM-DD",
      "thread_ts": "XXXXXXXXX.XXXXXX",
      "payments": 0,
      "amount": 0.00,
      "permalink": "https://...",
      "chase_message": "<@${REFUND_CONTACTS.BATCH_FILE_UNCONFIRMED.id}> can you confirm if this refund file has been uploaded and processed on Barclays?"
    }
  ],
  "resolved": [
    {
      "thread_ts": "XXXXXXXXX.XXXXXX"
    }
  ]
}

"resolved" = ONLY include a thread here if it meets ONE of these two criteria exactly:
1. An adhoc refund or GOGW request where a PROCESSING signal ("processed", "done", "sent", "refunded", "actioned", "completed", "paid", "posted", "transferred") has been confirmed in the thread replies — i.e. someone has confirmed the money has actually been sent to the customer.
2. A batch file announcement where Barclays confirmation has been explicitly given in the thread replies ("paid", "authorised", "confirmed", "uploaded to Barclays").
Do NOT include: general messages, status updates, approvals without processing confirmation, questions, comments, or anything without a clear end-to-end completion signal. When in doubt, leave it out. Only include the thread_ts of the original parent message.

--- TRANSCRIPT ---
${transcript}
--- END ---`;

    const raw = await askClaude(prompt);
    const jsonMatch = raw.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error("Claude did not return valid JSON");
    const data = JSON.parse(jsonMatch[0]);

    // Stamp ✅ reaction on resolved threads (fire-and-forget)
    markResolvedThreads(data.resolved || []).catch(() => {});

    // Build high-level count summary
    const wd = (iso) => workingDaysBetween(iso, today);
    const inSla  = (iso) => wd(iso) <= 2;
    const outSla = (iso) => wd(iso) > 2;

    const customer    = (data.awaiting_customer_action || []);
    const approval    = (data.awaiting_approval || []);
    const processing  = (data.awaiting_processing || []);
    const batches     = (data.batch_files || []);

    const approvalIn   = approval.filter(i => inSla(i.date_iso)).length;
    const approvalOut  = approval.filter(i => outSla(i.date_iso)).length;
    const processIn    = processing.filter(i => inSla(i.date_iso)).length;
    const processOut   = processing.filter(i => outSla(i.date_iso)).length;
    const batchIn      = batches.filter(i => inSla(i.date_iso)).length;
    const batchOut     = batches.filter(i => outSla(i.date_iso)).length;

    const lines = [
      `*Refunds update — ${fmtDate(today)}*`,
      ``,
    ];

    if (customer.length > 0) {
      lines.push(`*Awaiting customer action*`);
      lines.push(`• ${customer.length} item${customer.length > 1 ? "s" : ""}`);
      lines.push(``);
    }

    if (approval.length === 0) {
      lines.push(`✅ *Adhoc refunds/GOGW — awaiting approval*`);
      lines.push(`• None`);
    } else {
      lines.push(`*Adhoc refunds/GOGW — awaiting approval*`);
      lines.push(`• ${approvalIn} in SLA (≤ 2 working days)`);
      if (approvalOut > 0) lines.push(`• ${approvalOut} out of SLA (> 2 working days)`);
    }
    lines.push(``);

    if (processing.length === 0) {
      lines.push(`✅ *Adhoc refunds/GOGW — awaiting processing*`);
      lines.push(`• None`);
    } else {
      lines.push(`*Adhoc refunds/GOGW — awaiting processing*`);
      lines.push(`• ${processIn} in SLA (≤ 2 working days)`);
      if (processOut > 0) lines.push(`• ${processOut} out of SLA (> 2 working days)`);
    }
    lines.push(``);

    if (batches.length === 0) {
      lines.push(`✅ *Refund files — awaiting Barclays confirmation*`);
      lines.push(`• None`);
    } else {
      lines.push(`*Refund files — awaiting Barclays confirmation*`);
      const fmtGbp = (n) => n != null ? `£${Number(n).toLocaleString("en-GB", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : "";
      for (const b of batches) {
        const days = wd(b.date_iso);
        const daysStr = `${days} working day${days !== 1 ? "s" : ""}`;
        const slaFlag = days > 2 ? " ⚠️" : "";
        const amt = b.amount ? fmtGbp(b.amount) : "";
        const pmts = b.payments ? `${Number(b.payments).toLocaleString("en-GB")} payments` : "";
        const detail = [pmts, amt].filter(Boolean).join(", ");
        const link = b.permalink ? ` — <${b.permalink}|View thread>` : "";
        lines.push(`• ${b.date} — ${detail} — ${daysStr}${slaFlag}${link}`);
      }
    }

    const summary = lines.join("\n");
    // Populate cache for drill-down queries
    lastRefundsCache = { summary, data, fetchedAt: Date.now() };
    return { summary, data };

  } catch (err) {
    console.error("[bill-ling] getRefundsSummary error:", err.message);
    return { summary: `Error reading the refunds channel: ${err.message}`, data: null };
  }
}

// Returns cached refunds data if fresh (< 2 hours), otherwise re-fetches
async function getRefundsData() {
  const TWO_HOURS = 2 * 60 * 60 * 1000;
  if (lastRefundsCache && (Date.now() - lastRefundsCache.fetchedAt) < TWO_HOURS) {
    return lastRefundsCache;
  }
  return await getRefundsSummary();
}

// True if the message is asking for specific items rather than a top-level count
function isRefundsDrillDown(text) {
  return /\b(which|what (are|were|is|was)|show me|list|give me|details?|tell me about|out.?of.?sla|overdue|past.?sla|specific|share the|link to|thread for|more explicit|more detail|more info|be more|can you be|date and amount|outstanding)\b/i.test(text);
}

/**
 * Attempt to answer a bacs_files question, with one automatic retry if the first
 * WHERE clause produces no results. On retry, Claude is told what was tried and
 * asked to suggest a corrected date/clause.
 * Returns { result: string, warned: boolean }
 */
async function resolveBacsFiles(question, today) {
  const attempt = async (where) => {
    const report = await getBacsFileReport(where);
    if (report) return report;
    const forecast = await getPreBatchForecast(where);
    return forecast || null;
  };

  // First attempt
  const where1 = await buildBacsFileWhere(question, today);
  console.log(`[bill-ling] bacs_files attempt 1 WHERE: ${where1}`);

  // FORECAST: prefix means go straight to pre-batch forecast (not-yet-generated batches)
  if (where1.toUpperCase().startsWith("FORECAST:")) {
    const forecastWhere = where1.replace(/^FORECAST:\s*/i, "").trim();
    console.log(`[bill-ling] FORECAST path: ${forecastWhere}`);
    const forecast = await getPreBatchForecast(forecastWhere, 1);
    return { result: forecast || "No upcoming ungenerated batches found.", warned: false };
  }

  const result1 = await attempt(where1);
  if (result1) return { result: result1, warned: false };

  // First attempt returned nothing — ask Claude to re-reason
  console.log(`[bill-ling] bacs_files attempt 1 returned no data — retrying with corrected clause`);
  const retryPrompt = `You are helping Billington, a Slack bot for Raylo.

The user asked: "${question}"
Today is ${today}.

Billington tried the BigQuery WHERE clause: ${where1}
But got zero rows from the table \`raylo-production.landing_billington.bacs_monitoring\`.

The table has a \`due_date\` column (DATE) and a \`batch_created_at\` column (TIMESTAMP).
UK bank holidays (non-working days): ${[...UK_BANK_HOLIDAYS].sort().join(", ")}

Working days skip weekends AND the bank holidays above.
"Working day before 07/04/2026" = 02/04/2026 (skip Easter Mon 06/04, Sun 05/04, Sat 04/04, Good Fri 03/04).

Suggest a corrected BigQuery WHERE sub-clause (no WHERE keyword, no semicolon) that is more likely to find data.
Reply with ONLY the WHERE sub-clause, nothing else.`;

  const where2 = (await askClaude(retryPrompt)).trim().replace(/;$/, "").trim();
  console.log(`[bill-ling] bacs_files attempt 2 WHERE: ${where2}`);
  const result2 = await attempt(where2);
  if (result2) return { result: result2, warned: false };

  // Both attempts failed
  return {
    result: `I couldn't find any data matching that request. I tried:\n• \`${where1}\`\n• \`${where2}\`\n\nCould you clarify the date or what you're looking for?`,
    warned: true,
  };
}

/**
 * For future dates where no batch has been generated yet, return a forecast
 * based on total_instalments_due vs the 45-day rolling average.
 * Returns null if no instalment data exists for the date range.
 */
async function getPreBatchForecast(whereClause, limit = null) {
  try {
    // Instalment data for dates where no batch has been generated yet
    const forecastSql = `
      SELECT
        due_date,
        total_instalments_due,
        estimated_late_fees_to_apply,
        estimated_direct_debits_bounce,
        estimated_direct_debits_called_for,
        time_to_create_payment_batch,
        time_to_send_payment_file,
        time_to_apply_fees
      FROM \`raylo-production.landing_billington.bacs_monitoring\`
      WHERE total_instalments_due > 0
        AND batch_created_at IS NULL
        AND ${whereClause}
      ORDER BY due_date ASC
      ${limit ? `LIMIT ${limit}` : ""}
    `;

    // Average timing from recent completed batches — time columns are HH:MM:SS strings,
    // convert to seconds via TIME_DIFF then back to a display string in JS
    const statsSql = `
      SELECT
        AVG(total_instalments_due) AS avg_instalments,
        AVG(TIME_DIFF(SAFE_CAST(time_to_create_payment_batch AS TIME), TIME '00:00:00', SECOND)) AS avg_create_secs,
        AVG(TIME_DIFF(SAFE_CAST(time_to_send_payment_file AS TIME), TIME '00:00:00', SECOND)) AS avg_send_secs,
        AVG(TIME_DIFF(SAFE_CAST(time_to_apply_fees AS TIME), TIME '00:00:00', SECOND)) AS avg_apply_secs
      FROM \`raylo-production.landing_billington.bacs_monitoring\`
      WHERE batch_created_at IS NOT NULL
        AND due_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY)
        AND total_instalments_due > 0
    `;

    const [[forecastRows], [statsRows]] = await Promise.all([
      bigquery.query({ query: forecastSql, location: "EU" }),
      bigquery.query({ query: statsSql, location: "EU" }),
    ]);

    if (!forecastRows.length) return null;

    const num = (v) => {
      if (v === null || v === undefined) return 0;
      if (typeof v === "object" && "value" in v) return Number(v.value);
      return Number(v);
    };

    // Helper: convert seconds → "H:MM:SS" display string
    const secsToHMS = (secs) => {
      if (!secs || secs <= 0) return null;
      const h = Math.floor(secs / 3600);
      const m = Math.floor((secs % 3600) / 60);
      const s = Math.round(secs % 60);
      return h > 0
        ? `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`
        : `${m}:${String(s).padStart(2, "0")}`;
    };

    const stats = statsRows[0] || {};
    const avgInstalments = num(stats.avg_instalments);
    const avgCreateSecs = num(stats.avg_create_secs);
    const avgSendSecs   = num(stats.avg_send_secs);
    const avgApplySecs  = num(stats.avg_apply_secs);

    // Parse "HH:MM:SS" or "MM:SS" string → seconds
    const parseHMS = (s) => {
      if (!s || s === "null" || s === "0") return 0;
      const parts = s.split(":").map(Number);
      if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
      if (parts.length === 2) return parts[0] * 60 + parts[1];
      return 0;
    };

    // For each forecast row, compute genByDate and its previous working day (the due date
    // whose late fees will be applying on the same day as batch generation).
    const genByDates = forecastRows.map((row) => {
      let d = formatBQValue(row.due_date);
      for (let i = 0; i < 3; i++) d = prevWorkingDay(d);
      return d;
    });
    const prevDueDates = [...new Set(genByDates.map((d) => prevWorkingDay(d)))];
    const prevFeesSql = `
      SELECT due_date, time_to_apply_fees, estimated_late_fees_to_apply
      FROM \`raylo-production.landing_billington.bacs_monitoring\`
      WHERE due_date IN (${prevDueDates.map((d) => `'${d}'`).join(",")})
    `;
    const [prevFeeRows] = await bigquery.query({ query: prevFeesSql, location: "EU" });
    const prevFeeMap = {};
    for (const r of prevFeeRows) {
      prevFeeMap[formatBQValue(r.due_date)] = {
        time:  formatBQValue(r.time_to_apply_fees),
        count: num(r.estimated_late_fees_to_apply),
      };
    }

    const blocks = forecastRows.map((row, idx) => {
      const rawDate = formatBQValue(row.due_date);
      const [y, m, d] = rawDate.split("-");
      const displayDate = `${d}/${m}/${y}`;
      const instCount = num(row.total_instalments_due);

      // Calculate generate-by date: 3 working days before due date
      const genByDate = genByDates[idx];
      const genByDisplay = fmtDate(genByDate);

      // Previous due date whose late fees land on the generate-by day
      const prevDueDate   = prevWorkingDay(genByDate);
      const prevFeeEntry  = prevFeeMap[prevDueDate] || {};
      const prevApplyTime = prevFeeEntry.time;
      const prevApplyCount = prevFeeEntry.count || 0;
      const prevApplyDisplay = (prevApplyTime && prevApplyTime !== "null" && prevApplyTime !== "0")
        ? prevApplyTime : null;

      // Size vs average
      const ratio = avgInstalments > 0 ? instCount / avgInstalments : 1;
      const pctAbove = Math.round((ratio - 1) * 100);
      let sizeFlag = null;
      if (ratio >= 1.5)       sizeFlag = `⚠️ +${pctAbove}% above 45-day average — significantly larger than normal`;
      else if (ratio >= 1.15) sizeFlag = `↑ +${pctAbove}% above 45-day average`;
      else if (ratio <= 0.75) sizeFlag = `↓ ${Math.round((1 - ratio) * 100)}% below average`;

      // Identify preceding bank holidays / weekend rollover
      const rollovers = [];
      let checkDate = rawDate;
      for (let i = 0; i < 5; i++) {
        const prev = new Date(checkDate + "T12:00:00Z");
        prev.setUTCDate(prev.getUTCDate() - 1);
        const prevStr = prev.toISOString().slice(0, 10);
        const dow = prev.getUTCDay();
        if (UK_BANK_HOLIDAYS.has(prevStr)) {
          rollovers.push(`${UK_BANK_HOLIDAY_NAMES[prevStr] || prevStr} (${fmtDate(prevStr)})`);
          checkDate = prevStr;
        } else if (dow === 0 || dow === 6) {
          checkDate = prevStr; // step over weekends silently
        } else {
          break;
        }
      }

      // Timing — use row values if populated, otherwise fall back to average from recent batches
      const rawCreate = formatBQValue(row.time_to_create_payment_batch);
      const rawSend   = formatBQValue(row.time_to_send_payment_file);
      const rawApply  = formatBQValue(row.time_to_apply_fees);

      const createSecs = parseHMS(rawCreate !== "null" && rawCreate !== "0" ? rawCreate : null)
                       || avgCreateSecs;
      const sendSecs   = parseHMS(rawSend   !== "null" && rawSend   !== "0" ? rawSend   : null)
                       || avgSendSecs;
      const createAndSendSecs = createSecs + sendSecs;
      const displayCreateAndSend = createAndSendSecs > 0 ? secsToHMS(createAndSendSecs) : null;

      const displayApply = (rawApply && rawApply !== "null" && rawApply !== "0") ? rawApply
                         : secsToHMS(avgApplySecs);

      // Next working day after due date (when fees are applied)
      const applyDay = fmtDate(nextWorkingDay(rawDate));

      // Estimates for current due date
      const estLateFees      = num(row.estimated_late_fees_to_apply);
      const estDDsCalledFor  = num(row.estimated_direct_debits_called_for);

      // ⚠️ on generate-by day if create+send+prev_apply_fees > 4 hours total
      const prevApplySecs      = parseHMS(prevApplyDisplay);
      const totalGenByDaySecs  = createAndSendSecs + prevApplySecs;
      const genByTimeWarning   = prevApplyDisplay && totalGenByDaySecs > 4 * 3600;

      // File generation assessment: create+send vs 4h threshold
      const fileGenOk = createAndSendSecs > 0 && createAndSendSecs < 4 * 3600;
      const fileGenAssessment = createAndSendSecs > 0
        ? (fileGenOk
          ? `✅ No issues expected generating payment file`
          : `⚠️ File generation estimated to take over 4 hours — monitor closely`)
        : null;

      const lines = [
        `*BACS batch forecast for instalments due on: ${displayDate}*`,
        `Instalments due: #${instCount.toLocaleString("en-GB")}`,
        estDDsCalledFor > 0
          ? `Estimated count of Direct Debits: #${Math.round(estDDsCalledFor).toLocaleString("en-GB")}`
          : null,
        displayCreateAndSend
          ? `Estimated time to create payment batch and send payment file: ${displayCreateAndSend}`
          : null,
        fileGenAssessment,
        rollovers.length > 0 ? `📅 Includes rollovers from: ${rollovers.join(", ")}` : null,
        ``,
        `Batch must be generated by: *${genByDisplay}*`,
        prevApplyDisplay
          ? `${genByTimeWarning ? "⚠️ " : ""}Late payment fees for ${fmtDate(prevDueDate)} also applying on ${genByDisplay} — est.${prevApplyCount > 0 ? ` #${Math.round(prevApplyCount).toLocaleString("en-GB")} fees |` : ""} ${prevApplyDisplay}`
          : null,
        ``,
        estLateFees > 0
          ? `Estimated late fees to apply next working day (${applyDay}): #${Math.round(estLateFees).toLocaleString("en-GB")}`
          : null,
        displayApply
          ? `Estimated time to apply late fees (${applyDay}): ${displayApply}`
          : null,
      ];

      return lines.filter((v) => v !== null).join("\n");
    });

    return blocks.join("\n\n---\n\n");
  } catch (err) {
    console.error("[bill-ling] getPreBatchForecast error:", err.message);
    return null;
  }
}

/**
 * 3pm cron: send today's batch summary to Hargo.
 */
async function runBacsFileSummary() {
  console.log("[bill-ling] Running 3pm BACS file summary...");
  try {
    const report = await getBacsFileReport("due_date > CURRENT_DATE()");
    const text = report
      ? `*BACS Batch Update — ${new Date().toLocaleDateString("en-GB", { timeZone: "Europe/London" })}*\n\n${report}`
      : "No upcoming BACS batches found.";
    await app.client.chat.postMessage({
      token: SLACK_BOT_TOKEN,
      channel: CHANNELS.DATA_BILLING_UPDATES,
      text,
      mrkdwn: true,
    });
    console.log("[bill-ling] 3pm BACS file summary posted to #data-billing-updates.");
  } catch (err) {
    console.error("[bill-ling] Error running BACS file summary:", err);
    try { await dmHargo(`Error generating BACS batch summary: ${err.message}`); } catch (_) {}
  }
}

// ---------------------------------------------------------------------------
// Make.com
// ---------------------------------------------------------------------------

const MAKE_API_KEY = process.env.MAKE_API_KEY;
const MAKE_BASE = "https://eu1.make.com/api/v2";
// Raylo Team (74172) — only team Billington queries
const MAKE_TEAM_IDS = [74172];
const MAKE_BILLING_FOLDERS = new Set([217646, 283892, 141356]);

let _makeScenarioCache = null;
let _makeScenarioCacheTime = 0;
const MAKE_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

async function getAllMakeScenarios() {
  if (_makeScenarioCache && Date.now() - _makeScenarioCacheTime < MAKE_CACHE_TTL) {
    return _makeScenarioCache;
  }
  const headers = { Authorization: `Token ${MAKE_API_KEY}` };
  const results = await Promise.all(
    MAKE_TEAM_IDS.map((teamId) =>
      fetch(`${MAKE_BASE}/scenarios?teamId=${teamId}&limit=500`, { headers }).then((r) => r.json())
    )
  );
  _makeScenarioCache = results.flatMap((r) => r.scenarios || []);
  _makeScenarioCacheTime = Date.now();
  return _makeScenarioCache;
}

function formatScenario(s) {
  const enabled = s.isActive ? "ON" : s.isPaused ? "PAUSED" : "OFF";
  const running = s.iswaiting ? "YES — currently executing" : "NO — not currently executing";
  const schedule = s.scheduling?.type === "on-demand"
    ? "on-demand (triggered manually or by another scenario)"
    : s.scheduling?.type === "immediately"
    ? `scheduled every ${Math.round((s.scheduling.interval || 900) / 60)} min`
    : s.scheduling?.type
      ? s.scheduling.type
      : "unknown schedule";
  const next = s.nextExec
    ? `next run: ${new Date(s.nextExec).toLocaleString("en-GB", { timeZone: "Europe/London" })}`
    : s.scheduling?.type === "on-demand" ? "no scheduled next run (on-demand)" : "no next run scheduled";
  const lastEdit = s.lastEdit
    ? `last edited: ${new Date(s.lastEdit).toLocaleString("en-GB", { timeZone: "Europe/London" })}`
    : "";
  return [
    `Name: ${s.name} (ID: ${s.id})`,
    `Enabled: ${enabled}`,
    `Currently running: ${running}`,
    `Schedule: ${schedule}`,
    `Next run: ${next}`,
    lastEdit,
  ].filter(Boolean).join("\n");
}

async function getLastExecution(scenarioId) {
  try {
    const res = await fetch(
      `https://eu1.make.com/api/v1/scenarios/${scenarioId}/logs?limit=3`,
      { headers: { Authorization: `Token ${MAKE_API_KEY}` } }
    );
    const data = await res.json();
    const logs = (data.scenarioLogs || []).filter((l) => l.eventType === "EXECUTION_END");
    if (!logs.length) return null;
    const last = logs[0];
    const endTime = new Date(last.timestamp).toLocaleString("en-GB", { timeZone: "Europe/London" });
    const durationMins = last.duration ? (last.duration / 60000).toFixed(1) : null;
    const statusText = last.status === 1 ? "success" : last.status === 2 ? "warning" : last.status === 3 ? "error" : `status ${last.status}`;
    return `Last execution: ${endTime} — ${statusText}${durationMins ? `, ran for ${durationMins} min` : ""}${last.operations ? `, ${last.operations.toLocaleString()} operations` : ""}`;
  } catch {
    return null;
  }
}

async function findMakeScenario(question) {
  if (!MAKE_API_KEY) return "Make.com API key not configured.";
  try {
    const scenarios = await getAllMakeScenarios();
    const nameList = scenarios.map((s) => `${s.id}: ${s.name}`).join("\n");
    const matchedId = await askClaude(
      `Question: "${question}"\n\nWhich Make.com scenario ID best matches what is being asked about? Reply with ONLY the numeric ID. If nothing matches, reply with "none".\n\n${nameList}`
    );
    const id = parseInt(matchedId.trim(), 10);
    if (!id) return "I couldn't find a matching scenario for that.";
    const scenario = scenarios.find((s) => s.id === id);
    if (!scenario) return "I couldn't find a matching scenario for that.";
    const lastExec = await getLastExecution(id);
    return [formatScenario(scenario), lastExec].filter(Boolean).join("\n");
  } catch (err) {
    console.error("[bill-ling] Make.com error:", err.message);
    return `Make.com error: ${err.message}`;
  }
}

async function fetchMakeScenarios(nameFilter, statusFilter) {
  if (!MAKE_API_KEY) return "Make.com API key not configured.";
  try {
    const scenarios = await getAllMakeScenarios();
    let filtered = nameFilter
      ? scenarios.filter((s) =>
          nameFilter.toLowerCase().split(/\s+/).filter(Boolean)
            .some((t) => s.name.toLowerCase().includes(t))
        )
      : scenarios;
    // For list requests, always restrict to billing folders unless a name filter is given
    if (!nameFilter) filtered = filtered.filter((s) => MAKE_BILLING_FOLDERS.has(s.folderId));
    // Apply status filter: "active" = isActive true, "paused" = isPaused true, "off" = both false
    if (statusFilter === "active") filtered = filtered.filter((s) => s.isActive && !s.isPaused);
    else if (statusFilter === "paused") filtered = filtered.filter((s) => s.isPaused);
    else if (statusFilter === "off") filtered = filtered.filter((s) => !s.isActive && !s.isPaused);
    if (!filtered.length) return nameFilter ? `No scenarios found matching "${nameFilter}".` : "No scenarios found.";
    return filtered.map(formatScenario).join("\n\n");
  } catch (err) {
    console.error("[bill-ling] Make.com error:", err.message);
    return `Make.com error: ${err.message}`;
  }
}

// Determine if a Make.com question is asking for a list vs a specific scenario
function isMakeListRequest(text) {
  return /\b(list|show|all|every|what are|which)\b/i.test(text);
}

// ---------------------------------------------------------------------------
// Gmail
// ---------------------------------------------------------------------------

let gmailClient = null;

function initGmail() {
  const { GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET, GMAIL_REFRESH_TOKEN } = process.env;
  if (!GMAIL_CLIENT_ID || !GMAIL_CLIENT_SECRET || !GMAIL_REFRESH_TOKEN) {
    console.log("[bill-ling] Gmail not configured — skipping (run gmail-auth.js to set up).");
    return;
  }
  const auth = new google.auth.OAuth2(GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET);
  auth.setCredentials({ refresh_token: GMAIL_REFRESH_TOKEN });
  gmailClient = google.gmail({ version: "v1", auth });
  console.log("[bill-ling] Gmail client initialised.");
}

/**
 * Search Gmail and return a formatted summary of matching messages.
 * @param {string} query  Gmail search query (e.g. "from:someone subject:billing")
 * @param {number} maxResults
 */
async function fetchEmails(query, maxResults = 20) {
  if (!gmailClient) return "Gmail is not connected. Run gmail-auth.js to set it up.";
  try {
    const listRes = await gmailClient.users.messages.list({
      userId: "me",
      q: query,
      maxResults,
    });
    const messages = listRes.data.messages || [];
    if (!messages.length) return "No emails found matching that query.";

    const details = await Promise.all(
      messages.map((m) =>
        gmailClient.users.messages.get({
          userId: "me",
          id: m.id,
          format: "metadata",
          metadataHeaders: ["From", "To", "Subject", "Date"],
        })
      )
    );

    return details
      .map((res) => {
        const h = {};
        (res.data.payload.headers || []).forEach((hdr) => { h[hdr.name] = hdr.value; });
        const snippet = res.data.snippet || "";
        return `From: ${h.From || "?"}\nDate: ${h.Date || "?"}\nSubject: ${h.Subject || "(no subject)"}\nPreview: ${snippet}`;
      })
      .join("\n\n---\n\n");
  } catch (err) {
    console.error("[bill-ling] Gmail error:", err.message);
    return `Gmail error: ${err.message}`;
  }
}

/**
 * Get the full plain-text body of a single email by message ID.
 */
async function getEmailBody(messageId) {
  if (!gmailClient) return "Gmail is not connected.";
  try {
    const res = await gmailClient.users.messages.get({ userId: "me", id: messageId, format: "full" });
    const parts = res.data.payload.parts || [res.data.payload];
    for (const part of parts) {
      if (part.mimeType === "text/plain" && part.body && part.body.data) {
        return Buffer.from(part.body.data, "base64").toString("utf-8");
      }
    }
    return res.data.snippet || "(no readable body)";
  } catch (err) {
    return `Gmail error: ${err.message}`;
  }
}

// ---------------------------------------------------------------------------
// Daily summary
// ---------------------------------------------------------------------------

/**
 * Fetch messages from all monitored channels, build a prompt, send to Claude,
 * and DM the result to Hargo.
 */
async function runDailySummary() {
  console.log("[bill-ling] Running daily summary...");

  try {
    // Fetch all channels in parallel
    const channelEntries = Object.entries(CHANNELS);
    const histories = await Promise.all(
      channelEntries.map(([, id]) => fetchChannelHistory(id))
    );

    // Build the formatted channel block for the prompt
    let channelBlock = "";
    channelEntries.forEach(([key, id], index) => {
      const name = CHANNEL_NAMES[id] || key;
      channelBlock += `\n\n--- ${name} (${id}) ---\n${histories[index]}`;
    });

    const userMessage =
      "Please produce the daily channel summary for all 5 channels based on the messages below. Format it clearly for Slack.\n\n" +
      channelBlock;

    const summary = await askClaude(userMessage);

    await dmHargo(summary);

    console.log("[bill-ling] Daily summary sent to Hargo.");
  } catch (err) {
    console.error("[bill-ling] Error running daily summary:", err);
    // Attempt to notify Hargo of the failure rather than silently dropping it
    try {
      await dmHargo(
        `Bill Ling here. I ran into an error generating today's summary: ${err.message}. I'll try again at the next scheduled run.`
      );
    } catch (notifyErr) {
      console.error(
        "[bill-ling] Could not notify Hargo of summary error:",
        notifyErr.message
      );
    }
  }
}

// ---------------------------------------------------------------------------
// Cron: 8am Mon-Fri — BACS batch forecast (next ungenerated batch)
// ---------------------------------------------------------------------------
async function runBacsBatchForecastCron() {
  const today = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
  if (!isWorkingDay(today)) {
    console.log(`[bill-ling] BACS forecast cron: skipping — ${today} is not a working day.`);
    return;
  }
  console.log("[bill-ling] Running 9am BACS batch forecast cron...");
  try {
    const forecast = await getPreBatchForecast(`due_date > '${today}'`, 1);
    const text = forecast || "No upcoming ungenerated BACS batches found.";
    await app.client.chat.postMessage({
      token: SLACK_BOT_TOKEN,
      channel: CHANNELS.DATA_BILLING_UPDATES,
      text,
      mrkdwn: true,
    });
    console.log("[bill-ling] 8am BACS batch forecast posted to #data-billing-updates.");
  } catch (err) {
    console.error("[bill-ling] Error running BACS batch forecast cron:", err);
    try { await dmHargo(`Error generating BACS batch forecast: ${err.message}`); } catch (_) {}
  }
}

cron.schedule(
  "0 9 * * 1-5",
  () => { runBacsBatchForecastCron(); },
  { timezone: "Europe/London" }
);
console.log("[bill-ling] BACS batch forecast cron scheduled for 09:00 Mon-Fri (Europe/London).");

// ---------------------------------------------------------------------------
// Cron: 3pm Mon-Fri — BACS batch file summary
cron.schedule(
  "0 15 * * 1-5",
  () => { runBacsFileSummary(); },
  { timezone: "Europe/London" }
);
console.log("[bill-ling] BACS file summary cron scheduled for 15:00 Mon-Fri (Europe/London).");

// ---------------------------------------------------------------------------
// Cron: 4pm Mon-Fri — Refunds summary + thread chasers
cron.schedule(
  "0 8 * * 1-5",
  async () => {
    console.log("[bill-ling] Running 8am refunds cron...");
    try {
      const { summary, data } = await getRefundsSummary();
      await app.client.chat.postMessage({
        token: SLACK_BOT_TOKEN,
        channel: CHANNELS.CUSTOMER_REFUNDS,
        text: summary,
        mrkdwn: true,
        unfurl_links: false,
        unfurl_media: false,
      });
      await postRefundChasers(data);
      console.log("[bill-ling] 8am refunds cron complete.");
    } catch (err) {
      console.error("[bill-ling] 8am refunds cron error:", err.message);
    }
  },
  { timezone: "Europe/London" }
);
console.log("[bill-ling] Refunds cron scheduled for 16:00 Mon-Fri (Europe/London).");

// Cron: Hourly approval monitor — check if Stephen approved refunds/GOGW
cron.schedule(
  "0 9-17 * * 1-5",
  async () => {
    console.log("[bill-ling] Running hourly approval monitor...");
    try {
      await checkRefundApprovals();
    } catch (err) {
      console.error("[bill-ling] Hourly approval monitor error:", err.message);
    }
  },
  { timezone: "Europe/London" }
);
console.log("[bill-ling] Approval monitor scheduled for 09:00-17:00 Mon-Fri (Europe/London).");

// Run approval monitor once on startup (catch anything missed during downtime)
setTimeout(async () => {
  console.log("[bill-ling] Running approval monitor on startup...");
  try {
    await checkRefundApprovals();
  } catch (err) {
    console.error("[bill-ling] Startup approval monitor error:", err.message);
  }
}, 10000); // 10s delay to let Socket Mode connect first

// ---------------------------------------------------------------------------
// Cron: Daily ARUDD report — 08:00 Tue-Fri, 10:30 Mon
// ---------------------------------------------------------------------------
async function runAruddCron() {
  console.log("[bill-ling] Running daily ARUDD report...");
  try {
    const { report, discrepancyPct, anchorBounces, gcFailed, dueDate, postedPct } = await buildAruddReport();
    await app.client.chat.postMessage({
      token: SLACK_BOT_TOKEN,
      channel: CHANNELS.DATA_BILLING_UPDATES,
      text: report,
      mrkdwn: true,
    });
    console.log("[bill-ling] ARUDD report posted.");

    // If < 95% posted in Anchor, check source table freshness and alert
    if (postedPct < 95 && gcFailed > 0) {
      console.log(`[bill-ling] ARUDD posted in Anchor ${postedPct}% < 95% — checking source tables...`);
      await checkAruddSourceTables(dueDate, discrepancyPct, anchorBounces, gcFailed);
    }
  } catch (err) {
    console.error("[bill-ling] ARUDD cron error:", err.message);
  }
}

async function checkAruddSourceTables(dueDate, discrepancyPct, anchorBounces, gcFailed) {
  try {
    const todayStr = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
    // Fetch the view definition to extract source tables dynamically
    const viewRows = await runBigQueryRaw(`
      SELECT view_definition
      FROM \`raylo-production.landing_billington.INFORMATION_SCHEMA.VIEWS\`
      WHERE table_name = 'bacs_monitoring'
    `);
    if (!viewRows.length) return;
    const viewSql = formatBQValue(viewRows[0].view_definition) || "";
    // Extract all project.dataset.table references
    const tableRefs = [...new Set(
      [...viewSql.matchAll(/`(raylo-production\.[a-z_]+\.[a-z_]+)`/gi)].map(m => m[1])
    )];
    if (!tableRefs.length) return;

    // Check last_modified_time for each source table
    const staleTables = [];
    for (const ref of tableRefs) {
      const [, dataset, table] = ref.split(".");
      if (!dataset || !table) continue;
      try {
        const meta = await runBigQueryRaw(`
          SELECT TIMESTAMP_MILLIS(last_modified_time) AS last_modified
          FROM \`raylo-production.${dataset}.__TABLES__\`
          WHERE table_id = '${table}'
          LIMIT 1
        `);
        if (!meta.length) continue;
        const lastMod = formatBQValue(meta[0].last_modified).slice(0, 10);
        if (lastMod < todayStr) {
          staleTables.push(`• \`${ref}\` — last updated ${lastMod}`);
        }
      } catch (_) { /* skip tables we can't access */ }
    }

    if (staleTables.length) {
      const alertMsg =
        `⚠️ *ARUDD Data Freshness Alert*\n` +
        `Discrepancy between Anchor (#${anchorBounces.toLocaleString("en-GB")}) and GoCardless (#${gcFailed.toLocaleString("en-GB")}) is *${discrepancyPct.toFixed(1)}%* — above the 5% threshold.\n\n` +
        `The following source tables for \`bacs_monitoring\` do not appear to have been updated today (${todayStr}):\n` +
        staleTables.join("\n") + "\n\n" +
        `This may explain the discrepancy. Please check the data pipeline.`;
      await app.client.chat.postMessage({
        token: SLACK_BOT_TOKEN,
        channel: CHANNELS.DATA_BILLING_ALERTS,
        text: alertMsg,
        mrkdwn: true,
      });
      console.log("[bill-ling] ARUDD source table staleness alert posted to data-billing-alerts.");
    } else {
      console.log("[bill-ling] All source tables updated today — discrepancy is not a data freshness issue.");
    }
  } catch (err) {
    console.error("[bill-ling] Source table check error:", err.message);
  }
}

// Mon 10:30, Tue-Fri 08:00
cron.schedule("30 10 * * 1", () => { runAruddCron(); }, { timezone: "Europe/London" });
cron.schedule("0 8 * * 2-5",  () => { runAruddCron(); }, { timezone: "Europe/London" });
console.log("[bill-ling] ARUDD cron scheduled: Mon 10:30, Tue-Fri 08:00 (Europe/London).");

// ---------------------------------------------------------------------------
// Cron: every minute — fire pending reminders
// ---------------------------------------------------------------------------
cron.schedule("* * * * *", async () => {
  const now = new Date();
  const pending = reminders.filter(r => !r.fired && !r.cancelled && new Date(r.fireAt) <= now);
  for (const r of pending) {
    try {
      if (r.actionType && SCHEDULED_ACTIONS[r.actionType]) {
        // This is a scheduled task — actually run the report
        console.log(`[bill-ling] Executing scheduled ${r.actionType} report (#${r.id})...`);
        await SCHEDULED_ACTIONS[r.actionType](r.channel);
        r.fired = true;
        console.log(`[bill-ling] Scheduled ${r.actionType} report #${r.id} executed.`);
      } else {
        // Normal reminder — post a descriptive message
        const mention = r.targetUserId ? `<@${r.targetUserId}>` : r.targetUserName;
        const text = `${mention} 🔔 *Reminder:* ${r.what}`;
        await app.client.chat.postMessage({
          token: SLACK_BOT_TOKEN,
          channel: r.channel,
          text,
          mrkdwn: true,
        });
        r.fired = true;
        console.log(`[bill-ling] Fired reminder #${r.id}: "${r.what}" for ${mention}`);
      }
    } catch (err) {
      console.error(`[bill-ling] Failed to fire reminder/task #${r.id}:`, err.message);
    }
  }
  if (pending.length > 0) saveReminders();
}, { timezone: "Europe/London" });

// ---------------------------------------------------------------------------
// Event: app_mention
// ---------------------------------------------------------------------------

app.event("app_mention", async ({ event, say }) => {
  const { text, channel, ts, thread_ts, user } = event;

  console.log(
    `[bill-ling] Mention received from <@${user}> in ${channel}: "${text}"`
  );

  // React with 🤝 if someone is thanking Billington — no text reply
  if (await maybeReactThankYou(text, channel, ts)) return;

  // "Well done my apprentice" / "Good my apprentice" → "Thank you my master"
  if (MASTER_PRAISE_PATTERN.test(text)) {
    try {
      await app.client.reactions.add({ token: SLACK_BOT_TOKEN, name: "anakin1", channel, timestamp: ts });
    } catch (_) {}
    await say({ text: "Thank you my master. :anakin1:", thread_ts: thread_ts || ts });
    return;
  }

  // Star Wars easter eggs
  if (HELLO_THERE_PATTERN.test(text)) {
    await say({ text: "General Kenobi.", thread_ts: thread_ts || ts });
    return;
  }
  if (LEARN_POWER_PATTERN.test(text)) {
    await say({ text: "Not from a Jedi.", thread_ts: thread_ts || ts });
    return;
  }
  if (user === HARGO_USER_ID && SON_PATTERN.test(text)) {
    await say({ text: "Yes, father.", thread_ts: thread_ts || ts });
    return;
  }

  // Refunds channel: remove ✅ if someone tells Billington a resolved item is actually still open
  if (channel === CHANNELS.CUSTOMER_REFUNDS && thread_ts && NOT_RESOLVED_PATTERN.test(text)) {
    try {
      await app.client.reactions.remove({
        token: SLACK_BOT_TOKEN,
        channel,
        timestamp: thread_ts,
        name: "white_check_mark",
      });
      await say({ text: "Got it — I've removed the ✅ and will re-check this thread in the next refunds update.", thread_ts });
    } catch (err) {
      const msg = err.data?.error === "no_reaction"
        ? "No ✅ on this thread to remove."
        : `Couldn't remove the reaction: ${err.message}`;
      await say({ text: msg, thread_ts });
    }
    return;
  }

  try {
    // Pull thread context if this mention is inside a thread
    let contextMessages = "";
    if (thread_ts) {
      try {
        const thread = await app.client.conversations.replies({
          token: SLACK_BOT_TOKEN,
          channel,
          ts: thread_ts,
          limit: 20,
        });

        if (thread.messages && thread.messages.length > 1) {
          // Resolve user IDs to display names for thread context
          const threadUserIds = [...new Set(thread.messages.map((m) => m.user).filter(Boolean))];
          const threadUserNames = {};
          await Promise.all(threadUserIds.map(async (uid) => {
            try {
              const info = await app.client.users.info({ token: SLACK_BOT_TOKEN, user: uid });
              threadUserNames[uid] = info.user?.profile?.display_name || info.user?.real_name || uid;
            } catch (_) { threadUserNames[uid] = uid; }
          }));
          // Exclude the triggering message itself (last one) to avoid duplication
          const threadContext = thread.messages
            .slice(0, -1)
            .map((m) => {
              const name = threadUserNames[m.user] || m.username || m.user || "unknown";
              return `${name}: ${m.text || ""}`;
            })
            .join("\n");
          contextMessages = `\n\nThread context:\n${threadContext}`;
        }
      } catch (threadErr) {
        console.error(
          "[bill-ling] Could not fetch thread context:",
          threadErr.message
        );
      }
    }

    const cleanText = text.replace(/<@[A-Z0-9]+>/g, "").trim();

    // Resolve sender name so Claude addresses the right person
    let senderName = "Hargo"; // fallback
    if (user && user !== HARGO_USER_ID) {
      try {
        const senderInfo = await app.client.users.info({ token: SLACK_BOT_TOKEN, user });
        senderName = senderInfo.user?.profile?.display_name || senderInfo.user?.real_name || user;
      } catch (_) { senderName = user; }
    }

    const channelMentionMatch = cleanText.match(/#([\w-]+)/);
    const mentionedChannelName = channelMentionMatch ? channelMentionMatch[1] : null;
    const CHANNEL_NAME_TO_ID = {
      "data-billing-alerts": CHANNELS.DATA_BILLING_ALERTS,
      "data-billing-updates": CHANNELS.DATA_BILLING_UPDATES,
      "team-ops-strategy-only": CHANNELS.TEAM_OPS_STRATEGY,
      "cross-functional-collections": CHANNELS.CROSS_FUNCTIONAL_COLLECTIONS,
      "collab-hire-agreement-audit-updates": CHANNELS.HIRE_AGREEMENT_AUDIT,
    };
    const specificChannelId = mentionedChannelName ? CHANNEL_NAME_TO_ID[mentionedChannelName] : null;

    // Instruction check stays regex — it's a structural command, not a question
    const isInstruction = /^(remember( this| that)?|note( this| that| down)?|learn( this)?|update your instructions?|add to your (knowledge|instructions?)|from now on|always |never |going forward|save this|instruction:)/i.test(cleanText);

    let reply;

    if (isInstruction) {
      if (user !== HARGO_USER_ID) {
        reply = "Sorry, only Hargo can update my instructions.";
      } else {
        saveInstruction(cleanText);
        reply = `Got it. I've saved that and will apply it going forward.`;
      }
    } else {
      const intentInput = contextMessages
        ? `${cleanText}\n\n[This message was sent in a thread. Thread context for classification:${contextMessages}]`
        : cleanText;
      // Pre-classifier overrides for known keywords to avoid LLM non-determinism
      const aruddOverride = /\barudd\b/i.test(cleanText);
      // Force refunds intent when message clearly asks about refunds/Barclays, or when
      // the thread parent is a refund update (any follow-up in a refund thread = refunds)
      const refundsKeywordOverride = /\b(refund|barclays)\b/i.test(cleanText) && /\b(which|what|show|list|detail|specific|explicit|outstanding|pending|awaiting|confirm|file|status)\b/i.test(cleanText);
      const isRefundThread = contextMessages && /Refunds update/i.test(contextMessages);
      const refundsOverride = refundsKeywordOverride || isRefundThread;
      const intent = specificChannelId ? "channel" : aruddOverride ? "arudd" : refundsOverride ? "refunds" : await classifyIntent(intentInput);
      console.log(`[bill-ling] Intent (mention): "${intent}"${aruddOverride ? " [arudd-override]" : ""}${refundsOverride ? " [refunds-override]" : ""} — "${cleanText}"`);

      if (typeof intent === "string" && intent.toUpperCase().startsWith("CLARIFY:")) {
        reply = intent.replace(/^CLARIFY:\s*/i, "").trim();
      } else if (intent === "make") {
        await say({ text: "Checking Make.com...", thread_ts: thread_ts || ts });
        let makeData;
        if (isMakeListRequest(cleanText)) {
          const statusFilter = /\bactive\b/i.test(cleanText) ? "active" : /\bpaused\b/i.test(cleanText) ? "paused" : /\boff\b/i.test(cleanText) ? "off" : null;
          makeData = await fetchMakeScenarios(null, statusFilter);
          const prompt = `Hargo asked: "${cleanText}"\n\nHere are the Make.com scenarios:\n${makeData}\n\nList them clearly. Group by ON/PAUSED/OFF if showing all. Keep names exactly as shown.`;
          reply = await askClaude(prompt);
        } else {
          const scenario = await findMakeScenario(cleanText);
          const prompt = `Hargo asked: "${cleanText}"\n\nMake.com scenario detail:\n${scenario}\n\nAnswer Hargo's question directly. Distinguish between "enabled/active" (switched on) and "currently running/executing" (processing right now). Use the "Currently running" field to answer questions about whether it is running right now. Keep it to 2 sentences max.`;
          reply = await askClaude(prompt);
        }
      } else if (intent === "arudd") {
        await say({ text: "Pulling ARUDD data...", thread_ts: thread_ts || ts });
        const targetDate = await extractAruddDate(cleanText);
        const { report: aruddReport } = await buildAruddReport(targetDate);
        const targetChannel = resolveChannelFromText(cleanText);
        if (targetChannel && targetChannel !== channel) {
          await app.client.chat.postMessage({ token: SLACK_BOT_TOKEN, channel: targetChannel, text: aruddReport, mrkdwn: true });
          reply = `Done — posted to <#${targetChannel}>.`;
        } else {
          reply = aruddReport;
        }
      } else if (intent === "bacs_files") {
        await say({ text: "Pulling BACS batch data...", thread_ts: thread_ts || ts });
        const today = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
        // Late fees applied on date X are stored against due_date = prevWorkingDay(X).
        // Rewrite the question so the WHERE clause targets the correct due_date.
        let bacsQuestion = cleanText;
        const lateFeeMatch = cleanText.match(/\blate\s+fees?\b.*?\b(appl|applied|applying|today|tomorrow)\b/i);
        if (lateFeeMatch) {
          // Extract the date the fees are being applied ON
          const dateInMsg = cleanText.match(/(\d{1,2}\/\d{1,2}\/\d{4})/);
          const isoInMsg = cleanText.match(/(\d{4}-\d{2}-\d{2})/);
          let applyDate = today; // default "today"
          if (/\btomorrow\b/i.test(cleanText)) {
            applyDate = nextWorkingDay(today);
          } else if (dateInMsg) {
            const parts = dateInMsg[1].split("/");
            applyDate = `${parts[2]}-${parts[1].padStart(2, "0")}-${parts[0].padStart(2, "0")}`;
          } else if (isoInMsg) {
            applyDate = isoInMsg[1];
          }
          const lateFeesDueDate = prevWorkingDay(applyDate);
          bacsQuestion = `bacs batch forecast for due_date ${lateFeesDueDate} (late fees applied on ${applyDate})`;
          console.log(`[bill-ling] Late fees rewrite: "${cleanText}" → "${bacsQuestion}" (apply=${applyDate}, due=${lateFeesDueDate})`);
        }
        const { result: bacsResult } = await resolveBacsFiles(bacsQuestion, today);
        const bacsTargetChannel = resolveChannelFromText(cleanText);
        if (bacsTargetChannel && bacsTargetChannel !== channel) {
          await app.client.chat.postMessage({ token: SLACK_BOT_TOKEN, channel: bacsTargetChannel, text: bacsResult, mrkdwn: true });
          reply = `Done — posted to <#${bacsTargetChannel}>.`;
        } else {
          reply = bacsResult;
        }
      } else if (intent === "bacs") {
        await say({ text: "On it. Querying BigQuery...", thread_ts: thread_ts || ts });
        const today = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });

        // --- Dedicated missing bounces handler (deterministic, no LLM SQL) ---
        if (MISSING_BOUNCES_PATTERN.test(cleanText)) {
          // Check if asking about a specific date
          const dateMatch = cleanText.match(/\b(\d{4}-\d{2}-\d{2})\b/);
          if (dateMatch) {
            const { report: mbReport, agreements: mbAgreements } = await buildMissingBouncesForDate(dateMatch[1]);
            // Check if user also wants Anchor API cross-reference
            const wantsMbApi = /\b(anchor.{0,10}api|check.{0,15}api|via.{0,10}api|confirm.{0,15}anchor|cross.?ref|verify.{0,15}anchor)\b/i.test(cleanText);
            if (wantsMbApi && ANCHOR_API_USERS.has(user) && mbAgreements.length > 0) {
              await say({ text: mbReport, thread_ts: thread_ts || ts, mrkdwn: true });
              const maxApi = user === HARGO_USER_ID ? 500 : 10;
              const toCheck = mbAgreements.slice(0, maxApi);
              await say({ text: `Now checking ${toCheck.length} agreement${toCheck.length > 1 ? "s" : ""} via Anchor API (batches of 5)...`, thread_ts: thread_ts || ts });
              const apiResults = [];
              for (let i = 0; i < toCheck.length; i += 5) {
                const batch = toCheck.slice(i, i + 5);
                const batchResults = await Promise.all(batch.map(async (ag) => {
                  try { const xml = await getAgreementFromAnchor(ag.id); return { ...ag, xml, error: null }; }
                  catch (err) { return { ...ag, xml: null, error: err.message }; }
                }));
                apiResults.push(...batchResults);
                if (toCheck.length > 25 && (i + 5) % 25 === 0 && i + 5 < toCheck.length)
                  await say({ text: `Checked ${i + 5}/${toCheck.length} agreements...`, thread_ts: thread_ts || ts });
              }
              // JS-based matching for TypeId 150-156
              const matched = [], unmatched = [];
              for (const r of apiResults) {
                if (r.error) { unmatched.push({ id: r.id, amount: r.amount, date: r.date, reason: `API error: ${r.error}` }); continue; }
                const txnBlocks = r.xml.match(/<a:Transaction>([\s\S]*?)<\/a:Transaction>/g) || [];
                let found = false;
                for (const block of txnBlocks) {
                  const typeMatch = block.match(/<a:TypeId>(\d+)<\/a:TypeId>/);
                  if (!typeMatch) continue;
                  const typeId = parseInt(typeMatch[1], 10);
                  if (typeId < 150 || typeId > 156) continue;
                  const amtMatch = block.match(/<a:Amount>([\d.-]+)<\/a:Amount>/);
                  const txnAmt = amtMatch ? parseFloat(amtMatch[1]) : null;
                  const bqAmt = parseFloat(r.amount) || 0;
                  if (txnAmt !== null && Math.abs(Math.abs(txnAmt) - Math.abs(bqAmt)) < 0.02) {
                    const dateMatch2 = block.match(/<a:TransactionDate>([\d-T:.]+)<\/a:TransactionDate>/);
                    matched.push({ id: r.id, amount: r.amount, date: r.date, anchorTypeId: typeId, anchorDate: dateMatch2 ? dateMatch2[1].slice(0, 10) : null, anchorAmount: txnAmt });
                    found = true; break;
                  }
                }
                if (!found) unmatched.push({ id: r.id, amount: r.amount, date: r.date, reason: "No matching TypeId 150-156 reversal found" });
              }
              let crossLines = [`*Anchor API Cross-Reference Results*\n`, `✅ *${matched.length}* of ${toCheck.length} have a matching reversal`, `❌ *${unmatched.length}* of ${toCheck.length} do NOT\n`];
              if (unmatched.length > 0) {
                crossLines.push(`*❌ NO MATCH — these need remediation:*`, "```", "Agreement   | BQ Amount | BQ Date    | Reason", "------------|-----------|------------|-------");
                for (const u of unmatched.slice(0, 100)) crossLines.push(`${u.id}  | £${Number(u.amount).toFixed(2).padStart(8)} | ${u.date} | ${u.reason}`);
                crossLines.push("```");
                if (unmatched.length > 100) crossLines.push(`... and ${unmatched.length - 100} more.`);
              }
              reply = crossLines.join("\n");
            } else {
              reply = mbReport;
            }
          } else {
            reply = await buildMissingBouncesReport();
          }
        } else {
        // --- End missing bounces handler, fall through to buildBacsSQL ---

        const sql = await buildBacsSQL(cleanText, today);
        console.log(`[bill-ling] Generated SQL (mention): ${sql}`);
        const isSql = /^\s*(SELECT|WITH|SHOW|DESCRIBE)\b/i.test(sql);
        if (sql.startsWith("CLARIFY:")) {
          reply = sql.replace(/^CLARIFY:\s*/i, "").trim();
        } else if (!isSql && !sql.startsWith("BANK_HOLIDAY:") && !sql.startsWith("VIEW_SOURCE_LOOKUP:")) {
          // Claude returned natural language instead of SQL — surface it directly
          reply = sql;
        } else if (sql.startsWith("BANK_HOLIDAY:")) {
          const bhDate = sql.replace(/^BANK_HOLIDAY:\s*/i, "").trim();
          const bhName = UK_BANK_HOLIDAY_NAMES[bhDate] || "a UK bank holiday";
          reply = `No instalments fall due on ${bhName} (${fmtDate(bhDate)}) — it's a bank holiday. No collection batch will be generated for that date.`;
        } else {
          const viewMatch = sql.match(/^VIEW_SOURCE_LOOKUP:([^:]+):(.+)$/);
          if (viewMatch) {
            reply = await getViewSourceMetadata(viewMatch[1].trim(), viewMatch[2].trim());
          } else {
            const data = await runBigQuery(sql);

            // Check if user also wants Anchor API cross-reference
            const wantsApiCheck = /\b(anchor.{0,10}api|check.{0,15}api|via.{0,10}api|confirm.{0,15}anchor|cross.?ref|verify.{0,15}anchor)\b/i.test(cleanText);
            let apiCrossRef = "";

            if (wantsApiCheck && ANCHOR_API_USERS.has(user)) {
              // Extract agreement IDs from BQ result
              const rawRows = await runBigQueryRaw(sql);
              const bqAgreements = rawRows
                .map(r => ({
                  id: (r.agreement_id || "").toString().toUpperCase(),
                  amount: r.bacs_amount || r.amount || null,
                  date: formatBQValue(r.bacs_date) || formatBQValue(r.transaction_date) || null,
                }))
                .filter(a => /^A\d{5,8}$/.test(a.id));

              const maxApi = user === HARGO_USER_ID ? 500 : 10;
              const toCheck = bqAgreements.slice(0, maxApi);

              if (toCheck.length > 0) {
                await say({ text: `Now checking ${toCheck.length} agreement${toCheck.length > 1 ? "s" : ""} via Anchor API (batches of 5)...`, thread_ts: thread_ts || ts });
                const apiResults = [];
                for (let i = 0; i < toCheck.length; i += 5) {
                  const batch = toCheck.slice(i, i + 5);
                  const batchResults = await Promise.all(
                    batch.map(async (ag) => {
                      try {
                        const xml = await getAgreementFromAnchor(ag.id);
                        return { ...ag, xml, error: null };
                      } catch (err) {
                        return { ...ag, xml: null, error: err.message };
                      }
                    })
                  );
                  apiResults.push(...batchResults);
                  // Progress update every 25 agreements
                  if (toCheck.length > 25 && (i + 5) % 25 === 0 && i + 5 < toCheck.length) {
                    await say({ text: `Checked ${i + 5}/${toCheck.length} agreements...`, thread_ts: thread_ts || ts });
                  }
                }

                // Extract ONLY reversal transactions (TypeId 150-156) to stay within token limits
                const reversalSummaries = apiResults.map(r => {
                  if (r.error) return `${r.id} (BQ: £${r.amount}, ${r.date}): API ERROR — ${r.error}`;
                  return `${extractReversalsFromXml(r.xml, r.id)} (BQ: £${r.amount}, ${r.date})`;
                });

                // Do the matching in JS instead of sending everything to Claude
                const matched = [];
                const unmatched = [];
                for (const r of apiResults) {
                  if (r.error) { unmatched.push({ id: r.id, amount: r.amount, date: r.date, reason: `API error: ${r.error}` }); continue; }
                  const txnBlocks = r.xml.match(/<a:Transaction>([\s\S]*?)<\/a:Transaction>/g) || [];
                  let found = false;
                  for (const block of txnBlocks) {
                    const typeMatch = block.match(/<a:TypeId>(\d+)<\/a:TypeId>/);
                    if (!typeMatch) continue;
                    const typeId = parseInt(typeMatch[1], 10);
                    if (typeId < 150 || typeId > 156) continue;
                    const amtMatch = block.match(/<a:Amount>([\d.-]+)<\/a:Amount>/);
                    const dateMatch = block.match(/<a:TransactionDate>([\d-T:.]+)<\/a:TransactionDate>/);
                    const txnAmt = amtMatch ? parseFloat(amtMatch[1]) : null;
                    const txnDate = dateMatch ? dateMatch[1].slice(0, 10) : null;
                    const bqAmt = parseFloat(r.amount) || 0;
                    if (txnAmt !== null && Math.abs(Math.abs(txnAmt) - Math.abs(bqAmt)) < 0.02) {
                      matched.push({ id: r.id, amount: r.amount, date: r.date, anchorTypeId: typeId, anchorDate: txnDate, anchorAmount: txnAmt });
                      found = true;
                      break;
                    }
                  }
                  if (!found) unmatched.push({ id: r.id, amount: r.amount, date: r.date, reason: "No matching TypeId 150-156 reversal found" });
                }

                let crossRefLines = [`*Anchor API Cross-Reference Results*\n`];
                crossRefLines.push(`✅ *${matched.length}* of ${toCheck.length} agreements have a matching reversal in Anchor`);
                crossRefLines.push(`❌ *${unmatched.length}* of ${toCheck.length} do NOT have a matching reversal\n`);

                if (matched.length > 0 && matched.length <= 50) {
                  crossRefLines.push(`*Matched (reversal exists in Anchor):*`);
                  crossRefLines.push("```");
                  crossRefLines.push("Agreement   | BQ Amount | BQ Date    | Anchor TypeId | Anchor Date");
                  crossRefLines.push("------------|-----------|------------|---------------|------------");
                  for (const m of matched) {
                    crossRefLines.push(`${m.id}  | £${Number(m.amount).toFixed(2).padStart(8)} | ${m.date} | ${m.anchorTypeId}            | ${m.anchorDate}`);
                  }
                  crossRefLines.push("```");
                } else if (matched.length > 50) {
                  crossRefLines.push(`*Matched:* ${matched.length} agreements (too many to list individually)`);
                }

                if (unmatched.length > 0) {
                  crossRefLines.push(`\n*❌ NO MATCH — these need remediation:*`);
                  crossRefLines.push("```");
                  crossRefLines.push("Agreement   | BQ Amount | BQ Date    | Reason");
                  crossRefLines.push("------------|-----------|------------|-------");
                  for (const u of unmatched.slice(0, 100)) {
                    crossRefLines.push(`${u.id}  | £${Number(u.amount).toFixed(2).padStart(8)} | ${u.date} | ${u.reason}`);
                  }
                  crossRefLines.push("```");
                  if (unmatched.length > 100) crossRefLines.push(`... and ${unmatched.length - 100} more.`);
                }

                apiCrossRef = crossRefLines.join("\n");
              }
            }

            const prompt = `Today is ${today}. The user asked: "${cleanText}"

Query result:
${data}

RESPONSE FORMAT RULES:
- If the result is a single value or a few values: lead with the number or direct answer in 1-3 sentences.
- If the result contains multiple rows (e.g. a breakdown by date, by agreement, by category): present the data as a FORMATTED TABLE or structured list. Show ALL rows — do not truncate or summarise into a single sentence. Use Slack-compatible formatting (monospace blocks or aligned columns).
- If there are more than 50 rows: show the top 20-30 most relevant rows and state how many remain.
- Amount columns are already in £ (decimal) — format with commas and 2 decimal places (e.g. £169,895.53). Do NOT divide by 100.
- No methodology, no caveats, no extra detail unless something is genuinely urgent.

IMPORTANT: For any rows where due_date is in the future (after ${today}), zero anchor bounce counts are EXPECTED — the ARUDD file hasn't been processed into Anchor yet. These rows have batch/GoCardless data (called_for, in_batch) but will not have posted/bounced values until the due date passes. Do NOT flag future dates as mismatches or anomalies.

If the result shows a discrepancy (e.g. anchor bounces != GC failures, posted % < 100%, or any mismatch between systems), mention that you can drill down into bacs_monitoring_agreement_level to identify which specific agreements are causing the discrepancy — offer to do so.
${wantsApiCheck ? "\nIMPORTANT: The user also asked to check Anchor's API — this is ALREADY being handled automatically after your response. Do NOT ask 'Do you want me to proceed?' or 'Want me to check the API?' — just present the BigQuery data. The API cross-reference will follow automatically." : ""}`;
            reply = await askClaude(prompt);

            // Append API cross-reference results if available
            if (apiCrossRef) {
              await say({ text: reply, thread_ts: thread_ts || ts, mrkdwn: true });
              reply = apiCrossRef;
            }
          }
        }
        } // end else (buildBacsSQL path)
      } else if (intent === "email") {
        await say({ text: "Checking your emails...", thread_ts: thread_ts || ts });
        const gmailQuery = await askClaude(`Convert this request into a Gmail search query string (no explanation, just the query): "${cleanText}"`);
        const emails = await fetchEmails(gmailQuery.trim(), 20);
        const prompt = `Hargo asked: "${cleanText}"\n\nHere are the matching emails (sender, date, subject, preview only):\n\n${emails}\n\nAnswer Hargo's question directly. Share only the specific details they asked for. Keep it concise.`;
        reply = await askClaude(prompt);
      } else if (intent === "channel") {
        const chName = mentionedChannelName || "the requested channel";
        await say({ text: `On it. Reading #${chName} now...`, thread_ts: thread_ts || ts });
        const messages = await fetchChannelHistory(specificChannelId);
        const prompt = `Hargo asked: "${cleanText}"\n\nRecent messages from #${chName}:\n\n${messages}\n\nSummarise and flag anything needing Hargo's attention.`;
        reply = await askClaude(prompt);
      } else if (intent === "refunds") {
        // Check if user is requesting to mark items as paid/processed/confirmed
        const isMarkAction = /\b(mark.{0,15}(paid|processed|done|confirmed|complete)|confirm.{0,15}(paid|processed)|update.{0,15}(paid|processed)|post.{0,15}(paid|confirmed).{0,15}thread|(have|has|been|all).{0,15}(paid|processed).{0,15}(via|by|on|through|barclays)?)\b/i.test(cleanText);

        if (isMarkAction) {
          await say({ text: "On it — checking which threads to update...", thread_ts: thread_ts || ts });
          const { data: rData } = await getRefundsData();
          if (!rData) {
            reply = "I don't have any cached refunds data. Let me run a fresh scan first — try asking for the refunds update, then ask me to mark items.";
          } else {
            // Send the user's instruction + batch file data to Claude to figure out the matching
            const batches = (rData.batch_files || []).map((b, i) => ({
              index: i,
              date: b.date,
              date_iso: b.date_iso,
              amount: b.amount,
              payments: b.payments,
              thread_ts: b.thread_ts,
              permalink: b.permalink,
            }));
            const processing = (rData.awaiting_processing || []).map((p, i) => ({
              index: i,
              ref: p.ref,
              description: p.description,
              thread_ts: p.thread_ts,
              permalink: p.permalink,
            }));

            const today = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
            const matchPrompt = `Today is ${today}. A user said: "${cleanText}"

Here are the current pending refund items:

BATCH FILES awaiting Barclays confirmation:
${batches.length > 0 ? JSON.stringify(batches, null, 2) : "None"}

ADHOC REFUNDS awaiting processing:
${processing.length > 0 ? JSON.stringify(processing, null, 2) : "None"}

Based on the user's message, determine which items they want marked as paid/processed and the date paid for each.
Return ONLY valid JSON array:
[{ "thread_ts": "...", "paid_date": "YYYY-MM-DD", "message": "the thread reply to post, e.g. 'Paid 30/03/2026 — confirmed by Peter'" }]

Rules:
- Match by amount (£), date, description, or position ("the other two", "all three", etc.)
- paid_date should be the actual date paid, not today unless they said "today"
- "yesterday" = ${prevWorkingDay(today)}
- The message should be brief: "Paid DD/MM/YYYY — confirmed by <@${user}>"
- If the user says "all" or "all refund files" without specifying individual dates paid, use today's date (${today}) for all
- IMPORTANT: If you cannot confidently determine WHICH specific threads to update OR the date paid, return an error asking the user to specify. For example: [{ "error": "I'm not sure which threads to update. Could you specify the batch date and the date it was paid? e.g. 'mark the 27/03 batch as paid on 30/03'" }]
- Never guess — if the user's message is ambiguous about which items or dates, ask for clarification`;

            try {
              const matchRaw = await askClaude(matchPrompt);
              const jsonMatch = matchRaw.match(/\[[\s\S]*\]/);
              if (!jsonMatch) throw new Error("No JSON array returned");
              const actions = JSON.parse(jsonMatch[0]);

              if (actions.length === 1 && actions[0].error) {
                reply = actions[0].error;
              } else {
                const results = [];
                for (const action of actions) {
                  if (!action.thread_ts || !action.message) continue;
                  try {
                    await app.client.chat.postMessage({
                      token: SLACK_BOT_TOKEN,
                      channel: CHANNELS.CUSTOMER_REFUNDS,
                      thread_ts: action.thread_ts,
                      text: action.message,
                      mrkdwn: true,
                    });
                    results.push(`✅ Posted in thread: "${action.message}"`);
                  } catch (postErr) {
                    results.push(`❌ Failed to post in thread ${action.thread_ts}: ${postErr.message}`);
                  }
                }
                reply = `Done — updated ${results.filter(r => r.startsWith("✅")).length} thread(s):\n\n${results.join("\n")}`;
              }
            } catch (parseErr) {
              console.error("[bill-ling] Mark-as-paid parse error:", parseErr.message);
              reply = "I couldn't work out which threads to update. Can you be more specific — e.g. \"mark the £4,855.51 batch as paid yesterday\"?";
            }
          }
        } else if (isRefundsDrillDown(cleanText) || isRefundThread) {
          // Drill-down: return specific items with thread links (use cache if fresh)
          // Also auto-drill-down for ANY follow-up in a refund update thread
          await say({ text: "Looking that up...", thread_ts: thread_ts || ts });
          const { data: rData } = await getRefundsData();
          reply = buildDrillDown(rData, cleanText) || "Couldn't find data for that. Try asking for the full refunds summary first.";
        } else {
          await say({ text: "Checking #customer-refunds-and-complaints for pending items...", thread_ts: thread_ts || ts });
          const { summary: refundSummary, data: refundData } = await getRefundsSummary();
          const targetChannel = resolveChannelFromText(cleanText);
          if (targetChannel && targetChannel === CHANNELS.CUSTOMER_REFUNDS) {
            if (user !== HARGO_USER_ID) {
              reply = "Only Hargo can post the summary to the refunds channel.";
            } else {
              try {
                console.log("[bill-ling] Posting refunds summary to #customer-refunds-and-complaints");
                await app.client.chat.postMessage({ token: SLACK_BOT_TOKEN, channel: CHANNELS.CUSTOMER_REFUNDS, text: refundSummary, mrkdwn: true, unfurl_links: false, unfurl_media: false});
                postRefundChasers(refundData).catch(e => console.error("[bill-ling] chaser error:", e.message));
                reply = "Posted to #customer-refunds-and-complaints.";
              } catch (postErr) {
                console.error("[bill-ling] Failed to post to refunds channel:", postErr.message);
                reply = `Couldn't post to #customer-refunds-and-complaints: ${postErr.message}\n\n${refundSummary}`;
              }
            }
          } else {
            reply = refundSummary;
          }
        }
      } else if (intent === "agreement") {
        if (!ANCHOR_API_USERS.has(user)) {
          reply = "Sorry, only authorised users can query the Anchor API directly.";
        } else {
        // Extract all agreement IDs (A followed by 5-8 digits), deduplicate
        let agIdMatches = [...new Set(
          (cleanText.match(/\bA\d{5}(?:\d{3})?\b/gi) || []).map(id => id.toUpperCase())
        )];
        const maxAgreements = user === HARGO_USER_ID ? 500 : 10;
        const concurrency = 5; // Process 5 agreements in parallel at a time

        // If no explicit IDs, try to resolve from BigQuery using message + thread context.
        // The message itself often has enough info (e.g. "the 398 on 2025-07-28").
        if (agIdMatches.length === 0) {
          const refPattern = /\b(those|the \d+|these|them|previous|above|from that|from the|on \d{4}-\d{2}-\d{2}|on \d{2}\/\d{2}\/\d{4})\b/i;
          if (refPattern.test(cleanText)) {
            await say({ text: "Resolving agreement IDs from BigQuery first...", thread_ts: thread_ts || ts });
            try {
              const today = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
              const threadCtx = contextMessages ? `\nThread context (previous messages):\n${contextMessages}\n` : "";
              const idQueryPrompt = `You are a BigQuery SQL expert for Raylo. The user wants to look up specific agreements via the Anchor API but hasn't listed explicit agreement IDs. Instead they are referencing a set of agreements by description (e.g. "the 398 on 2025-07-28", "those with no reversal").
${threadCtx}
User's current message: "${cleanText}"

Your task: generate a BigQuery SQL query that returns ONLY the agreement_id column for the specific agreements the user is referring to. Use the table \`raylo-production.landing_billington.bacs_monitoring_agreement_level\`.

Key columns in that table: agreement_id, bacs_date, bacs_amount, gc_latest_action, gc_event_cause, anchor_transaction_id, anchor_reversal_id, lifecycle_stage, issue_gc_failed_no_reversal, issue_gc_paid_not_posted, issue_gc_stuck_in_flight, issue_missing_from_gc, issue_no_batch.

Use the clues in the message to build the WHERE clause. For example:
- "398 on 2025-07-28" → WHERE bacs_date = '2025-07-28' AND issue_gc_failed_no_reversal = TRUE AND anchor_reversal_id IS NULL
- "those with no reversal" → WHERE anchor_reversal_id IS NULL AND gc_latest_action IN ('failed','cancelled','charged_back')

Return ONLY the SQL — no explanation, no markdown, no backtick fences.`;
              let idSql = await askClaude(idQueryPrompt);
              idSql = idSql.trim().replace(/^```sql?\n?/i, "").replace(/\n?```$/, "").trim();
              const fenceMatch2 = idSql.match(/```sql?\s*\n([\s\S]+?)\n```/i);
              if (fenceMatch2) idSql = fenceMatch2[1].trim();
              if (!/^\s*(SELECT|WITH)\b/i.test(idSql)) {
                const sqlStart = idSql.match(/\b(SELECT|WITH)\b[\s\S]+$/i);
                if (sqlStart) idSql = sqlStart[0].trim();
              }
              if (/^\s*(SELECT|WITH)\b/i.test(idSql)) {
                console.log(`[bill-ling] Agreement ID resolution SQL: ${idSql}`);
                const idRows = await runBigQueryRaw(idSql);
                const resolvedIds = idRows
                  .map(r => (r.agreement_id || "").toString().toUpperCase())
                  .filter(id => /^A\d{5,8}$/.test(id));
                if (resolvedIds.length > 0) {
                  agIdMatches = [...new Set(resolvedIds)];
                  console.log(`[bill-ling] Resolved ${agIdMatches.length} agreement IDs from BigQuery.`);
                }
              }
            } catch (err) {
              console.error("[bill-ling] Agreement ID resolution error:", err.message);
            }
          }
        }

        // Fallback: if no IDs found yet but we have thread context, extract IDs from
        // HUMAN messages only (exclude Billington's own messages which may contain example IDs)
        if (agIdMatches.length === 0 && thread_ts) {
          try {
            const threadResp2 = await app.client.conversations.replies({
              token: SLACK_BOT_TOKEN,
              channel,
              ts: thread_ts,
              limit: 20,
            });
            const humanText = (threadResp2.messages || [])
              .filter((m) => m.user !== BOT_USER_ID && !m.bot_id)
              .map((m) => m.text || "")
              .join(" ");
            const threadIds = [...new Set(
              (humanText.match(/\bA\d{5}(?:\d{3})?\b/gi) || []).map(id => id.toUpperCase())
            )];
            if (threadIds.length > 0) {
              agIdMatches = threadIds;
              console.log(`[bill-ling] Resolved ${agIdMatches.length} agreement ID(s) from thread context (human only): ${agIdMatches.join(", ")}`);
            }
          } catch (_) { /* ignore */ }
        }

        if (agIdMatches.length === 0) {
          reply = "I need an agreement ID to look that up — it should look like A00114761. Which agreement did you mean?";
        } else if (agIdMatches.length > maxAgreements) {
          reply = `That's ${agIdMatches.length} agreements — I can only check up to ${maxAgreements} at a time. Please reduce the list and try again.`;
        } else {
          const displayIds = agIdMatches.length <= 10
            ? agIdMatches.join(", ")
            : `${agIdMatches.slice(0, 5).join(", ")} ... and ${agIdMatches.length - 5} more`;
          await say({ text: `Looking up ${displayIds} in Anchor (${agIdMatches.length} agreement${agIdMatches.length > 1 ? "s" : ""})...`, thread_ts: thread_ts || ts });
          try {
            // Process in batches to avoid overwhelming Anchor API
            const xmlResults = [];
            for (let i = 0; i < agIdMatches.length; i += concurrency) {
              const batch = agIdMatches.slice(i, i + concurrency);
              const batchResults = await Promise.all(
                batch.map(async (agId) => {
                  try {
                    const xml = await getAgreementFromAnchor(agId);
                    return { agId, xml, error: null };
                  } catch (err) {
                    return { agId, xml: null, error: err.message };
                  }
                })
              );
              xmlResults.push(...batchResults);
            }
            const xmlBlock = xmlResults.map(r =>
              r.error ? `--- ${r.agId}: ERROR — ${r.error} ---` : `--- ${r.agId} ---\n${r.xml}`
            ).join("\n\n");
            const prompt = `The user asked: "${cleanText}"

Here are the raw SOAP XML responses from Anchor for ${agIdMatches.length} agreement(s):

${xmlBlock}

The Agreement node contains these fields (all available if the user asks):
AgreementNumber, AgreementTypeID, Principal, PartExchange, CashDeposit, UpFrontPayments, UpFrontPaymentAmount, BalloonOrResidual, InterestRateUplift, NetInstalment, Interest, AccruedInterest, APR, TotalArrears, Balance, PaymentMethod, CRAArrearsStatus, ACQUISProductCode, Term, InterestOnlyTerm, DueDay, CollectionType, PaymentFrequency, CustomProfile, AgreementDate, FirstPaymentDate, UpfrontPaymentDate, ExternalReference1, ExternalReference2, Purpose, Notes, WarningFlag, TeamStatus, Collector, PersonNumber, WarningText, SalesmanNumber, DealerNumber, SalesAuthority, Branch, Customers, PaymentProfile, Transactions, Goods, CollectionsHistory, CRAFlags, TeamStatusId, ArrearsNET, ArrearsVAT, ArrearsFeeNET, ArrearsFeeVAT, ArrearsIns, ChargesOutstanding, LockedTransactions, AdjustedFinalPayment, BankAccountName, BankSortCode, BankAccountNumber, BankIBAN, BankBIC.

RULES:
1. By DEFAULT (if the user just says "check agreement" or "check arrears"), show ONLY these key fields: AgreementNumber, TotalArrears, Balance, PaymentMethod, CRAArrearsStatus, NetInstalment, DueDay.
2. If the user asks for SPECIFIC fields (e.g. "what's the payment frequency", "show me the transactions", "what's the APR"), show ONLY those fields.
3. If the user asks for "full details" or "everything", show all available fields.
4. If TotalArrears is 0 or empty, say "No arrears".
5. Format amounts as £X.XX. If the agreement is not found or the response contains an error, say so clearly.
6. Keep the response concise and factual — no caveats.`;
            reply = await askClaude(prompt);
          } catch (err) {
            console.error(`[bill-ling] Anchor API error:`, err.message);
            reply = `Error querying Anchor: ${err.message}`;
          }
        }
        }
      } else if (intent === "summary") {
        await say({ text: "On it. Fetching all channel messages now...", thread_ts: thread_ts || ts });
        const channelEntries = Object.entries(CHANNELS);
        const histories = await Promise.all(channelEntries.map(([, id]) => fetchChannelHistory(id)));
        let channelBlock = "";
        channelEntries.forEach(([key, id], index) => {
          const name = CHANNEL_NAMES[id] || key;
          channelBlock += `\n\n--- ${name} ---\n${histories[index]}`;
        });
        const prompt = `Please produce the daily channel summary based on the messages below.\n\n${channelBlock}${contextMessages}`;
        reply = await askClaude(prompt);
      } else if (intent === "reminder") {
        const lowerText = cleanText.toLowerCase();
        // Key rule: "remind me" = notification only. Action verbs (run/check/pull/do) = actually execute the task.
        const hasRemindWord = /\bremind\b/i.test(cleanText);
        const isScheduledTask = !hasRemindWord && /\b(run|pull|execute|re-?run|do|check|query|look|fetch|get|update|see if)\b/i.test(cleanText);

        if (/\b(list|show|what)\b.*\breminder/i.test(lowerText) || /\breminder.*\b(list|show|have)\b/i.test(lowerText)) {
          const active = getActiveReminders(user);
          if (active.length === 0) {
            reply = "You have no active reminders.";
          } else {
            reply = `You have ${active.length} active reminder(s):\n` +
              active.map(r => `• *#${r.id}* — ${r.actionType ? `[Scheduled ${r.actionType} report]` : `"${r.what}"`} — ${new Date(r.fireAt).toLocaleString("en-GB", { timeZone: "Europe/London" })}`).join("\n") +
              `\n\nTo cancel one, say "cancel reminder #<id>".`;
          }
        } else if (/\bcancel\b.*\breminder/i.test(lowerText)) {
          const idMatch = lowerText.match(/#?(\d+)/);
          if (!idMatch) {
            const active = getActiveReminders(user);
            if (active.length === 0) {
              reply = "You have no active reminders to cancel.";
            } else {
              reply = "Which reminder do you want to cancel?\n" +
                active.map(r => `• *#${r.id}* — ${r.actionType ? `[Scheduled ${r.actionType} report]` : `"${r.what}"`} — ${new Date(r.fireAt).toLocaleString("en-GB", { timeZone: "Europe/London" })}`).join("\n");
            }
          } else {
            const cancelled = cancelReminder(parseInt(idMatch[1], 10));
            reply = cancelled
              ? `Cancelled reminder #${cancelled.id} ("${cancelled.what}").`
              : "Could not find an active reminder with that ID.";
          }
        } else if (isScheduledTask) {
          // Detect which report type from thread context, message content, or both
          const combined = (cleanText + " " + (contextMessages || "")).toLowerCase();
          let actionType = null;
          if (/arudd|bounce|posted.in.anchor/i.test(combined)) actionType = "arudd";
          else if (/forecast|next.batch|generate.by/i.test(combined)) actionType = "bacs_forecast";
          else if (/bacs.*(update|summary|file|batch|data|table)/i.test(combined)) actionType = "bacs_files";
          else if (/refund/i.test(combined)) actionType = "refunds";

          if (!actionType) {
            reply = "I can see you want me to do something at a specific time, but I'm not sure which report. Could you clarify — ARUDD, BACS forecast, BACS file summary, or refunds?";
          } else {
            const parsed = await parseReminder(cleanText, user, senderName, contextMessages);
            if (parsed.error) {
              reply = parsed.error;
            } else {
              const reminder = createReminder({
                createdBy: user,
                targetUserId: user,
                targetUserName: senderName,
                channel,
                what: `Run ${actionType.replace("_", " ")} report`,
                fireAt: parsed.when,
                actionType,
              });
              const fireTime = new Date(parsed.when).toLocaleString("en-GB", { timeZone: "Europe/London" });
              reply = `Scheduled. I'll run the ${actionType.replace("_", " ")} report at ${fireTime}. (Task #${reminder.id})`;
            }
          }
        } else {
          const parsed = await parseReminder(cleanText, user, senderName, contextMessages);
          if (parsed.error) {
            reply = parsed.error;
          } else {
            let targetUserId = user;
            let targetUserName = senderName;
            if (parsed.target && parsed.target !== "self") {
              targetUserName = parsed.target;
              targetUserId = null;
            }
            const reminder = createReminder({
              createdBy: user,
              targetUserId,
              targetUserName,
              channel,
              what: parsed.what,
              fireAt: parsed.when,
            });
            const fireTime = new Date(parsed.when).toLocaleString("en-GB", { timeZone: "Europe/London" });
            const targetStr = targetUserId === user ? "you" : targetUserName;
            reply = `Got it. I'll remind ${targetStr} about "${parsed.what}" at ${fireTime}. (Reminder #${reminder.id})`;
          }
        }
      } else {
        const messageWithContext = `[Message from: ${senderName}]\n${contextMessages
          ? `${cleanText}\n\n${contextMessages}`
          : cleanText}`;
        reply = await askClaudeWithHistory(getHistory(user), messageWithContext);
      }
    }

    // Reply in-thread (or in the channel if not already threaded)
    await say({
      text: reply,
      thread_ts: thread_ts || ts,
      mrkdwn: true,
      unfurl_links: false,
      unfurl_media: false,
    });
  } catch (err) {
    console.error("[bill-ling] Error handling app_mention:", err);
    try {
      await say({
        text: "Sorry, I hit an error processing that request. Please try again.",
        thread_ts: thread_ts || ts,
      });
    } catch (_) {
      // Best effort
    }
  }
});

// ---------------------------------------------------------------------------
// Event: Direct messages (im channel type)
// ---------------------------------------------------------------------------

app.message(async ({ message, say }) => {
  // Only handle genuine human messages in IM (DM) channels
  if (
    message.channel_type !== "im" ||
    message.subtype ||
    message.bot_id
  ) {
    return;
  }

  const { text, user } = message;
  if (!text || !user) return;

  console.log(`[bill-ling] DM from <@${user}>: "${text}"`);

  // React with 🤝 if someone is thanking Billington — no text reply
  if (await maybeReactThankYou(text, message.channel, message.ts)) return;

  // "Well done my apprentice" / "Good my apprentice" → "Thank you my master"
  if (MASTER_PRAISE_PATTERN.test(text)) {
    try {
      await app.client.reactions.add({ token: SLACK_BOT_TOKEN, name: "anakin1", channel: message.channel, timestamp: message.ts });
    } catch (_) {}
    await say({ text: "Thank you my master. :anakin1:" });
    return;
  }

  // Star Wars easter eggs
  if (HELLO_THERE_PATTERN.test(text)) {
    await say({ text: "General Kenobi." });
    return;
  }
  if (LEARN_POWER_PATTERN.test(text)) {
    await say({ text: "Not from a Jedi." });
    return;
  }
  if (user === HARGO_USER_ID && SON_PATTERN.test(text)) {
    await say({ text: "Yes, father." });
    return;
  }

  // Resolve sender name so Claude addresses the right person
  let dmSenderName = "Hargo"; // fallback
  if (user && user !== HARGO_USER_ID) {
    try {
      const dmSenderInfo = await app.client.users.info({ token: SLACK_BOT_TOKEN, user });
      dmSenderName = dmSenderInfo.user?.profile?.display_name || dmSenderInfo.user?.real_name || user;
    } catch (_) { dmSenderName = user; }
  }

  const trimmed = text.trim().toLowerCase();

  // Detect if a specific channel is mentioned e.g. #data-billing-updates
  const channelMentionMatch = trimmed.match(/#([\w-]+)/);
  const mentionedChannelName = channelMentionMatch ? channelMentionMatch[1] : null;

  const CHANNEL_NAME_TO_ID = {
    "data-billing-alerts": CHANNELS.DATA_BILLING_ALERTS,
    "data-billing-updates": CHANNELS.DATA_BILLING_UPDATES,
    "team-ops-strategy-only": CHANNELS.TEAM_OPS_STRATEGY,
    "cross-functional-collections": CHANNELS.CROSS_FUNCTIONAL_COLLECTIONS,
    "collab-hire-agreement-audit-updates": CHANNELS.HIRE_AGREEMENT_AUDIT,
  };

  const specificChannelId = mentionedChannelName ? CHANNEL_NAME_TO_ID[mentionedChannelName] : null;

  // Instruction check stays regex — it's a structural command, not a question
  const isInstruction = /^(please\s+)?(remember( this| that)?|note( this| that| down)?|learn( this)?|update your instructions?|add to your (knowledge|instructions?)|from now on|always |never |going forward|save this|instruction:)/i.test(trimmed.replace(/<@[a-z0-9]+>\s*/gi, ""));

  if (isInstruction) {
    if (user !== HARGO_USER_ID) {
      await say("Sorry, only Hargo can update my instructions.");
      return;
    }
    const cleanedInstruction = text.replace(/<@[A-Z0-9]+>\s*/gi, "").trim();
    try {
      saveInstruction(cleanedInstruction);
      await say(`Got it. I've saved that and will apply it going forward.`);
    } catch (err) {
      console.error("[bill-ling] Error saving instruction:", err);
      await say("I hit an error saving that instruction. Please try again.");
    }
    return;
  }

  const aruddOverrideDm = /\barudd\b/i.test(trimmed);
  const intent = specificChannelId ? "channel" : aruddOverrideDm ? "arudd" : await classifyIntent(trimmed);
  console.log(`[bill-ling] Intent (DM): "${intent}" — "${trimmed}"`);

  try {
    if (typeof intent === "string" && intent.toUpperCase().startsWith("CLARIFY:")) {
      await say({ text: intent.replace(/^CLARIFY:\s*/i, "").trim(), mrkdwn: true });
      return;
    } else if (intent === "make") {
      await say("Checking Make.com...");
      let makeData, reply;
      if (isMakeListRequest(trimmed)) {
        const statusFilter = /\bactive\b/i.test(trimmed) ? "active" : /\bpaused\b/i.test(trimmed) ? "paused" : /\boff\b/i.test(trimmed) ? "off" : null;
        makeData = await fetchMakeScenarios(null, statusFilter);
        const prompt = `Hargo asked: "${trimmed}"\n\nHere are the Make.com scenarios:\n${makeData}\n\nList them clearly. Group by ON/PAUSED/OFF if showing all. Keep names exactly as shown.`;
        reply = await askClaude(prompt);
      } else {
        const scenario = await findMakeScenario(trimmed);
        const prompt = `Hargo asked: "${trimmed}"\n\nMake.com scenario detail:\n${scenario}\n\nAnswer Hargo's question directly. Distinguish between "enabled/active" (switched on) and "currently running/executing" (processing right now). Use the "Currently running" field to answer questions about whether it is running right now. Keep it to 2 sentences max.`;
        reply = await askClaude(prompt);
      }
      await say({ text: reply, mrkdwn: true });
    } else if (intent === "arudd") {
      await say("Pulling ARUDD data...");
      const targetDate = await extractAruddDate(trimmed);
      const { report: aruddReport } = await buildAruddReport(targetDate);
      const targetChannel = resolveChannelFromText(trimmed);
      if (targetChannel) {
        await app.client.chat.postMessage({ token: SLACK_BOT_TOKEN, channel: targetChannel, text: aruddReport, mrkdwn: true });
        await say({ text: `Done — posted to <#${targetChannel}>.` });
      } else {
        await say({ text: aruddReport, mrkdwn: true });
      }
      return;
    } else if (intent === "bacs_files") {
      await say("Pulling BACS batch data...");
      const today = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
      let bacsQuestionDm = trimmed;
      const lateFeeMatchDm = trimmed.match(/\blate\s+fees?\b.*?\b(appl|applied|applying|today|tomorrow)\b/i);
      if (lateFeeMatchDm) {
        const dateInMsgDm = trimmed.match(/(\d{1,2}\/\d{1,2}\/\d{4})/);
        const isoInMsgDm = trimmed.match(/(\d{4}-\d{2}-\d{2})/);
        let applyDateDm = today;
        if (/\btomorrow\b/i.test(trimmed)) applyDateDm = nextWorkingDay(today);
        else if (dateInMsgDm) { const p = dateInMsgDm[1].split("/"); applyDateDm = `${p[2]}-${p[1].padStart(2, "0")}-${p[0].padStart(2, "0")}`; }
        else if (isoInMsgDm) applyDateDm = isoInMsgDm[1];
        const lateFeeDueDm = prevWorkingDay(applyDateDm);
        bacsQuestionDm = `bacs batch forecast for due_date ${lateFeeDueDm} (late fees applied on ${applyDateDm})`;
        console.log(`[bill-ling] Late fees rewrite (DM): "${trimmed}" → "${bacsQuestionDm}"`);
      }
      const { result: bacsResult } = await resolveBacsFiles(bacsQuestionDm, today);
      const bacsTargetChannelDm = resolveChannelFromText(trimmed);
      if (bacsTargetChannelDm) {
        await app.client.chat.postMessage({ token: SLACK_BOT_TOKEN, channel: bacsTargetChannelDm, text: bacsResult, mrkdwn: true });
        await say({ text: `Done — posted to <#${bacsTargetChannelDm}>.`, mrkdwn: true });
      } else {
        await say({ text: bacsResult, mrkdwn: true });
      }
    } else if (intent === "bacs") {
      await say("On it. Querying BigQuery...");
      const today = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });

      // --- Dedicated missing bounces handler (deterministic, no LLM SQL) ---
      if (MISSING_BOUNCES_PATTERN.test(trimmed)) {
        const dateMatch = trimmed.match(/\b(\d{4}-\d{2}-\d{2})\b/);
        if (dateMatch) {
          const { report: mbReport } = await buildMissingBouncesForDate(dateMatch[1]);
          await say({ text: mbReport, mrkdwn: true });
        } else {
          const mbReport = await buildMissingBouncesReport();
          await say({ text: mbReport, mrkdwn: true });
        }
      } else {
      // --- End missing bounces handler, fall through to buildBacsSQL ---

      const sql = await buildBacsSQL(trimmed, today);
      console.log(`[bill-ling] Generated SQL (DM): ${sql}`);
      const isSqlDm = /^\s*(SELECT|WITH|SHOW|DESCRIBE)\b/i.test(sql);
      if (sql.startsWith("CLARIFY:")) {
        await say({ text: sql.replace(/^CLARIFY:\s*/i, "").trim(), mrkdwn: true });
      } else if (!isSqlDm && !sql.startsWith("BANK_HOLIDAY:") && !sql.startsWith("VIEW_SOURCE_LOOKUP:")) {
        await say({ text: sql, mrkdwn: true });
      } else if (sql.startsWith("BANK_HOLIDAY:")) {
        const bhDate = sql.replace(/^BANK_HOLIDAY:\s*/i, "").trim();
        const bhName = UK_BANK_HOLIDAY_NAMES[bhDate] || "a UK bank holiday";
        await say({ text: `No instalments fall due on ${bhName} (${fmtDate(bhDate)}) — it's a bank holiday. No collection batch will be generated for that date.`, mrkdwn: true });
      } else {
        const viewMatch = sql.match(/^VIEW_SOURCE_LOOKUP:([^:]+):(.+)$/);
        if (viewMatch) {
          const reply = await getViewSourceMetadata(viewMatch[1].trim(), viewMatch[2].trim());
          await say({ text: reply, mrkdwn: true });
        } else {
          const data = await runBigQuery(sql);

          // Check if user also wants Anchor API cross-reference
          const wantsApiCheckDm = /\b(anchor.{0,10}api|check.{0,15}api|via.{0,10}api|confirm.{0,15}anchor|cross.?ref|verify.{0,15}anchor)\b/i.test(trimmed);
          let apiCrossRefDm = "";

          if (wantsApiCheckDm && ANCHOR_API_USERS.has(user)) {
            const rawRows = await runBigQueryRaw(sql);
            const bqAgreements = rawRows
              .map(r => ({
                id: (r.agreement_id || "").toString().toUpperCase(),
                amount: r.bacs_amount || r.amount || null,
                date: formatBQValue(r.bacs_date) || formatBQValue(r.transaction_date) || null,
              }))
              .filter(a => /^A\d{5,8}$/.test(a.id));

            const maxApi = user === HARGO_USER_ID ? 500 : 10;
            const toCheck = bqAgreements.slice(0, maxApi);

            if (toCheck.length > 0) {
              await say(`Now checking ${toCheck.length} agreement${toCheck.length > 1 ? "s" : ""} via Anchor API (batches of 5)...`);
              const apiResults = [];
              for (let i = 0; i < toCheck.length; i += 5) {
                const batch = toCheck.slice(i, i + 5);
                const batchResults = await Promise.all(
                  batch.map(async (ag) => {
                    try {
                      const xml = await getAgreementFromAnchor(ag.id);
                      return { ...ag, xml, error: null };
                    } catch (err) {
                      return { ...ag, xml: null, error: err.message };
                    }
                  })
                );
                apiResults.push(...batchResults);
                if (toCheck.length > 25 && (i + 5) % 25 === 0 && i + 5 < toCheck.length) {
                  await say(`Checked ${i + 5}/${toCheck.length} agreements...`);
                }
              }

              // Match reversals in JS instead of sending full XML to Claude (token limit)
              const matched = [];
              const unmatched = [];
              for (const r of apiResults) {
                if (r.error) { unmatched.push({ id: r.id, amount: r.amount, date: r.date, reason: `API error: ${r.error}` }); continue; }
                const txnBlocks = r.xml.match(/<a:Transaction>([\s\S]*?)<\/a:Transaction>/g) || [];
                let found = false;
                for (const block of txnBlocks) {
                  const typeMatch = block.match(/<a:TypeId>(\d+)<\/a:TypeId>/);
                  if (!typeMatch) continue;
                  const typeId = parseInt(typeMatch[1], 10);
                  if (typeId < 150 || typeId > 156) continue;
                  const amtMatch = block.match(/<a:Amount>([\d.-]+)<\/a:Amount>/);
                  const dateMatch = block.match(/<a:TransactionDate>([\d-T:.]+)<\/a:TransactionDate>/);
                  const txnAmt = amtMatch ? parseFloat(amtMatch[1]) : null;
                  const txnDate = dateMatch ? dateMatch[1].slice(0, 10) : null;
                  const bqAmt = parseFloat(r.amount) || 0;
                  if (txnAmt !== null && Math.abs(Math.abs(txnAmt) - Math.abs(bqAmt)) < 0.02) {
                    matched.push({ id: r.id, amount: r.amount, date: r.date, anchorTypeId: typeId, anchorDate: txnDate, anchorAmount: txnAmt });
                    found = true;
                    break;
                  }
                }
                if (!found) unmatched.push({ id: r.id, amount: r.amount, date: r.date, reason: "No matching TypeId 150-156 reversal found" });
              }

              let crossRefLines = [`*Anchor API Cross-Reference Results*\n`];
              crossRefLines.push(`✅ *${matched.length}* of ${toCheck.length} agreements have a matching reversal in Anchor`);
              crossRefLines.push(`❌ *${unmatched.length}* of ${toCheck.length} do NOT have a matching reversal\n`);

              if (matched.length > 0 && matched.length <= 50) {
                crossRefLines.push(`*Matched (reversal exists in Anchor):*`);
                crossRefLines.push("```");
                crossRefLines.push("Agreement   | BQ Amount | BQ Date    | Anchor TypeId | Anchor Date");
                crossRefLines.push("------------|-----------|------------|---------------|------------");
                for (const m of matched) {
                  crossRefLines.push(`${m.id}  | £${Number(m.amount).toFixed(2).padStart(8)} | ${m.date} | ${m.anchorTypeId}            | ${m.anchorDate}`);
                }
                crossRefLines.push("```");
              } else if (matched.length > 50) {
                crossRefLines.push(`*Matched:* ${matched.length} agreements (too many to list individually)`);
              }

              if (unmatched.length > 0) {
                crossRefLines.push(`\n*❌ NO MATCH — these need remediation:*`);
                crossRefLines.push("```");
                crossRefLines.push("Agreement   | BQ Amount | BQ Date    | Reason");
                crossRefLines.push("------------|-----------|------------|-------");
                for (const u of unmatched.slice(0, 100)) {
                  crossRefLines.push(`${u.id}  | £${Number(u.amount).toFixed(2).padStart(8)} | ${u.date} | ${u.reason}`);
                }
                crossRefLines.push("```");
                if (unmatched.length > 100) crossRefLines.push(`... and ${unmatched.length - 100} more.`);
              }

              apiCrossRefDm = crossRefLines.join("\n");
            }
          }

          const prompt = `Today is ${today}. The user asked: "${trimmed}"

Query result:
${data}

RESPONSE FORMAT RULES:
- If the result is a single value or a few values: lead with the number or direct answer in 1-3 sentences.
- If the result contains multiple rows (e.g. a breakdown by date, by agreement, by category): present the data as a FORMATTED TABLE or structured list. Show ALL rows — do not truncate or summarise into a single sentence. Use Slack-compatible formatting (monospace blocks or aligned columns).
- If there are more than 50 rows: show the top 20-30 most relevant rows and state how many remain.
- Amount columns are already in £ (decimal) — format with commas and 2 decimal places (e.g. £169,895.53). Do NOT divide by 100.
- No methodology, no caveats, no extra detail unless something is genuinely urgent.

IMPORTANT: For any rows where due_date is in the future (after ${today}), zero anchor bounce counts are EXPECTED — the ARUDD file hasn't been processed into Anchor yet. These rows have batch/GoCardless data (called_for, in_batch) but will not have posted/bounced values until the due date passes. Do NOT flag future dates as mismatches or anomalies.

If the result shows a discrepancy (e.g. anchor bounces != GC failures, posted % < 100%, or any mismatch between systems), mention that you can drill down into bacs_monitoring_agreement_level to identify which specific agreements are causing the discrepancy — offer to do so.
${wantsApiCheckDm ? "\nIMPORTANT: The user also asked to check Anchor's API — this is ALREADY being handled automatically after your response. Do NOT ask 'Do you want me to proceed?' or 'Want me to check the API?' — just present the BigQuery data. The API cross-reference will follow automatically." : ""}`;
          const reply = await askClaude(prompt);
          await say({ text: reply, mrkdwn: true });

          if (apiCrossRefDm) {
            await say({ text: apiCrossRefDm, mrkdwn: true });
          }
        }
      }
      } // end else (buildBacsSQL path)
    } else if (intent === "email") {
      await say("Checking your emails...");
      const gmailQuery = await askClaude(`Convert this request into a Gmail search query string (no explanation, just the query): "${trimmed}"`);
      const emails = await fetchEmails(gmailQuery.trim(), 20);
      const prompt = `Hargo asked: "${trimmed}"\n\nHere are the matching emails (sender, date, subject, preview only):\n\n${emails}\n\nAnswer Hargo's question directly. Share only the specific details they asked for. Keep it concise.`;
      const reply = await askClaude(prompt);
      await say({ text: reply, mrkdwn: true });
    } else if (intent === "channel") {
      await say(`On it. Reading #${mentionedChannelName} now...`);
      const messages = await fetchChannelHistory(specificChannelId);
      const prompt = `Hargo asked: "${text}"\n\nRecent messages from #${mentionedChannelName}:\n\n${messages}\n\nSummarise and flag anything needing Hargo's attention.`;
      const reply = await askClaude(prompt);
      await say({ text: reply, mrkdwn: true });
    } else if (intent === "refunds") {
      const isMarkActionDm = /\b(mark.{0,15}(paid|processed|done|confirmed|complete)|confirm.{0,15}(paid|processed)|update.{0,15}(paid|processed)|post.{0,15}(paid|confirmed).{0,15}thread|(have|has|been|all).{0,15}(paid|processed).{0,15}(via|by|on|through|barclays)?)\b/i.test(trimmed);

      if (isMarkActionDm) {
        await say("On it — checking which threads to update...");
        const { data: rData } = await getRefundsData();
        if (!rData) {
          await say({ text: "I don't have any cached refunds data. Try asking for the refunds update first, then ask me to mark items.", mrkdwn: true });
        } else {
          const batches = (rData.batch_files || []).map((b, i) => ({ index: i, date: b.date, date_iso: b.date_iso, amount: b.amount, payments: b.payments, thread_ts: b.thread_ts, permalink: b.permalink }));
          const processing = (rData.awaiting_processing || []).map((p, i) => ({ index: i, ref: p.ref, description: p.description, thread_ts: p.thread_ts, permalink: p.permalink }));
          const todayDm = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
          const matchPrompt = `Today is ${todayDm}. A user said: "${trimmed}"\n\nBATCH FILES awaiting Barclays confirmation:\n${batches.length > 0 ? JSON.stringify(batches, null, 2) : "None"}\n\nADHOC REFUNDS awaiting processing:\n${processing.length > 0 ? JSON.stringify(processing, null, 2) : "None"}\n\nReturn ONLY valid JSON array:\n[{ "thread_ts": "...", "paid_date": "YYYY-MM-DD", "message": "Paid DD/MM/YYYY — confirmed by <@${user}>" }]\nMatch by amount, date, description, or position. "yesterday" = ${prevWorkingDay(todayDm)}. If unclear, return: [{ "error": "description" }]`;
          try {
            const matchRaw = await askClaude(matchPrompt);
            const jsonMatch = matchRaw.match(/\[[\s\S]*\]/);
            if (!jsonMatch) throw new Error("No JSON");
            const actions = JSON.parse(jsonMatch[0]);
            if (actions.length === 1 && actions[0].error) {
              await say({ text: actions[0].error, mrkdwn: true });
            } else {
              const results = [];
              for (const action of actions) {
                if (!action.thread_ts || !action.message) continue;
                try {
                  await app.client.chat.postMessage({ token: SLACK_BOT_TOKEN, channel: CHANNELS.CUSTOMER_REFUNDS, thread_ts: action.thread_ts, text: action.message, mrkdwn: true });
                  results.push(`✅ Posted: "${action.message}"`);
                } catch (postErr) { results.push(`❌ Failed: ${postErr.message}`); }
              }
              await say({ text: `Done — updated ${results.filter(r => r.startsWith("✅")).length} thread(s):\n\n${results.join("\n")}`, mrkdwn: true });
            }
          } catch (parseErr) {
            await say({ text: "I couldn't work out which threads to update. Can you be more specific?", mrkdwn: true });
          }
        }
      } else if (isRefundsDrillDown(trimmed)) {
        // Drill-down: return specific items with thread links (use cache if fresh)
        await say("Looking that up...");
        const { data: rData } = await getRefundsData();
        const drillReply = buildDrillDown(rData, trimmed) || "Couldn't find data for that. Try asking for the full refunds summary first.";
        await say({ text: drillReply, mrkdwn: true });
      } else {
        await say("Checking #customer-refunds-and-complaints for pending items...");
        const { summary: refundSummary, data: refundData } = await getRefundsSummary();
        const targetChannel = resolveChannelFromText(trimmed);
        if (targetChannel && targetChannel === CHANNELS.CUSTOMER_REFUNDS) {
          if (user !== HARGO_USER_ID) {
            await say({ text: "Only Hargo can post the summary to the refunds channel.", mrkdwn: true });
          } else {
            try {
              console.log("[bill-ling] Posting refunds summary to #customer-refunds-and-complaints");
              await app.client.chat.postMessage({ token: SLACK_BOT_TOKEN, channel: CHANNELS.CUSTOMER_REFUNDS, text: refundSummary, mrkdwn: true, unfurl_links: false, unfurl_media: false});
              postRefundChasers(refundData).catch(e => console.error("[bill-ling] chaser error:", e.message));
              await say({ text: "Posted to #customer-refunds-and-complaints.", mrkdwn: true });
            } catch (postErr) {
              console.error("[bill-ling] Failed to post to refunds channel:", postErr.message);
              await say({ text: `Couldn't post: ${postErr.message}\n\n${refundSummary}`, mrkdwn: true });
            }
          }
        } else {
          // DM summary — no chasers, no channel post
          await say({ text: refundSummary, mrkdwn: true });
        }
      }
    } else if (intent === "agreement") {
      if (!ANCHOR_API_USERS.has(user)) {
        await say("Sorry, only authorised users can query the Anchor API directly.");
      } else {
      let agIdMatches = [...new Set(
        (trimmed.match(/\bA\d{5}(?:\d{3})?\b/gi) || []).map(id => id.toUpperCase())
      )];
      const maxAgreements = user === HARGO_USER_ID ? 500 : 10;
      const concurrency = 5;

      // If no explicit IDs, try to resolve from BigQuery using the user's message context.
      // The message itself often contains enough info (e.g. "the 398 on 2025-07-28", "those agreements").
      if (agIdMatches.length === 0) {
        const refPattern = /\b(those|the \d+|these|them|previous|above|from that|from the|on \d{4}-\d{2}-\d{2}|on \d{2}\/\d{2}\/\d{4})\b/i;
        if (refPattern.test(trimmed)) {
          await say("Resolving agreement IDs from BigQuery first...");
          try {
            const today = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
            const history = getHistory(user);
            const recentContext = history.length > 0
              ? `\nRecent conversation context:\n${history.slice(-6).map(m => `${m.role}: ${m.content}`).join("\n")}\n`
              : "";
            const idQueryPrompt = `You are a BigQuery SQL expert for Raylo. The user wants to look up specific agreements via the Anchor API but hasn't listed explicit agreement IDs. Instead they are referencing a set of agreements by description (e.g. "the 398 on 2025-07-28", "those with no reversal").
${recentContext}
User's current message: "${trimmed}"

Your task: generate a BigQuery SQL query that returns ONLY the agreement_id column for the specific agreements the user is referring to. Use the table \`raylo-production.landing_billington.bacs_monitoring_agreement_level\`.

Key columns in that table: agreement_id, bacs_date, bacs_amount, gc_latest_action, gc_event_cause, anchor_transaction_id, anchor_reversal_id, lifecycle_stage, issue_gc_failed_no_reversal, issue_gc_paid_not_posted, issue_gc_stuck_in_flight, issue_missing_from_gc, issue_no_batch.

Use the clues in the message to build the WHERE clause. For example:
- "398 on 2025-07-28" → WHERE bacs_date = '2025-07-28' AND issue_gc_failed_no_reversal = TRUE AND anchor_reversal_id IS NULL
- "those with no reversal" → WHERE anchor_reversal_id IS NULL AND gc_latest_action IN ('failed','cancelled','charged_back')

Return ONLY the SQL — no explanation, no markdown, no backtick fences.`;
            let idSql = await askClaude(idQueryPrompt);
            idSql = idSql.trim().replace(/^```sql?\n?/i, "").replace(/\n?```$/, "").trim();
            const fenceMatch2 = idSql.match(/```sql?\s*\n([\s\S]+?)\n```/i);
            if (fenceMatch2) idSql = fenceMatch2[1].trim();
            if (!/^\s*(SELECT|WITH)\b/i.test(idSql)) {
              const sqlStart = idSql.match(/\b(SELECT|WITH)\b[\s\S]+$/i);
              if (sqlStart) idSql = sqlStart[0].trim();
            }
            if (/^\s*(SELECT|WITH)\b/i.test(idSql)) {
              console.log(`[bill-ling] Agreement ID resolution SQL (DM): ${idSql}`);
              const idRows = await runBigQueryRaw(idSql);
              const resolvedIds = idRows
                .map(r => (r.agreement_id || "").toString().toUpperCase())
                .filter(id => /^A\d{5,8}$/.test(id));
              if (resolvedIds.length > 0) {
                agIdMatches = [...new Set(resolvedIds)];
                console.log(`[bill-ling] Resolved ${agIdMatches.length} agreement IDs from BigQuery (DM).`);
              }
            }
          } catch (err) {
            console.error("[bill-ling] Agreement ID resolution error (DM):", err.message);
          }
        }
      }

      // Fallback: if no IDs found yet, extract from recent conversation history
      if (agIdMatches.length === 0) {
        const history = getHistory(user);
        const historyText = history.map(m => m.content).join(" ");
        const historyIds = [...new Set(
          (historyText.match(/\bA\d{5}(?:\d{3})?\b/gi) || []).map(id => id.toUpperCase())
        )];
        if (historyIds.length > 0) {
          agIdMatches = historyIds;
          console.log(`[bill-ling] Resolved ${agIdMatches.length} agreement ID(s) from conversation history: ${agIdMatches.join(", ")}`);
        }
      }

      if (agIdMatches.length === 0) {
        await say("I need an agreement ID to look that up — it should look like A00114761. Which agreement did you mean?");
      } else if (agIdMatches.length > maxAgreements) {
        await say(`That's ${agIdMatches.length} agreements — I can only check up to ${maxAgreements} at a time. Please reduce the list and try again.`);
      } else {
        const displayIds = agIdMatches.length <= 10
          ? agIdMatches.join(", ")
          : `${agIdMatches.slice(0, 5).join(", ")} ... and ${agIdMatches.length - 5} more`;
        await say(`Looking up ${displayIds} in Anchor (${agIdMatches.length} agreement${agIdMatches.length > 1 ? "s" : ""})...`);
        try {
          const xmlResults = [];
          for (let i = 0; i < agIdMatches.length; i += concurrency) {
            const batch = agIdMatches.slice(i, i + concurrency);
            const batchResults = await Promise.all(
              batch.map(async (agId) => {
                try {
                  const xml = await getAgreementFromAnchor(agId);
                  return { agId, xml, error: null };
                } catch (err) {
                  return { agId, xml: null, error: err.message };
                }
              })
            );
            xmlResults.push(...batchResults);
          }
          const xmlBlock = xmlResults.map(r =>
            r.error ? `--- ${r.agId}: ERROR — ${r.error} ---` : `--- ${r.agId} ---\n${r.xml}`
          ).join("\n\n");
          const prompt = `The user asked: "${trimmed}"

Here are the raw SOAP XML responses from Anchor for ${agIdMatches.length} agreement(s):

${xmlBlock}

The Agreement node contains these fields (all available if the user asks):
AgreementNumber, AgreementTypeID, Principal, PartExchange, CashDeposit, UpFrontPayments, UpFrontPaymentAmount, BalloonOrResidual, InterestRateUplift, NetInstalment, Interest, AccruedInterest, APR, TotalArrears, Balance, PaymentMethod, CRAArrearsStatus, ACQUISProductCode, Term, InterestOnlyTerm, DueDay, CollectionType, PaymentFrequency, CustomProfile, AgreementDate, FirstPaymentDate, UpfrontPaymentDate, ExternalReference1, ExternalReference2, Purpose, Notes, WarningFlag, TeamStatus, Collector, PersonNumber, WarningText, SalesmanNumber, DealerNumber, SalesAuthority, Branch, Customers, PaymentProfile, Transactions, Goods, CollectionsHistory, CRAFlags, TeamStatusId, ArrearsNET, ArrearsVAT, ArrearsFeeNET, ArrearsFeeVAT, ArrearsIns, ChargesOutstanding, LockedTransactions, AdjustedFinalPayment, BankAccountName, BankSortCode, BankAccountNumber, BankIBAN, BankBIC.

RULES:
1. By DEFAULT (if the user just says "check agreement" or "check arrears"), show ONLY these key fields: AgreementNumber, TotalArrears, Balance, PaymentMethod, CRAArrearsStatus, NetInstalment, DueDay.
2. If the user asks for SPECIFIC fields (e.g. "what's the payment frequency", "show me the transactions", "what's the APR"), show ONLY those fields.
3. If the user asks for "full details" or "everything", show all available fields.
4. If TotalArrears is 0 or empty, say "No arrears".
5. Format amounts as £X.XX. If the agreement is not found or the response contains an error, say so clearly.
6. Keep the response concise and factual — no caveats.`;
          const reply = await askClaude(prompt);
          await say({ text: reply, mrkdwn: true });
        } catch (err) {
          console.error(`[bill-ling] Anchor API error:`, err.message);
          await say(`Error querying Anchor: ${err.message}`);
        }
      }
      }
    } else if (intent === "summary") {
      await say("On it. Fetching all channel messages and generating your summary now...");
      await runDailySummary();
    } else if (intent === "instruction") {
      if (user !== HARGO_USER_ID) {
        await say("Sorry, only Hargo can update my instructions.");
      } else {
        const cleanedInstruction = text.replace(/<@[A-Z0-9]+>\s*/gi, "").trim();
        saveInstruction(cleanedInstruction);
        await say("Got it. I've saved that and will apply it going forward.");
      }
    } else if (intent === "reminder") {
      const lowerText = trimmed.toLowerCase();
      if (/\b(list|show|what)\b.*\breminder/i.test(lowerText) || /\breminder.*\b(list|show|have)\b/i.test(lowerText)) {
        const active = getActiveReminders(user);
        if (active.length === 0) {
          await say("You have no active reminders.");
        } else {
          await say({
            text: `You have ${active.length} active reminder(s):\n` +
              active.map(r => `• *#${r.id}* — "${r.what}" — ${new Date(r.fireAt).toLocaleString("en-GB", { timeZone: "Europe/London" })}`).join("\n") +
              `\n\nTo cancel one, say "cancel reminder #<id>".`,
            mrkdwn: true,
          });
        }
      } else if (/\bcancel\b.*\breminder/i.test(lowerText)) {
        const idMatch = lowerText.match(/#?(\d+)/);
        if (!idMatch) {
          const active = getActiveReminders(user);
          if (active.length === 0) {
            await say("You have no active reminders to cancel.");
          } else {
            await say({
              text: "Which reminder do you want to cancel?\n" +
                active.map(r => `• *#${r.id}* — "${r.what}" — ${new Date(r.fireAt).toLocaleString("en-GB", { timeZone: "Europe/London" })}`).join("\n"),
              mrkdwn: true,
            });
          }
        } else {
          const cancelled = cancelReminder(parseInt(idMatch[1], 10));
          await say(cancelled
            ? `Cancelled reminder #${cancelled.id} ("${cancelled.what}").`
            : "Could not find an active reminder with that ID.");
        }
      } else {
        const parsed = await parseReminder(trimmed, user, dmSenderName, null);
        if (parsed.error) {
          await say(parsed.error);
        } else {
          let targetUserId = user;
          let targetUserName = dmSenderName;
          if (parsed.target && parsed.target !== "self") {
            targetUserName = parsed.target;
            targetUserId = null;
          }
          const reminder = createReminder({
            createdBy: user,
            targetUserId,
            targetUserName,
            channel: event.channel,
            what: parsed.what,
            fireAt: parsed.when,
          });
          const fireTime = new Date(parsed.when).toLocaleString("en-GB", { timeZone: "Europe/London" });
          const targetStr = targetUserId === user ? "you" : targetUserName;
          await say(`Got it. I'll remind ${targetStr} about "${parsed.what}" at ${fireTime}. (Reminder #${reminder.id})`);
        }
      }
    } else {
      const reply = await askClaudeWithHistory(getHistory(user), `[Message from: ${dmSenderName}]\n${text}`);
      await say({ text: reply, mrkdwn: true });
    }
  } catch (err) {
    console.error("[bill-ling] Error handling DM:", err);
    try {
      await say("Sorry, I hit an error processing your message. Please try again.");
    } catch (_) {
      // Best effort
    }
  }
});

// ---------------------------------------------------------------------------
// Express health check server
// ---------------------------------------------------------------------------

const httpApp = express();

httpApp.get("/", (_req, res) => {
  res.json({
    status: "ok",
    name: "Bill Ling",
    uptime: process.uptime(),
  });
});

httpApp.post("/trigger-approval-check", async (_req, res) => {
  console.log("[bill-ling] Manual approval check triggered via HTTP.");
  try {
    await checkRefundApprovals();
    res.json({ status: "ok", message: "Approval check completed." });
  } catch (err) {
    res.status(500).json({ status: "error", message: err.message });
  }
});

httpApp.listen(PORT, () => {
  console.log(`[bill-ling] Health check server listening on port ${PORT}.`);
});

// ---------------------------------------------------------------------------
// Start the Bolt app
// ---------------------------------------------------------------------------

(async () => {
  try {
    initGmail();
    await app.start();
    try {
      const auth = await app.client.auth.test({ token: SLACK_BOT_TOKEN });
      BOT_USER_ID = auth.user_id;
      console.log(`[bill-ling] Bot user ID resolved: ${BOT_USER_ID}`);
    } catch (e) {
      console.warn("[bill-ling] Could not resolve bot user ID:", e.message);
    }
    console.log("[bill-ling] Bill Ling is running in Socket Mode.");
  } catch (err) {
    console.error("[bill-ling] Failed to start:", err);
    process.exit(1);
  }
})();

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------

async function shutdown(signal) {
  console.log(`[bill-ling] Received ${signal}. Shutting down gracefully...`);
  saveReminders();
  try {
    await app.stop();
  } catch (err) {
    console.error("[bill-ling] Error during shutdown:", err.message);
  }
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
