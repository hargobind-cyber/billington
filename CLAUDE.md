# Billington — Claude Code Operating Rules

## Project Context
Billington is Hargo's AI direct report at Raylo (billing/ops lead).
It runs as a Node.js Slack bot on Hargo's MacBook via PM2.
The main file is `/Users/hargobindbadesha/bill-ling/index.js`.
Never touch Denton — that is a separate bot owned by Chris Aiken.

---

## Workflow Orchestration

### 1. Plan Before Acting
- Enter plan mode for ANY non-trivial change (new feature, intent routing, schema changes)
- If something breaks mid-change: STOP, re-plan, don't keep pushing
- Always read the relevant section of index.js before editing it
- Write the why, not just the what

### 2. Verification Before Done
- After every code change: run `node --check index.js` before restarting
- After restart: check `npx pm2 logs bill-ling --lines 5 --nostream` for errors
- Never mark a task complete without confirming Billington is online and responding
- If restart count (↺) jumps, investigate the crash before continuing

### 3. Autonomous Bug Fixing
- When given a bug report: read the logs, find the root cause, fix it
- Don't ask Hargo for hand-holding — point at the log line, explain it, fix it
- Zero context-switching required from Hargo

### 4. Self-Improvement Loop
- After any correction from Hargo: note what was wrong and why
- Update approach so the same mistake doesn't repeat in this session
- Common past mistakes are documented in the Lessons section below

### 5. Demand Elegance
- Before adding regex intent detection: ask "should Claude classify this instead?"
- Avoid hardcoding data that BigQuery or an API can provide dynamically
- If a fix feels like a workaround, find the real fix

---

## Architecture Rules

### Intent Classification
- ALL intent detection uses Claude (not regex) — this was a deliberate decision
- Regex-based intent was replaced because it was too brittle
- Do NOT reintroduce regex matching for routing decisions

### BigQuery
- Dataset: `raylo-production.landing_billington`
- Billington has access to ALL tables and views in `landing_billington` — currently 1 table but more will be added over time
- Always discover available tables dynamically via `INFORMATION_SCHEMA` rather than assuming table names
- Service account: `billington-bq-viewer@raylo-production.iam.gserviceaccount.com`
- Amounts in `landing_billington` tables are in £ (decimal) — NOT pence. Never divide by 100
- BigQuery DATE fields return as `{value: 'YYYY-MM-DD'}` — always use `formatBQValue()` to unwrap
- For view source tables: always fetch definition from `INFORMATION_SCHEMA.VIEWS` dynamically — never hardcode source tables
- Location: always pass `location: 'EU'` to all BigQuery queries

### Make.com
- Base URL: `https://eu1.make.com/api/v2`
- Team ID: 74172 (Raylo) — this is the only team Billington queries
- Billing folders (used for list requests): 217646, 283892, 141356
- Scenario list is cached for 5 minutes — don't over-fetch
- "Active" = switched on. "Currently running" = actively executing right now. These are different

### Slack
- Socket Mode — no public URL, no webhooks
- Bot token: `SLACK_BOT_TOKEN`, App token: `SLACK_APP_TOKEN`
- Hargo's user ID: `U01DMHVF8KG` — only Hargo can give standing instructions
- 3pm cron posts to `#data-billing-updates` (not a DM)
- 1pm cron DMs Hargo directly

### Environment
- `.env` is loaded with `override: true` — this is critical, do not change it
- PM2 process name: `bill-ling`
- Port: 3048
- Knowledge files: `~/bill-ling/knowledge/*.txt` — loaded at startup

---

## Response Style Rules (enforced in prompts)
- Lead with the number or direct answer
- £ amounts: formatted with commas and 2 decimal places
- Counts: `#` prefix with commas
- No hedging, no methodology, no caveats unless urgent
- More detail only if asked or something is genuinely wrong

---

## Scheduled Jobs
| Job | Time | Destination |
|-----|------|-------------|
| Daily channel summary | 1pm Mon-Fri | DM to Hargo |
| BACS batch report | 3pm Mon-Fri | #data-billing-updates |

---

## Lessons Learned
- **Amounts were divided by 100 incorrectly** — `landing_billington` tables store £ as decimal, not pence
- **`percent_of_instalment_due_called_for` is NULL in the view** — calculate from counts instead
- **`dotenv` override is critical** — without `override: true`, Claude Code's empty env var wins
- **Regex intent detection was too brittle** — replaced with Claude classification
- **`history.push` crash** — conversation history must be initialised as an array per user, not shared
- **BigQuery DATE objects** — must use `formatBQValue()` or they render as `[object Object]`
- **`due_date > CURRENT_DATE()`** for 3pm report — shows all future batched dates, not just today's
- **Make.com team scope** — only query team ID 74172 (Raylo), not 453539

---

## Core Principles
- **Minimal Impact**: Changes should only touch what's necessary
- **No Laziness**: Find root causes. No temporary fixes
- **Simplicity First**: Make every change as simple as possible
- **Ask Hargo Before**: Any change to scheduled job destinations, credentials, or channel targets
