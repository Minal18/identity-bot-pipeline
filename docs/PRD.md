# Real-Time Identity Resolution & Bot Detection — Product Requirements

## The One-Paragraph Version

When someone browses a travel site, we see a browser identifier, sometimes an app device identifier, and sometimes an account login once they sign in. We can't tell whether three of these belong to one real person shopping across their phone, laptop, and work computer — or to one bot pretending to be three people. This pipeline answers that in real time: it links the identifiers that belong together into one identity, then scores how likely that identity is automated. It publishes the answer for other teams to use; it doesn't block anyone itself.

## Key Terms

Everything here is spelled out in full in the document too — this table is just a quick reference.

| Term | What it means |
|---|---|
| Clickstream | The stream of events produced as people browse — page views, searches, clicks. One event per action. |
| **Browser ID** | A long-lived identifier stored in a browser cookie, typically lasting years. Scoped to **one browser**, not to a person or a machine — the same laptop running two browsers produces two browser IDs. Despite often being labelled a "device" ID in clickstream schemas, it is not hardware-based. |
| **App device ID** | An operating-system-level device identifier available only in **app traffic** (and app webviews). Unlike a browser ID, it involves no cookies at all, so clearing browser data doesn't touch it. |
| **Account ID** | The identifier for a signed-in user. Only present after login, which is often late in a session or never. |
| Session ID / Visit ID | Short-lived groupings of one browser's activity — a visit typically ends after ~30 minutes of inactivity. Useful for measuring behaviour, **not** for linking identities. |
| IP address | The network address a device connects from. Many real people share one (a whole office), and people can hide theirs behind a virtual private network. |
| Identity cluster | Our best guess at "all the identifiers belonging to the same visitor," grouped together. |
| Bot | Automated software pretending to be a person. |
| Scraper | A bot that repeatedly reads prices or inventory, usually a competitor or reseller. |
| Attribution | Deciding which ad or campaign deserves credit for a booking, so budget goes to the right place. |
| Customer acquisition cost | The average marketing spend required to win one new paying customer. |
| Return on ad spend | Revenue earned per dollar spent on advertising. |

## An Important Constraint: What Web Traffic Can and Can't Tell Us

This shapes the whole design, so it belongs up front.

**On web, there is no true device identifier.** What clickstream schemas call a "device ID" is almost always a long-lived cookie value tied to a browser. It's durable — often set to expire years out, and frequently stored redundantly across paired cookies so that clearing just one doesn't reset it — but it is still cookie-backed. Deliberately clearing browser data produces a brand-new identifier.

**In apps, we do get a real device identifier**, because apps don't rely on cookies at all.

The practical consequences:

- On web, a browser ID and a "device" ID are **the same signal**, not two independent ones. Treating them as two sources of corroboration would be double-counting.
- **Account ID is the only genuine cross-device bridge on web.** Without a login, two browsers belonging to the same traveler stay separate — correctly, since we have no evidence linking them.
- The most valuable linking evidence of all is an **app webview**, where the app device ID and the browser ID appear on the same event. That single event is what connects someone's app identity to their web identity.
- For bot detection, this means **a bot that clears cookies between requests defeats identity linking entirely** — every request looks like a brand-new visitor. Detection has to lean on signals that don't depend on successful linking. See Scope below.

## Problem Statement

Signals arrive one event at a time, and each on its own is ambiguous. A browser ID tells us "this browser came back," not "this is the same human as that phone." Nothing in the raw data says whether we're seeing one real traveler on three browsers or one bot generating three fake visitors.

That ambiguity causes two problems that cost real money:

**Problem 1 — Bots get credited for ad clicks no human ever made.**
Some bots click our paid ads. Because each bot visit looks like a brand-new person, attribution counts them as real prospects. We then believe a campaign is performing better than it is. Our measured cost to acquire a customer is wrong, our measured return on ad spend is wrong, and the next budget decision is made on bad numbers.

**Problem 2 — Scrapers hide behind fresh identities to evade our limits.**
Price scrapers clear cookies or rotate browser contexts so each request looks like a different shopper. That slips past per-user rate limits, adds load to search and pricing systems no real customer asked for, and pollutes the behavioural data our personalization models learn from — those models end up partly trained on bot behaviour instead of traveler behaviour.

**Why one pipeline serves both:** these look like separate problems but need the same thing underneath — group the scattered identifiers into one identity, then judge whether that identity is automated. Today neither team has it. Marketing discovers bot clicks weeks later while reconciling campaign numbers, then goes back and removes the bad ones. The platform team discovers scrapers when traffic spikes and reacts live. Both are after-the-fact cleanup, and nobody is building the real-time signal the other one needs.

## Goal

Maintain a live, continuously-updated answer to "which identifiers belong to the same visitor," and flag the ones that look automated — fast enough to matter *before* the damage lands. Concretely: before a bot click is credited to a campaign, and before a scraper finishes pulling our prices.

## Non-Goals

- **We don't make the final call.** The pipeline outputs an identity and a bot score with the reasons behind it. What to *do* — drop the click from attribution, open a fraud case, throttle the traffic — is each consuming team's decision. We never block, ban, or discount anything ourselves.
- **We're not verifying who anyone really is.** We group anonymous signals that probably belong together. That's a best guess, not identity verification, and no substitute for looking someone up in the customer database.
- **We don't enforce anything.** No firewall rules, no rate limiting. We produce the signal; enforcement would be built separately on top of it.

## Target Users

| Who | What they get |
|---|---|
| Marketing analytics | Ad performance numbers with bot clicks excluded, so budget decisions use real traveler data |
| Fraud / trust & safety | Early warning when a cluster of fake identities starts forming, rather than after accounts are created |
| Search & pricing platform | Knowing which traffic is scrapers, so they can throttle *those* rather than throttling by IP address — which would also hit real customers sharing an office or home network |
| Personalization | Training data with bot sessions removed, so models learn from real travelers only |

## Scope — v1

**Link identities in real time.** As events arrive, group identifiers that appear together into one identity. This uses union-find, an algorithm for merging groups: when two identifiers show up on the same event, their groups merge, and that merge persists going forward. "Incremental" means we update group by group as events arrive, never recomputing from scratch.

The linking evidence we actually act on:

| Evidence | Where it appears | Why it matters |
|---|---|---|
| Browser ID ↔ account ID | Web, at login | The only way to connect two separate browsers to one person |
| App device ID ↔ account ID | App, at login | Connects app activity to the person |
| App device ID ↔ browser ID | App webviews, where both are on one event | Bridges someone's app identity and their web identity |

Session and visit identifiers are deliberately **not** linking inputs — they subdivide a single browser's activity rather than connecting different identifiers, so feeding them to union-find would add noise and no information.

**Score each identity for bot-likelihood** using four signals:

- **Fan-out** — one identity absorbing an unusual number of identifiers in a short time. Strong on app traffic; weak on web, for the cookie-clearing reason above.
- **Shared IP address** — many *separate* identities connecting from one IP address. This is computed per address rather than per identity precisely so it still works when linking fails.
- **New-identifier churn per IP address** — how many brand-new browser IDs first appear from one address per minute. Real traffic mints these slowly; a bot clearing cookies between requests mints hundreds. **This is the signal that compensates when fan-out collapses**, and it's the main defence against cookie-rotating scrapers.
- **Click timing** — gaps between consecutive events that are too fast to be human, or too evenly spaced. People are irregular; scripts are not.

**Handle out-of-order events.** Signals don't arrive in the order they happened — account ID in particular usually appears at checkout, long after the browsing events from the same session. We process on *event time* (when it actually happened) rather than arrival time, using watermarks: a marker meaning "we believe we've now seen everything up to this point," which tells us when a result is safe to finalize.

**Survive failures without corrupting output.** Exactly-once processing, meaning each event affects the output exactly one time even if a machine crashes mid-stream — no duplicates, nothing lost. State lives in RocksDB (an on-disk store, so the identity graph can exceed available memory), and unused entries expire automatically so it doesn't grow forever.

**Publish results to Kafka** in two forms: an enriched event stream, and a compacted per-identity verdict table. (Details in the tech design — the two shapes exist because some consumers filter traffic as it flows and others look up a verdict after the fact.)

## Out of Scope — v1 (future consideration)

- Acting on the score automatically (auto-excluding clicks, auto-throttling). v1 publishes the signal only.
- Linking identity across separate sibling brands or domains. Single brand only.
- A review screen for humans to inspect and overturn individual bot flags.
- Behavioural fingerprinting beyond what's listed — mouse movement, scroll patterns, and similar are a plausible later addition, not v1.

## Success Metrics

| Category | Metric | Why it matters |
|---|---|---|
| Identity quality | Where we *know* two sessions were the same person (they later logged into the same account on both), how often did we group them correctly before the login revealed it? | Our accuracy check. Logged-in users give us a correct answer to grade against. |
| Identity quality | How often we wrongly group unrelated people — e.g. two people sharing a family laptop and browser profile | The dangerous failure. A wrong merge makes one "identity" look like it holds many identifiers, which is exactly the bot pattern. **A false merge can manufacture a false bot flag.** |
| Identity quality | Share of web sessions that link to an account at all | Tells us how much of the web population we can resolve at all. If low, our web identity coverage is thin and the bot signals must carry correspondingly more weight. |
| Bot detection quality | Share of total sessions flagged, tracked over time | Confirms we're finding a meaningful, stable volume rather than noise. A sudden swing means either an attack or a broken threshold. |
| Bot detection quality | How often we wrongly flag legitimate heavy users — travel agents, affiliate partners | These genuinely browse fast and in volume, so they trip the same signals as bots. Keeping this low is what makes the output safe to act on. |
| Business impact | Estimated ad spend tied to clicks we flagged as bots | The direct answer to problem 1, in dollars. |
| Business impact | Share of search and pricing traffic attributed to scrapers | The direct answer to problem 2, and it tells the platform team how much capacity serves bots. |
| Speed | Time from an event arriving to its identity and score being available downstream | The signal is only useful if it lands before the click is credited or the scrape completes. |
| Reliability | Zero duplicated or lost output records when a processing node is killed mid-stream | Verified by deliberately killing worker processes while data flows, then checking the output is still exactly right. These numbers feed money decisions, so "mostly correct" isn't good enough. |
| Reliability | Stored state size staying flat over time rather than growing | Confirms unused entries really are expiring. Unbounded growth eventually takes the job down. |

## Open Questions

- Real people share IP addresses constantly — families, offices, anyone on a virtual private network. How many false flags will we accept to catch more bots, and who owns that threshold?
- We're building a stored graph connecting browser, app, and account identifiers. Even with automatic expiry, does holding that linked view need its own privacy and retention review, separate from the consent already covering raw event collection?
- If we discover we grouped two people together incorrectly, can we tell downstream teams to correct what they already acted on, or do we let the bad grouping expire and move on?
- Given how much weight rests on account ID for web linking, is there appetite to treat a signed-out-but-previously-signed-in browser as still linked, and for how long?

---
Architecture and implementation detail are in the separate tech design doc.
