# Real-Time Identity Resolution & Bot Detection — Product Requirements

## The One-Paragraph Version

When someone browses a travel site, we see signals like a device ID, a browser cookie, and sometimes an account login. We can't tell whether three different cookies are one real person shopping on their phone, laptop, and work computer — or one bot pretending to be three people. This pipeline answers that question in real time: it links those signals together into one identity, then scores how likely that identity is a bot. It publishes the answer for other teams to use; it doesn't block anyone itself.

## Key Terms

Everything here is spelled out in full in the document too — this table is just a quick reference.

| Term | What it means |
|---|---|
| Clickstream | The stream of events we get as people browse — page views, searches, clicks. One event per action. |
| Device ID | An identifier for a physical phone or laptop. |
| Cookie ID | An identifier stored in one browser. The same person gets a different cookie on a different browser, or if they clear their cookies. |
| Account ID | The identifier for a logged-in user. We only get this once someone signs in, which is often late or never. |
| IP address | The network address a device connects from. Many real people can share one (a whole office building), and people can hide theirs using a virtual private network. |
| Virtual private network | A service that sends someone's traffic through a different IP address, hiding their real one. |
| Identity cluster | Our guess at "all the IDs that belong to the same visitor," grouped together. |
| Bot | Automated software pretending to be a person. |
| Scraper | A bot that repeatedly reads prices or inventory off the site, usually a competitor or a reseller. |
| Attribution | Deciding which ad or campaign deserves credit for a booking, so we know where to spend the ad budget. |
| Customer acquisition cost | The average amount spent on marketing to win one new paying customer. |
| Return on ad spend | How much revenue we earn for each dollar spent on advertising. |

## Problem Statement

Signals arrive one event at a time, and each one on its own is ambiguous. A cookie ID tells us "this browser came back," not "this is the same human as that phone." Nothing in the raw data says whether we're looking at one real traveler using three devices, or one bot generating three fake visitors.

That ambiguity causes two problems that cost real money:

**Problem 1 — Bots get credited for ad clicks that no human ever made.**
Some bots click on our paid ads. Because each bot visit looks like a brand-new person, our attribution system counts them as real prospects. The result: we think a campaign is working better than it is. Our measured cost to acquire a customer looks wrong, our measured return on ad spend looks wrong, and the next budget decision gets made on bad numbers.

**Problem 2 — Scrapers hide behind fake identities to get around our limits.**
Price scrapers rotate through many cookies and devices so each request looks like a different shopper. That lets them slip past per-user rate limits, it adds load to our search and pricing systems that no real customer asked for, and it pollutes the behavioral data our personalization models learn from — the models end up partly trained on bot behavior instead of traveler behavior.

**Why one pipeline for both:** these look like two different problems, but they need the exact same thing underneath — link the scattered IDs into one identity, then judge whether that identity is automated. Today, neither team gets that. Marketing finds out about bot clicks weeks later, when they reconcile campaign numbers and have to go back and remove the bad ones. The platform team finds out about scrapers when traffic spikes and they have to react to it live. Both are after-the-fact cleanup. Neither team has a real-time signal, and nobody is building one for the other.

## Goal

Keep a live, continuously-updated answer to "which IDs belong to the same visitor," and flag the ones that look automated — fast enough to be useful *before* the damage lands. Concretely: before a bot click gets credited to a campaign, and before a scraper finishes pulling our prices.

## Non-Goals

- **We don't make the final call.** This pipeline outputs an identity and a bot score, plus the reasons behind that score. What to *do* about it — drop the click from attribution, open a fraud investigation, slow the traffic down — is each consuming team's decision. We never block, ban, or discount anything ourselves.
- **We're not verifying who someone really is.** We're grouping anonymous signals that probably belong together. That's a best guess, not identity verification, and it's not a replacement for looking someone up in the customer database.
- **We don't enforce anything.** No firewall rules, no rate limiting. We produce the signal; enforcement would be a separate system built later on top of it.

## Target Users

| Who | What they get from this |
|---|---|
| Marketing analytics team | Ad performance numbers that exclude bot clicks, so budget decisions use real traveler data |
| Fraud / trust & safety team | An early warning when a cluster of fake identities starts forming, instead of finding out after accounts are created |
| Search & pricing platform team | Knowing which traffic is scrapers, so they can limit *those* specifically instead of limiting by IP address — which would also hit real customers sharing an office or home network |
| Personalization team | Training data with bot sessions removed, so recommendation models learn from real travelers only |

## Scope — v1

- **Link identities in real time.** As events arrive, group device IDs, cookie IDs, and account IDs that show up together into one identity cluster. Uses union-find, an algorithm for merging groups: when two IDs appear on the same event, their groups get merged, and the merge is permanent going forward. "Incremental" means we update the grouping event by event rather than recomputing from scratch.
- **Score each identity for bot-likelihood** using three signals:
  - *Fan-out* — one identity absorbing an unusual number of device and cookie IDs in a short time. A real traveler has a handful of devices; a bot farm generates dozens.
  - *Shared IP address* — many separate identities all connecting from the same IP address.
  - *Click timing* — gaps between clicks that are too fast for a human, or too evenly spaced. People are irregular; scripts are not.
- **Handle events arriving out of order.** Signals don't arrive in the order they happened — account ID in particular often shows up much later, at checkout. We process on *event time* (when it actually happened) rather than arrival time, using watermarks: a marker that says "we've probably seen everything up to this point in time," which tells us when it's safe to finalize a result.
- **Survive failures without corrupting output.** Exactly-once processing, meaning every event affects the output exactly one time even if a machine crashes mid-stream — no duplicates, nothing lost. State is stored in RocksDB (an on-disk store, so the identity graph can be bigger than memory), and old unused entries expire automatically so it doesn't grow forever.
- **Publish results to Kafka** — the identity ID and bot score, with the signal breakdown, for other teams to consume.

## Out of Scope — v1 (future consideration)

- Acting on the score automatically (auto-excluding clicks, auto-throttling). v1 only publishes the signal.
- Linking identity across separate sibling brands or domains. Single brand only.
- A review screen for humans to inspect and overturn individual bot flags.

## Success Metrics

| Category | Metric | Why it matters |
|---|---|---|
| Identity quality | Of the cases where we *know* two sessions were the same person (because they logged into the same account on both), how often did we correctly group them before the login told us? | This is our accuracy check. Logged-in users give us the right answer to grade ourselves against. |
| Identity quality | How often we wrongly group two unrelated people together — e.g. two colleagues on the same office network | This is the dangerous failure. Wrongly merging real people makes one "identity" look like it has lots of devices, which is exactly what our bot signal looks for. A false merge can manufacture a false bot flag. |
| Bot detection quality | Share of total sessions flagged as bots, tracked over time | Tells us we're finding a meaningful, stable amount of bot traffic rather than noise. A sudden swing means either an attack or a broken threshold. |
| Bot detection quality | How often we wrongly flag legitimate heavy users — travel agents, affiliate partners — as bots | These users genuinely do browse fast and in volume, so they trip the same signals as bots. Keeping this low is what makes the system safe to act on. |
| Business impact | Estimated ad spend tied to clicks we flagged as bots | The direct answer to problem 1, in dollars. |
| Business impact | Share of search and pricing traffic attributed to scrapers | The direct answer to problem 2, and it tells the platform team how much of their capacity is serving bots. |
| Speed | Time from an event arriving to its identity and bot score being available to other teams | The signal is only useful if it lands before the ad click is credited or the scrape completes. |
| Reliability | Zero duplicated or lost output records when a processing node is killed mid-stream | Verified by deliberately killing TaskManagers (Flink's worker processes) while data is flowing and checking the output is still exactly right. These numbers feed money decisions, so "mostly correct" isn't good enough. |
| Reliability | Stored state size staying flat over time instead of growing | Confirms old entries really are expiring. If this grows without limit, the job eventually falls over. |

## Open Questions

- Real people share IP addresses all the time — families at home, employees in an office, anyone on a virtual private network. How many false flags are we willing to accept to catch more bots, and who decides where that line sits?
- We're building a stored graph of which cookies, devices, and accounts belong together. Even though entries expire, does holding that connected view need its own privacy and data-retention review, separate from the consent that already covers collecting the raw events?
- If we discover we grouped two people together incorrectly, can we tell downstream teams to correct what they already acted on — or do we just let the bad grouping expire and move on?

---
The architecture and implementation details are in a separate tech design doc.
