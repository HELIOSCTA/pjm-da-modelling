---
description: Log feedback on the morning brief — interactive Q&A + structured persistence to the right specialist's feedback.md so the next run picks it up automatically.
---

# Brief Feedback Session

You are running an interactive feedback-logging flow for the user. The
goal: capture today's correction precisely enough that the next brief
run encodes it automatically — no manual prompt edits.

## Step 1 — quick triage

Ask the user, in ONE message:

```
What's the feedback?

  1. Kind: miss (brief should have caught X but didn't) /
           false-positive (flagged X but it didn't matter) /
           style (format, ordering, length)
  2. Which specialist owns it?
       a. outage-constraint-overlap — outage→constraint→price triples
       b. outage-delta-analyst       — 24h CLEARED/NEW/REVISED
       c. outage-network-curator     — active hotspots + topology
       d. outage-7d-arc              — week-ahead calendar + watchlist
       e. orchestrator               — synthesis (headline, watchlist
                                       compose, section ordering)
  3. State the rule in one sentence, lead with a verb
     (Surface / Suppress / Drop / Always include / Never filter by ...).
```

If the user can't pick a specialist, infer from the rule:

- Anything about $/MWh, shadow prices, constraint precedent → constraint-overlap
- Anything about CLEARED tickets or 24h diffs → delta-analyst
- Anything about substation hotspots, deduped tickets, active outage tables, network gaps, or PSS/E coverage → network-curator
- Anything about the 7-day calendar, single-day clusters, returning today → 7d-arc
- Anything about the headline, watchlist composition, section ordering, or how digests are stitched → orchestrator

If still ambiguous, ask the user — don't guess.

## Step 2 — validate specificity

If the user's rule fails any of these, push back and ask for more:

- Names a specific facility / zone / kV threshold / day / state
  transition — concrete, not vague.
- Lead verb says what the specialist should DO differently
  (Surface / Suppress / Drop / Always / Never — not "Consider").
- Cites the incident that motivated it (the miss / false-positive /
  surprise from today's brief or recent days).

Acceptable: *"Surface ANY 230 kV outage with risk_flag=True regardless
of zone — GRACETON-class tie-corridor risks were missing from the
sub-500 table."*

Reject: *"Make the network section more comprehensive."*

## Step 3 — hand off to the logger subagent

Once you have (specialist, kind, rule, why), invoke the
`brief-feedback-logger` subagent via the Agent tool with a single
structured message:

```
Specialist: <name>
Kind: <miss|false-positive|style>
Rule: <one-sentence rule>
Why: <reason / cited incident>
Date: <today YYYY-MM-DD in EPT>
```

The subagent will:
1. Map specialist → file path
2. Append a dated entry to `backend/mcp_server/runs/<view>/feedback.md`
3. Return confirmation with file path + total rule count

## Step 4 — confirm to the user

Pass the subagent's confirmation through verbatim. Then add ONE line
inviting follow-up: *"Anything else from today, or are we done?"*

If they have more, restart at Step 1. If they're done, exit.

## Anti-patterns

- Don't write to feedback.md yourself — always delegate to
  `brief-feedback-logger`. It encodes the entry format and validates
  the routing.
- Don't promote rules into the specialist's main prompt. That's a
  deliberate weekly-review step, not part of this flow.
- Don't accept vague feedback. The cost of one extra clarifying
  question now beats a useless rule that bloats feedback.md forever.
- Don't run this without a specific brief in mind. If the user hasn't
  generated a brief recently or can't recall a specific item from
  one, ask them to look at today's brief first.
