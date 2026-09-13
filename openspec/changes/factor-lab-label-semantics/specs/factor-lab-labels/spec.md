# Factor Lab Label Semantics

## ADDED Requirements

### Requirement: Forward labels are positional and drop-on-untradable

The factor lab SHALL define the h-horizon forward label as the return from the
T+1 session open to that same stock's open `h` trading sessions later. A session
that is suspended, missing a usable price, or sitting at its price limit against
the order side SHALL yield a null label with the reason recorded in
`blocked_h{h}`, and SHALL NOT roll the order forward to a later session.

#### Scenario: Weekend inside the holding period

- GIVEN an observation on a Thursday
- WHEN the h=1 label is resolved
- THEN the entry is the Friday open and the exit is the following Monday open
- AND the label resolves, unlike a calendar-window rule that rejects the leg

#### Scenario: Untradable entry yields no label

- GIVEN an observation whose T+1 session is limit-up for a buy
- WHEN the label is built
- THEN `fwd_h{h}` is null
- AND `blocked_h{h}` records the reason
- AND the observation is excluded from IC and quantile statistics

#### Scenario: No roll-forward

- GIVEN a blocked entry session
- WHEN the label is built
- THEN no later session is substituted as the entry
- AND the holding period is never silently extended

### Requirement: Research price limits are board-aware

The factor lab SHALL resolve the daily price limit from the instrument's board —
main board ±10%, ChiNext and STAR ±20%, BSE ±30%, ST ±5% (best effort) — rather
than applying a flat ±9.9% threshold. A session with no usable change rate SHALL
be treated as untradable so that a limit session cannot fabricate an entry.

#### Scenario: ChiNext session up 15 percent

- GIVEN a ChiNext (`sz300*`) observation whose T+1 session is up 15%
- WHEN the entry tradability is evaluated
- THEN the entry is tradable
- AND a flat ±9.9% rule would have wrongly blocked it

#### Scenario: Missing change rate

- GIVEN an observation whose T+1 session has no change rate
- WHEN the entry tradability is evaluated
- THEN the entry is untradable
- AND the reason is recorded
