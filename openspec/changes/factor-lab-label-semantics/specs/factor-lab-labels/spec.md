# Factor Lab Label Semantics

## ADDED Requirements

### Requirement: Forward labels are trading-session offsets and drop-on-untradable

The factor lab SHALL define the h-horizon forward label as the return from the
T+1 session open to that same stock's open `h` trading sessions later. A session
that is suspended, missing a usable price, or sitting at its price limit against
the order side SHALL yield a null label with the reason recorded in
`blocked_h{h}`, and SHALL NOT roll the order forward to a later session.

The offset SHALL be validated against the market trading calendar: when a stock
is missing a quote row such that the positional shift lands on a different
session than the calendar prescribes, the label SHALL be dropped as
`missing_session_between` rather than silently lengthening the holding period.

Each horizon SHALL also carry the mirrored short leg in `fwd_short_h{h}` with
its own `blocked_short_h{h}`. The short leg sells the T+1 open and covers the
h-th open, and is blocked on the opposite side of the book: `limit_down_entry`
(cannot sell into a limit-down open) and `limit_up_exit` (cannot buy back to
cover).

#### Scenario: Weekend inside the holding period

- GIVEN an observation on a Thursday
- WHEN the h=1 label is resolved
- THEN the entry is the Friday open and the exit is the following Monday open
- AND the label resolves, unlike a calendar-window rule that rejects the leg

#### Scenario: Untradable entry yields no label

- GIVEN an observation whose T+1 session opens limit-up for a buy
- WHEN the label is built
- THEN `fwd_h{h}` is null
- AND `blocked_h{h}` records the reason
- AND the observation is excluded from IC and quantile statistics

#### Scenario: Missing quote row

- GIVEN a stock with no quote row on the session after the observation
- WHEN the label is built with the market trading calendar
- THEN `blocked_h{h}` is `missing_session_between`
- AND the label is not built from the next available row

#### Scenario: Mirrored short leg

- GIVEN an observation whose exit session opens limit-up
- WHEN the short label is built
- THEN `fwd_short_h{h}` is null with reason `limit_up_exit`
- AND the long label is unaffected, because a long seller can sell into it

#### Scenario: No roll-forward

- GIVEN a blocked entry session
- WHEN the label is built
- THEN no later session is substituted as the entry
- AND the holding period is never silently extended

### Requirement: Research price limits are board-aware and resolved from the open

The factor lab SHALL resolve the daily price limit from the instrument's board —
main board ±10 %, ChiNext and STAR ±20 % (including their ST names), BSE ±30 %,
main-board ST ±5 % (best effort) — rather than applying a flat ±9.9 % threshold.
A session's limit verdict SHALL come from its **open against its previous
close**, which is the price an order would face, not from its close-to-close
change rate.

A session whose previous close cannot be resolved from `previous_close` or from
`close`/`change_rate` SHALL be treated as untradable with the reason recorded,
so that a limit session can never fabricate an entry.

#### Scenario: ChiNext session up 15 percent

- GIVEN a ChiNext (`sz300*`) observation whose T+1 session opens up 15 %
- WHEN the entry tradability is evaluated
- THEN the entry is tradable
- AND a flat ±9.9 % rule would have wrongly blocked it

#### Scenario: Opens at the limit but closes below it

- GIVEN a T+1 session whose open is limit-up but whose close is not
- WHEN the entry tradability is evaluated
- THEN the entry is untradable, because the buy could not have filled there

#### Scenario: Main-board ST name

- GIVEN a main-board observation flagged `isST` whose T+1 session opens up 6 %
- WHEN the entry tradability is evaluated
- THEN the entry is untradable at the 5 % ST band
- AND the same open on a ChiNext ST name stays tradable at the 20 % band

#### Scenario: Unknown previous close

- GIVEN an observation whose T+1 session has no resolvable previous close
- WHEN the entry tradability is evaluated
- THEN the entry is untradable with reason `missing_previous_close`
