# Brief 0001: the first customer journey

Status: draft 2026-09-07, second revision under review. A product brief,
not a record: it names the first complete customer journey the products
must support, what that journey must demonstrate, what the numbers about it
may and may not claim, and how it is accepted, so that the first
implementation record on the sales and marketing lane is chosen by the
journey rather than by the plumbing. It decides no mechanism and no code
placement.

## Why this journey first

The operator's stated fear is no longer the polish the desktop products
need; it is the long build of a sales and marketing platform that runs on
Toron, keeps its numbers in Toron, and shows them in Polariton. That
platform is many products. The way to start it is not an ingest path that is
technically complete and helps no one become a customer; it is one journey
that works for a person end to end and is observable through our own
products from the first cut. Later journeys - evaluation to purchase,
purchase to continued use - follow the same shape.

The journey is: **a person discovers Polariton, gets the right build, runs
it, and produces a first useful analysis.** The business question it must
answer, in Polariton over Toron, is: where did people arrive from, where did
they stop, and which failures stood between them and the first analysis?

## What "first useful analysis" means

A figure, over data the person brought, that they kept: published as a
package or saved in a session, with at least one deliberate change after
the first render - a series hidden, a setting written back, a filter
applied. A showcase figure the product put in front of them does not count.
A figure over their own data that they closed without keeping does not
count. The definition is deliberately conservative so that the funnel's
last step means something; a later journey can add a looser leading
indicator beside it, never in place of it.

## The journey, step by step, against today

| Step | What the person does | What exists today | What the journey needs |
|---|---|---|---|
| Discover | Arrives at a page that says what Polariton is and shows one | Nothing public. The repo README describes a Rust workspace. | One page with one real published dashboard on it, and the Rune behind it one click away. |
| Get | Downloads a build for their machine | `cargo install --locked --path crates/polariton` from a checkout; a deb asset list in the manifest; Linux only in practice. Mac and Windows heads are pending the two new machines. | A signed download per platform, or an honest "Linux today, Mac and Windows on these dates" with a checkout path that works. |
| Run | Opens it and sees something | The window opens on an empty ephemeral buffer; a SQLite scratch database is always present; the local workspace attaches only if `nilket local` is running. No first-run state exists. | A first-run that puts a working example in front of them without a database, tells them what they are looking at, and hands them the next step toward their own data. |
| First analysis | Points it at their own data and gets one figure they keep | Adapters for Postgres, MySQL, SQLite, BigQuery, and Parquet; Rune views; publish to a package. | A guided path from "here is my file or connection" to a kept figure, with the failures along it named in the product's own words. |

Every row's third column is a product gap the journey makes concrete.

## What the products must demonstrate

1. **The customer succeeds.** The deliverable of every cut on this journey
   is a step a person can complete, with its instrumentation and its view
   included. Emitting an event is not a customer's success; a cut that
   ships an emitter without the step it observes has shipped nothing.
2. **The journey is observable in our own products.** Each step reports to
   Toron under a fixed schema, and a Polariton view over those reports
   answers the business question above. The view is part of acceptance,
   not a follow-up.
3. **Failure is a first-class report.** A download that did not start, a
   run that could not open a window, a source that refused, a workspace
   that was not there: each is a report with a reason, because "where did
   they stop" is the question.
4. **The example is real.** The dashboard on the discovery page is a
   published package from the same publisher the person will use, produced
   by the batch driver on a schedule, over the journey's own data. The
   "inspect how it was made" step opens the Rune that made it.
5. **The person's data stays theirs.** Nothing about the person's own
   analysis - their source, their queries, their figure - is reported. The
   journey records that a first analysis happened and what kind of source
   it used, never what it was.

## The observation contract

The funnel is honest only if it says what it does not know.

- **Observed, absent, and unknown are three states.** A step is *observed*
  when a report for it arrived; *absent* when a later step for the same
  journey was observed but this one was not; *unknown* when no later report
  exists. A person who downloaded and was never heard from again is
  unknown, not abandoned. A launch that crashed before it could report is
  unknown. The view shows unknown as its own column and never folds it into
  a stop.
- **Later steps are lower bounds.** Anything reported from the desktop
  depends on desktop reporting being present and allowed, so counts after
  "Get" are floors, and the view says so beside them.
- **Correlation is explicit and consent-shaped.** A browser session and a
  desktop run are correlated only through a token the person carried
  deliberately from the page to the build - a download that embeds one, or
  a code they paste - never through fingerprinting. Without the token, the
  desktop reports stand alone as their own journeys, starting at "Run".
- **Reporting can be off, and off is a state, but not a denominator.**
  Desktop reporting is visible on first run with a one-click off and a
  setting that stays put. Whether it defaults on or off is the first
  record's decision, as is what choosing off does on the wire: sends one
  final "reporting off" report, cancels anything queued, or sends nothing
  at all. Whichever it is, the view must distinguish "reporting off" from
  "stopped": a run whose last report says reporting is off is observed
  once, then unknown, and is counted as neither a stop nor a success. Runs
  that never report - installations that start with reporting off, builds
  that never reach the endpoint - are invisible, so the total population of
  runs is unknown and the view says so; the off count is a count of
  observed reporting-off runs, nothing more.
- **Reporting unavailable is not abandonment.** A desktop that cannot reach
  the reporting endpoint queues its reports and retries; if the queue is
  lost, the journey is unknown from that point. The site's own outage is
  covered under recovery below.

## The business view

The acceptance view is one Polariton dashboard over the journey's reports:

- Arrivals by source and day.
- A funnel: arrived, got a build, ran, first analysis - with observed,
  absent, and unknown as separate series at each step.
- Stops: at which step people were last observed, by platform, with the
  unknown column beside it.
- Failures: reasons by step, ranked, for observed failures only.
- Time between steps, as a distribution, for journeys observed end to end.
- Observed reporting-off runs: how many desktop runs reported "off"
  before going quiet, labelled as observed, beside a stated unknown total
  population. The floors above have no full denominator, and the view does
  not pretend to one.

It is authored in Rune, published as a package, and served on the discovery
page as the example. That is the dogfood claim made literally: the page's
own dashboard is the journey's own numbers.

## The public boundary

The glass box is a designed public dataset, and a public page never runs a
Toron query.

- **Dashboards are artifacts.** A public dashboard request serves a
  published package: static files, produced by the batch driver on a
  schedule from tables materialized for that purpose. Materialization
  bounds what is computed; serving artifacts bounds what a visitor can
  cause, which is nothing. Toron has no query-wide concurrency control
  today, and even a materialized table costs the engine to read, so the
  request path stops at the artifact.
- **The per-visit receipt is its own bounded design, and it says which of
  two things it proves.** "Your event was accepted and reached an
  aggregate" cannot be a query per visitor, and it is two claims. The
  site acknowledges the event with an opaque receipt id at ingest: that
  proves durable acceptance by the site's queue, not persistence in Toron,
  and the page says "accepted", not "stored". A scheduled materialization
  later produces a compact index of receipt ids that reached an aggregate,
  served as an artifact the page checks: that proves inclusion in a Toron
  aggregate, with the delay shown. The first record that builds the page
  decides the index's shape. The constraint is that no public request
  synchronously executes an engine query; visitor events reach Toron, by
  design, through ingestion.
- **Two public views**: *your visit*, the receipt above; and *the running
  service*, a small set of numbers designed to be explained - throughput,
  latency distribution, storage growth, maintenance activity - with the
  delay and the sampling labelled on the page.
- **Every public number is designed with its reporting half at the same
  time.** A counter that reads well internally and has no view reads as
  zero.
- **The live connection into a window stays off the public surface.** It
  is a same-user socket for an operator's agent, not a web endpoint.
- **Candour about operational reality is part of the design**: restarts,
  delayed ingestion, maintenance pauses are shown, labelled, and dated.

## Durability, authority, and the shadow

Three ideas that this brief keeps apart, because the first draft did not:

- **Authority** is where the application's state is decided. Toron holds
  customer state - who arrived, who got a build, who reached a first
  analysis - as an event-sourced record, and it is the one authority.
- **Durable retention of commands** is a different thing. A site that
  queues reports durably before Toron acknowledges them, and replays them
  after an outage, has a second durable record without a second authority:
  the queue holds intent, Toron holds state. The recovery requirement below
  depends on exactly that queue. A durable record is not an authority; the
  first draft's equivalence was wrong.
- **Independent verification** that Toron kept every acknowledged write -
  comparing what was sent against what Toron holds - is a third thing. It
  is possible, since the queue above is a record of what was sent, and it
  is left out of this journey's scope by choice, not because it would make
  a second authority.

The SQLite shadow is a fourth thing: a copy replayed from Toron's own event
stream, for **failures after replication** - a lost node, a corrupted store,
a migration gone wrong, running the numbers where Toron is not. Because it
is built from Toron's account of what it kept, it says nothing about the
pre-replication case. Its acceptance is a scheduled replay-and-compare that
produces the business view from the shadow alone and matches.

## Recovery requirement

If Toron is unavailable, the discovery page still serves its last published
package, the download still works, and reports queue durably at the site
until Toron returns. No step of the person's journey depends on the
analytics being up. The business view shows the gap, labelled, and the
funnel counts the replayed reports when they land, with their original
timestamps.

## Left open on purpose

- **The license.** The repository is MIT today and the operator has said
  that is not settled. Two rows depend on the answer: what a visitor may do
  with the Rune they inspect, and what a person who downloads a build
  agrees to. The decision must come before the discovery page ships. What
  was verified on 2026-09-07: every one of the 2,519 commits across the
  whole history, including the migrated engine history, has one author and
  one committer; four commits carry a co-author trailer naming an AI
  assistant and no other person. The licenses of dependencies bound
  what can be distributed and are a separate check the license decision
  must include. This audit is evidence about authorship as git records it;
  it does not establish ownership. Commit metadata says nothing about the
  provenance of code that was copied or imported, and copyright ownership
  can differ from authorship. The license decision needs its own provenance
  check before it is made.
- **Website code placement.** Not decided here. It is settled when the
  first implementation cut has concrete dependencies, and a separate
  deployment cadence does not by itself imply a separate repository.
- **Which mechanism carries reports from a browser or a desktop to Toron.**
  The first record decides, against the schema this brief implies.
- **Whether desktop reporting defaults on or off.** The first record
  decides; the view is correct either way.
- **Mac and Windows builds.** A gate once the machines arrive; the "Get"
  row is honest about Linux until then.
- **Payments, identity, mail.** Not in this journey. Commodity services when
  the second journey needs them; using one does not weaken the dogfood.
- **Managed Toron and a Kubernetes operator.** A third product. The journey's
  numbers are what would justify it.

## Acceptance across the whole experience

1. A person with a Linux machine and no checkout of this repository reaches
   the discovery page, gets a build, runs it, makes one deliberate change
   to a figure over a Parquet file of their own after its first render,
   and keeps it, following only what the page and the first-run show them.
   An operator watches without helping.
2. Every step that run observed, and one deliberately induced failure at
   each step that can report, appears in the business view within the
   labelled delay; a failure induced before reporting is possible - the
   binary killed before its first report - appears as unknown, not as a
   stop.
3. The discovery page's dashboard is that business view, published by the
   batch driver from a checked-in commands file, and "inspect" opens its
   Rune.
4. No public request executes a Toron query. A test proves the dashboard
   and receipt paths serve artifacts only, and that a burst of visitor
   requests does not change the engine's query count.
5. A journey reported with desktop reporting off appears once and then as
   unknown, and is counted as neither stop nor success; a correlated
   journey and an uncorrelated one both appear correctly.
6. The shadow reproduces the business view from its own copy and matches.
7. With Toron stopped, steps 1 through 3 of the person's journey still
   complete, the queued reports land after Toron returns with their
   original timestamps, and the view shows the gap.

## The first implementation record

The first record delivers the **Run** row as a customer's step: a first-run
experience that puts a working example in front of a person without a
database, tells them what they are looking at, and hands them the next step
toward their own data - together with the desktop reporting for that step,
the reporting-state choice, and the business view over what it reports.
The step is chosen because it lives entirely within the desktop product, so
it can be delivered and measured without the page or the download path
existing yet, and because it is the step where "where did they stop" is
answerable by the product itself. The discovery page, the download path,
the per-visit receipt, and the shadow follow as their own records once
there is a first-run for them to lead to.
