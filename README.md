# Fitz Destroyer

Fitz Destroyer is a disposable local recovery harness. Local runs build Fitz
from the sibling `../fitz` checkout, store its cloud data in the Sqrzl S3
emulator, and route storage through a Compose-local fault proxy. It builds its
`@cntryl/fitz` traffic generators into a separate Docker image. The host driver
controls lifecycle; all broker requests originate in disposable non-root
Distroless Node client containers. It does not need AWS credentials or an AWS
account.

This is a correctness and failure-recovery tool, not a performance benchmark.
Its same-run timings and observed rates are advisory operational evidence, not
capacity measurements, historical comparisons, or performance gates.
`cntryl-stress` is Fitz's authoritative benchmark suite.

## Requirements

- Docker with Compose v2
- Node.js 22 or newer
- the Fitz source checkout at `../fitz`, or `FITZ_SOURCE_DIR` set to its path

## Start here

```sh
npm install
npm run check
npm run destroy -- clean-restart --scale smoke
```

The first run builds the Fitz image and can take several minutes. Later runs use
Docker's build cache.

## Scenarios

`clean-restart` executes this lifecycle:

1. Start Sqrzl and Fitz and wait for `/readyz`.
2. Load Queue, KV, Stream, and Schedule data.
3. Gracefully stop and restart Fitz.
4. Open a fresh client connection.
5. Gracefully stop and restart Fitz again.
6. Verify every deterministic value through the public client.

`cache-loss` proves that Sqrzl, rather than Fitz's local cache, can recover the
data. It loads the same workload, stops Fitz, removes only that run's
`fitz-cache` volume, starts Fitz, verifies the data, then performs one more clean
restart and verifies it again.

`durability-crash-cuts` first acknowledges one Queue enqueue, KV commit, Stream
commit, and Schedule create. It then performs deterministic seeded iterations
around request dispatch, blocked provider access, provider recovery, broker
kill, acknowledgement, and restart with storage in flight. The default is 8
iterations for smoke, 32 for standard, and 100 for large. Its ledger requires
every acknowledged operation to be observable, allows an interrupted operation
to be present or absent, rejects duplicates, and records the seed and cut
identity for every iteration.

`queue-overload-recovery` blocks Fitz's storage path while several clients
issue bounded concurrent Queue bursts. It requires admission failures to be
reported instead of hanging, restores storage, reconciles every acknowledged
enqueue against an exact drain, rejects unstarted or duplicate records, and
requires a fresh Queue probe to complete afterward.

`response-loss` routes one durable request wave through a directional client
proxy that passes client-to-broker bytes while dropping broker replies. The
client must not report an acknowledgement, and a fresh direct verifier
reconciles Queue, KV, Stream, and Schedule outcomes as present or absent while
preserving every acknowledged baseline.

`active-graceful-shutdown` begins Queue, KV, Stream, and Schedule mutations
while a streaming RPC is active, then requests Fitz's normal graceful stop.
The live call must terminate, acknowledged durable outcomes must reconcile
after restart, and a fresh streaming RPC probe must complete.

`half-open-session` silently blackholes both directions of one proxied client
connection. The fitz-ts heartbeat must detect the dead path; after the proxy is
restored, the scenario applies the same stale-handle, Queue redelivery, KV and
Stream rollback, and Lease release assertions as `session-boundaries` without
restarting Fitz.

`authorization-isolation` starts Fitz in authenticated local-development mode
with two JWT identities mapped to distinct route families. Both identities use
the same KV route and key but must recover their own value, while operations in
an ungranted realm must return a permission error. Tokens and the HMAC key are
generated only for the disposable local Compose stack.

`lease-route-aliasing` sends raw Lease requests for every operation with an
extra path segment. Each request must return a bounded protocol rejection while
the exact three-segment canonical Lease remains held and reusable.

`tcp-preauth-framing-slowloris` holds many TCP sessions before CONNECT, using
empty and incomplete outer frames. Every session must close at the
authentication deadline, a second connection wave must be admitted, and both
TCP and WebSocket canaries must complete. This is transport deadline
conformance, not a capacity benchmark.

`connect-pipeline-family-rebind` sends CONNECT and a Queue mutation in one
WebSocket frame and one TCP frame for an identity mapped to RouteFamily 2. The
mutation and reply must both remain in family 2, or the entire operation must
be rejected atomically with no Queue side effect in either family.

`ephemeral-reply-loss-cleanup` creates Queue reservations and watches, KV
transactions and watches, Stream sessions and subscriptions, Notice and
Schedule subscriptions, Lease holders and waiters, and an RPC worker, then
drops setup replies before killing the clients. Server-side state must quiesce
and every route must be reusable from a fresh client.

`saturated-slow-recipient-isolation` pauses reads on one real downstream
socket while publishing at least 30 MiB of Notice payloads. A healthy observer
and seven-domain canaries must make exact progress, and the paused route must
retire cleanly after the pressure phase.

`shutdown-reconnect-cleanup-storm` repeatedly arms connection-bound state in
all seven domains while Fitz begins graceful shutdown. Readiness must become
unavailable, every client must reconnect after the bounded restart, stale
handles must reject, durable partial work must reconcile, and no ephemeral
registrations, workers, holders, or waiters may survive.

`control-lane-cleanup-under-saturation` continuously exercises every domain's
normal work lane while separate clients hold Queue reservations, KV and Stream
sessions, subscriptions, Lease ownership, and RPC workers. Killing those
clients must finish cleanup before saturation stops, while both the saturated
routes and an independent seven-domain canary continue progressing.

`route-family-isolation-matrix` gives two authenticated identities identical
route strings across Queue, KV, Stream, Schedule, Notice, Lease, and RPC. Each
family must retain its own durable state and live delivery; killing one holder
must clean only that family's ephemeral state while the other family remains
fully usable.

`rpc-response-state-conformance` drives wrong correlations, duplicate terminal
responses, late responses after caller cancellation or disconnect, and then
healthy follow-up calls through a one-credit raw RPC worker. Every caller must
terminate once and worker credit must remain reusable.

`response-envelope-boundaries` checks Queue, Stream, KV scan, Schedule listing,
Notice delivery, and RPC response paths at their bounded response envelopes.
Oversized aggregate results must paginate or reject with a typed error, one-over
payloads must reject, and a small follow-up operation must succeed in every domain.

`lease-waiter-disconnect-races` queues four Lease waiters per round, disconnects
them around owner release, and requires zero ghost acquisitions, zero pending
waiters, monotonic fencing, and successful same-route replacement ownership.

`wildcard-registration-quota-reclamation` fills the per-session wildcard
allowance for Queue, KV, Stream, Schedule, Notice, and RPC, proves the next
registration is rejected, then verifies reclamation through both explicit
unsubscribe and transport disconnect. Lease accepts exact subscriptions only.

`stream-selector-cursor-conformance` writes filtered and visible records across
multiple realms, areas, and resources, then checks eight selector shapes across
resource, area, realm, and global cursor axes with one-record pages. A fresh
connection must continue from the prior global cursor without gaps or duplicates.

`same-shard-family-fairness` pins two authenticated route families to one
family-actor shard, continuously fills one family's Notice lane, and requires
every sibling-family delivery canary to complete within the request timeout.

`actor-supervision-failpoint` explicitly enables Destroyer-only broker hooks,
panics the Notice and Queue domain actors one process at a time, requires each
panic to withdraw readiness and drain, then verifies exact Notice fanout and
Queue delivery/completion after clean-process restarts.

`stream-global-recovery` commits an ordered ledger across multiple realms,
areas, and resources, discards Fitz's cache, and replays `stream://**` through
small pages. It requires exact global offsets, resource-local offsets, routes,
payloads, and record counts after reconstruction from Sqrzl.

`queue-dead-letter-fencing` checks the public boundaries around Queue failure
handling: a body that can never fit a reserve response must be rejected before
acknowledgement, an expired delivery token must not complete a redelivered
record, and dead-letter admin mutations without the exact route-family scope
must be rejected. The default production Queue configuration has no retry cap,
so this scenario does not manufacture a dead letter through private storage.

`cold-boot-provider-outage` loads durable state, stops the running Fitz process,
blocks its storage provider, and cold-starts the broker. Fitz must
never report ready while the provider is unavailable. After storage returns,
the scenario waits for readiness and verifies the full durable workload.

`hostile-rpc-worker` registers handlers that either return without a terminal
frame or throw. A missing terminal must time out, while a thrown handler must
produce its bounded terminal error frame. Runtime state must quiesce, and an
independent well-behaved worker/caller probe must still complete afterward.

`upgrade-recovery` loads the durable workload through a source image, replaces
the Fitz container with the configured target image, and verifies every record
through a fresh client. Set `FITZ_UPGRADE_FROM_IMAGE` to an older immutable
image digest for a real cross-version run. Without it, the scenario still
qualifies the container-replacement path and records `crossVersion: false`; it
does not claim binary compatibility from a same-image replacement.

`cross-transport-recovery` loads over WebSocket, discards the Fitz cache, and
verifies over raw TCP. It then loads a separate ledger over TCP, performs a
clean restart, and verifies that ledger over WebSocket.

`outbound-blackhole` drops only broker-to-client bytes while preserving the
client-to-broker path. Heartbeat detection must retire the stale session, all
held handles must reject, durable in-flight state must reconcile, and the
ephemeral Lease must be released after the direction is restored.

`broker-pause` freezes the Fitz container long enough for heartbeat detection
while Queue, KV, Stream, and Lease handles are open. After unpausing the same
process, it applies the full stale-handle, rollback, redelivery, and Lease
release assertions without treating a process restart as the recovery trigger.

`route-cardinality-churn` executes a complete operation on a distinct concrete
route for every selected sequence and every domain, cleans ephemeral and
scheduled state, restarts Fitz, and requires an independent seven-domain
canary. Durable KV and Stream routes intentionally remain as recovery pressure.

`cache-and-disk-exhaustion` runs on two disposable 64 MiB tmpfs-backed Compose
volumes. A root-only helper in the isolated stack fills Fitz's cache and then
Sqrzl's blob volume to `ENOSPC`; each synchronous probe must reject. The helper
removes only its exact filler file, the affected services are reconstructed,
and the acknowledged baseline must verify after both phases. No Docker socket
is mounted and the host filesystem is never used as the fill target.

`session-boundaries` holds a Queue reservation, an uncommitted KV transaction,
an uncommitted Stream append session, and a Lease across a Fitz `SIGKILL` and
reconnect. Every stale handle must reject. The Queue item must redeliver, the
committed KV and Stream baselines must remain, the uncommitted mutations must
not appear, and the ephemeral Lease must be unheld and reacquirable.

`queue-redelivery` fills one Queue, reserves up to 1,024 messages in a victim
client, and kills that exact container without completing them. A fresh fleet
then drains the Queue. The host reconciles every deterministic sequence and
requires the killed client's entire reservation set to reappear exactly once.

`lease-contention` points every client at one Lease route and requires every
critical section to receive a unique fencing token. It then kills a client
inside a held critical section and requires a waiting client to acquire with a
higher token. A final independent query must report no owner and no waiters.
Fencing comparisons stay within one Fitz process lifetime; Lease state is
ephemeral across broker restarts.

`notice-fanout` starts two distinct fleets: `--clients` subscribers and the
same number of publishers. Every subscriber installs a wildcard registration
before any publisher starts. Publishers then send unique, fixed-size payloads
concurrently. The scenario verifies every while-connected subscriber received
every publication exactly once with the original route and bytes. It does not
expect replay after disconnect because Notice is ephemeral.

`schedule-delivery` proves that durable timing intent produces the documented
live handoff. It registers `--clients` wildcard subscribers, creates
`--entries` each of Broadcast, Single, and deliberately canceled schedules,
and verifies the remaining definitions through a fresh listing. Fitz is then
gracefully restarted before the due UTC minute. The scenario waits for every
client to reconnect and re-register, then requires every Broadcast occurrence
to reach every subscriber exactly once, every Single occurrence to reach
exactly one subscriber across the fleet, and every canceled occurrence to stay
silent. Routes and payloads are checked byte-for-byte, and delivery later than
Fitz's documented one-second window fails the run. The surviving definitions
are canceled afterward and Schedule definitions, subscriptions, pending fire
claims, acknowledgement retries, and session cleanup must all drain to zero.
This proves live notification handoff, not durable downstream execution; use a
Queue-backed design when execution itself must survive a consumer failure.

`rpc-pressure` starts `--clients` workers on one shared route, waits for every
registration, then starts the same number of caller containers. Callers keep a
scale-dependent number of requests in flight, verify two ordered response
frames byte-for-byte, and record which remote worker handled each call. The
scenario requires every registered worker to receive work and reconciles caller
and worker totals before passing.

`rpc-stream-hose` makes each RPC return a long deterministic response stream.
The main phase uses slow readers and verifies every frame, sequence, byte, and
terminal response. It then cancels a caller, sends `SIGKILL` to an active RPC
worker, and sends `SIGKILL` to Fitz while calls are streaming. Each destructive
phase must terminate rather than hang, drain RPC runtime state, and pass a fresh
streaming RPC probe afterward. The standard preset starts 100 calls across four
caller containers, with 1,000 maximum-size 65,506-byte bodies per call: about
6.1 GiB of verified responses without constructing the whole stream in memory.
The body cap leaves exactly 29 bytes for the RPC envelope inside Fitz's 65,535
byte TLV value limit.

`connection-storm` repeats live-domain setup, traffic, and teardown in waves.
Each wave concurrently starts `--clients` Notice subscribers, Notice publishers,
RPC workers, and RPC callers, so `--clients 8` creates 32 simultaneous client
containers and connections. The scale's resource count controls the number of
waves, and its live-operation count is spread across them. After every wave the
scenario polls Fitz's live admin snapshots and cleanup metrics until Notice has
no subscriptions or routes, RPC has no workers or pending requests, and no
session cleanup is pending. Any domain failure/drop/rejection counter increase
fails the run; recovered session-cleanup retries are recorded in the artifacts.

`domain-pressure` runs a short, continuously bombarding client fleet without
injecting faults. Use `--domains` to isolate one domain or an interference pair.
It requires every selected domain to make progress on every client in each
ten-second window and fails on definite operation errors. Queue operations with
an unknown durable outcome are accepted only when exact reconciliation proves
that every deterministic sequence resolved at most once.
`pressure-evidence.json` contains per-client/domain/stage totals, latency
percentiles, normalized error samples, Queue reconciliation, broker snapshots,
and diagnostic warnings.

`soak` runs the same exact pressure/reconciliation checks for `--duration-ms`
(15 minutes by default), sampling Fitz every `--sample-ms` (one second by
default). It additionally writes `soak-samples.ndjson`. High p95 latency,
three-sample pending growth, and post-warmup RSS growth are warnings rather than
performance gates.

## Operational guidance

Every scenario records a workload duration and scenario-specific count, rate,
latency, bandwidth, or recovery metrics when its structured completion evidence
is available. The workload envelope begins after image preparation and initial
broker readiness, and ends before the final broker shutdown, artifact
collection, and cleanup. Fault injection, intentional restarts, readiness waits,
and verification are part of the workload they characterize.

An observed rate is completed work divided by that same run's measured workload
or phase duration. It describes only the exercised workload, configuration,
client profile, and host. It is not a maximum sustainable rate or a capacity
claim. Completion labels remain domain-specific: in particular, a Notice
publication is publisher acceptance, not confirmed fanout; verified subscriber
deliveries are reported separately.

Only `domain-pressure` and `soak` receive a categorical advisory rating:

- `constrained`: a definite workload error, ingress dispatch timeout, router
  backpressure, or any stage p95 above 50% of the request timeout was observed.
- `watch`: none of the constrained signals occurred, but a stage p95 was above
  25% of the request timeout, pending work grew through the final samples, or
  the existing post-warmup RSS-growth signal occurred.
- `clear`: none of those saturation signals occurred at the observed rate. It
  does not prove maximum capacity.

The boundaries are strict: a p95 exactly at 25% does not trigger `watch`, and a
p95 exactly at 50% triggers `watch`, not `constrained`. Expected shutdown
cancellations and reconciled ambiguous durable outcomes remain visible but do
not lower the rating. Other scenarios render `not rated` with the reason, while
still reporting their available metrics. Ratings and metrics never change the
Destroyer correctness verdict.

`storage-faults` routes Fitz-to-Sqrzl traffic through the local proxy and cycles
bounded latency, connection reset, a five-second partition, restored-provider
traffic, and a Fitz crash with storage requests in flight. Its ledger records
acknowledged, failed, and ambiguous outcomes plus admission, routing,
persistence, or recovery attribution. A restored proxy must admit a healthy
probe and every acknowledged durable value must survive exactly once.

`queue-lifecycle` covers partial batch completion, lease expiry, disconnect
abandonment/redelivery, a deep deterministic backlog, and consumer progress
across the fleet. `transaction-contention` requires one winner for conflicting
KV commits and verifies rollback isolation, delete visibility, and cleanup of a
killed long-lived transaction. `stream-replay` covers concurrent offset
conflicts, paged and deliberately slow replay, and a 60,000-byte response
boundary with byte-for-byte verification.

`schedule-outage` keeps Fitz down across a due minute and verifies Fitz's
documented no-catch-up rule, then observes the next repeated occurrence while
cancellations race that firing and subscriber acknowledgements are delayed.
`live-churn` composes repeated Notice/RPC registration waves, Lease owner loss
with waiters, and RPC worker replacement while streaming calls are active.

`hot-route-canary` directs all bombarders at shared routes in the domains
selected by `--domains`. Shared KV, Stream, Schedule, and Lease operations can
conflict by design, so their errors are recorded rather than treated as the
canary verdict. While those routes are hot, an independent client performs
exact Queue, KV, Stream, Schedule, Notice, Lease, and RPC round trips on cold
routes. The run fails if a hot domain makes no progress or any cold canary
operation fails.

`protocol-abuse` bypasses the Fitz client for an isolated raw WebSocket phase.
Disposable containers send text-before-CONNECT, empty and truncated TLVs,
domain-before-CONNECT, unknown extended types, duplicate tags, declared-length
truncation, and oversized frames. The scenario does not depend on a specific
connection-close policy; afterward, an official-client canary must still pass
all seven domains.

`chaos` starts a configurable replica set of client containers. Every replica
continuously exercises Queue, KV, Stream, Schedule, Notice, Lease, and RPC. The
host driver then, in order:

1. sends `SIGKILL` to Fitz and restarts it;
2. sends `SIGKILL` to one exact client container, removes it, and restores the
   requested replica count;
3. sends `SIGKILL` to Sqrzl, restarts Sqrzl, and recycles Fitz so every durable
   domain actor is rebuilt from the preserved Sqrzl volume; the client replica
   set is then replaced so every worker starts with a fresh broker session.

After every fault, the driver requires fresh successful operations in every
domain. Expected errors during each outage are counted by the clients and kept
in their logs.

Run the complete ordered suite with isolated stacks and a distinct port per
scenario:

```sh
npm run destroy -- all --scale smoke --clients 4
```

## Load sizes

| Scale | Durable families | Entries / live operations | Payload bytes | Live concurrency | Schedule lead | Fault iterations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `smoke` | 2 | 20 | 256 | 8 | 45 s | 8 |
| `standard` | 10 | 1,000 | 1,024 | 64 | 120 s | 32 |
| `large` | 10 | 5,000 | 1,024 | 128 | 300 s | 100 |

The RPC stream hose has intentionally different presets:

| Scale | Calls / caller | Frames / call | Frame bytes | Reader delay |
| --- | ---: | ---: | ---: | ---: |
| `smoke` | 2 | 100 | 1,024 | 1 ms |
| `standard` | 25 | 1,000 | 65,506 | 1 ms |
| `large` | 100 | 5,000 | 65,506 | 2 ms |

The `standard` scale creates 10 queues, 10 streams, 10 KV tables, and 10
schedule families. Each family receives 1,000 entries, for 40,000 total durable
entries. Use `large` for 200,000 total entries.

```sh
npm run destroy -- clean-restart --scale standard
npm run destroy -- cache-loss --scale large --seed 8675309
npm run destroy -- durability-crash-cuts --scale smoke
npm run destroy -- queue-overload-recovery --scale smoke --clients 4
npm run destroy -- response-loss --scale smoke
npm run destroy -- active-graceful-shutdown --scale smoke
npm run destroy -- half-open-session --scale smoke
npm run destroy -- session-boundaries --scale smoke
npm run destroy -- queue-redelivery --scale standard --clients 8
npm run destroy -- lease-contention --scale standard --clients 8
npm run destroy -- notice-fanout --scale standard --clients 8
npm run destroy -- schedule-delivery --scale standard --clients 8
npm run destroy -- rpc-pressure --scale standard --clients 8
npm run destroy -- rpc-stream-hose --scale standard --clients 4
npm run destroy -- connection-storm --scale standard --clients 8
npm run destroy -- domain-pressure --domains queue,notice --clients 8 --phase-ms 5000
npm run destroy -- soak --duration-ms 900000 --sample-ms 1000
npm run destroy -- storage-faults --scale smoke --iterations 8
npm run destroy -- queue-lifecycle --scale smoke --clients 4
npm run destroy -- schedule-outage --scale smoke --clients 4
npm run destroy -- transaction-contention --scale smoke
npm run destroy -- stream-replay --scale standard --handler-delay-ms 2
npm run destroy -- live-churn --scale smoke --clients 4
npm run destroy -- hot-route-canary --domains queue,kv,stream --clients 8
npm run destroy -- protocol-abuse --scale standard --clients 8
npm run destroy -- chaos --clients 8 --phase-ms 10000
```

You can override the scale dimensions directly:

```sh
npm run destroy -- clean-restart \
  --resources 12 --entries 7500 --payload-bytes 2048 --port 4390
```

## Isolation and cleanup

Every scenario gets a unique Compose project name and Fitz storage prefix. On
success, its containers, network, and both named volumes are removed. On
standalone failure, the stack is deliberately left intact for inspection and
the CLI prints the exact cleanup command. An `all` suite always removes the exact
project for every result, including failures, continues in order, and exits
nonzero after the final scenario when any failed. Pass `--keep` to preserve a
successful standalone stack.

Run artifacts are written to `artifacts/<run-id>/` and include:

- `events.ndjson` with phase timings and counts
- `summary.json` with the final verdict, configuration, and workload timing
- `compose.log` with timestamped Fitz and Sqrzl logs
- `compose-ps.json` with final container state
- per-fault logs captured before killed containers are removed
- Schedule delivery's expected/observed cardinality, missing-sequence samples,
  and client saturation events in `schedule-delivery-observed.json`
- durability crash-cut, response-loss, active-shutdown, Queue overload,
  Queue redelivery, and Lease fencing ledgers
- `pressure-evidence.json` and, for soak, `soak-samples.ndjson`
- `storage-fault-ledger.json` plus Queue/KV/Stream/Schedule/live-churn ledgers

Complete-suite results are written to
`artifacts/suites/<suite-id>/summary.json`, with ordered structured scenario
results and pass/fail totals.

GitHub Actions first builds one multi-role Destroyer harness image, publishes it
to GHCR under the workflow commit, and exposes its immutable digest. Every
concrete smoke scenario then runs as an independent `scenarios` matrix entry
using `compose.destroyer.yml` to pull that exact harness digest and the public
`ghcr.io/cntryl/fitz:latest` image. Each entry uploads its `artifacts/` directory
even when the scenario fails. Local runs continue to use `compose.yml` and build
the sibling Fitz checkout plus the local harness sources so uncommitted changes
can be tested before publication.

After the complete matrix finishes, the `analysis` job downloads all scenario
evidence, runs `npm run check`, and turns the structured scenario summaries into
the workflow's schema-version-3 Markdown and JSON report. The report is shown in the job
summary and retained as the `fitz-destroyer-report-*` artifact. It identifies
the exact workflow run, ref, commit, and evidence artifact for each scenario;
groups failures by classification; retains bounded expandable diagnostics; and
reports cleanup and diagnostic timing context. Its compact operational table
covers every scenario, with detailed pressure/soak domain and stage tables.
Typed count, rate, latency, bandwidth, recovery, completion-semantics, and
rating data remain in `summary.json`; raw histograms and samples stay in their
scenario artifacts. Older artifacts without the workload envelope remain
accepted and degrade unavailable fields to `not rated` instead of failing
report generation. For failed scenarios, analysis
also normalizes recurring Fitz actor-stop, connection-loss, storage-disappearance,
Queue reply-timeout, and KV inventory-warning evidence without claiming that an
observed signal is the root cause. Structured soak warnings remain diagnostic
and do not change the correctness verdict. The Destroyer
workflow bounds the image build at 5 minutes, parallel scenario jobs at 15
minutes, and final analysis at 5 minutes. Its soak matrix entry runs for 8
minutes, keeping the intended wall-clock budget at 25 minutes. Local soak runs
retain the 15-minute default.

The harness publishes Fitz and both fault proxies' ephemeral control ports only
on `127.0.0.1`. Sqrzl and both proxy data ports are reachable only inside the
Compose network. The storage proxy is always in Fitz's storage path; only
scenarios that need transport faults opt clients into the client proxy. The
host controls faults; no container receives the Docker socket.

## Options

```text
fitz-destroyer <clean-restart|cache-loss|chaos|durability-crash-cuts|queue-overload-recovery|response-loss|active-graceful-shutdown|half-open-session|authorization-isolation|stream-global-recovery|queue-dead-letter-fencing|cold-boot-provider-outage|hostile-rpc-worker|upgrade-recovery|cross-transport-recovery|outbound-blackhole|broker-pause|route-cardinality-churn|cache-and-disk-exhaustion|hot-route-canary|lease-contention|notice-fanout|protocol-abuse|queue-redelivery|schedule-delivery|session-boundaries|rpc-pressure|rpc-stream-hose|connection-storm|domain-pressure|soak|storage-faults|queue-lifecycle|schedule-outage|transaction-contention|stream-replay|live-churn|lease-route-aliasing|tcp-preauth-framing-slowloris|connect-pipeline-family-rebind|ephemeral-reply-loss-cleanup|saturated-slow-recipient-isolation|shutdown-reconnect-cleanup-storm|control-lane-cleanup-under-saturation|route-family-isolation-matrix|rpc-response-state-conformance|all> [options]

  --scale <smoke|standard|large>  Workload preset (default: smoke)
  --resources <n>                 Families per durable domain
  --entries <n>                   Entries per family
  --payload-bytes <n>             Value/body size
  --seed <n>                      Deterministic unsigned 32-bit seed
  --port <n>                      Loopback Fitz HTTP port (default: 4390)
  --startup-timeout-ms <n>        `/readyz` deadline (default: 180000)
  --clients <n>                   Bombard client replicas (default: 4)
  --phase-ms <n>                  Healthy traffic time around faults (default: 5000)
  --duration-ms <n>               Soak duration (default: 900000)
  --sample-ms <n>                 Soak/broker sampling interval (default: 1000)
  --iterations <n>                Deterministic fault iterations (scale default)
  --concurrency <n>               Live operations per producer/caller (scale default)
  --handler-delay-ms <n>          Live consumer/worker delay (scale default)
  --schedule-lead-ms <n>          Minimum lead before the due minute (scale default)
  --domains <list>                Bombard domains (default: all seven)
  --client-profile <name>         end-to-end or broker-isolation (default: end-to-end)
  --rpc-stream-calls <n>          Streaming RPC calls per caller (scale default)
  --rpc-stream-frames <n>         Response frames per streaming call (scale default)
  --rpc-stream-frame-bytes <n>    Bytes per streaming response frame (scale default)
  --rpc-stream-reader-delay-ms <n> Delay after each received frame (scale default)
  --keep                          Preserve a successful Compose stack
```

Local scenarios invoke Compose builds and rely on Docker layer caching, so their
evidence corresponds to the current Fitz, client, and proxy sources. Destroyer
workflow scenarios instead pull the exact harness digest produced by the
preceding `build` job, avoiding redundant matrix builds.

The default `--client-profile end-to-end` keeps the configured fitz-ts async
handler concurrency, so a run includes realistic client-side pressure. Use
`--client-profile broker-isolation` for a comparison run; it raises that
dispatcher limit enough that the client's finite callback queue should not be
the first bottleneck. This still uses fitz-ts for encoding, transport, and
domain APIs and therefore is not a pure server benchmark. `protocol-abuse` is
the only raw-WebSocket phase.

Notice and RPC live scenarios deliberately create `2N` client containers and
connections for `--clients N`: Notice uses separate publisher and subscriber
fleets, while RPC uses separate caller and worker fleets. `--entries` controls
the number of publications per publisher or calls per caller, while
`--concurrency` controls each producer/caller's maximum in-flight operations.
`connection-storm` runs both fleet pairs together, creating `4N` connections per
wave and repeatedly proving that all live state drains before the next wave.
`rpc-stream-hose` uses its dedicated options above because calls, response
frames, and frame bytes are independent destruction dimensions.
`schedule-delivery` creates `3 * --entries` definitions, cancels one third
before the due minute, and expects `--entries * --clients` Broadcast deliveries
plus `--entries` Single deliveries after the broker restart. Increase
`--schedule-lead-ms` when a large create set cannot leave ten seconds for the
restart and subscriber recovery before its due minute.

Use `--domains` with `chaos` to isolate a noisy domain or test cross-domain
interference, for example `--domains queue` or `--domains queue,notice`.
