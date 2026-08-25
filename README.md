# Fitz Destroyer

Fitz Destroyer is a disposable local recovery harness. It builds Fitz from the
sibling `../fitz` checkout, stores its cloud data in the Sqrzl S3 emulator, and
builds its `@cntryl/fitz` traffic generators into a separate Docker image. The
host driver controls lifecycle; all broker requests originate in disposable
non-root Distroless Node client containers. It does not need AWS credentials or
an AWS account.

This is a correctness and failure-recovery tool, not a performance benchmark.
Its timings are useful for spotting large regressions, but they are not stable
benchmark measurements.

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

`notice-fanout` starts two distinct fleets: `--clients` subscribers and the
same number of publishers. Every subscriber installs a wildcard registration
before any publisher starts. Publishers then send unique, fixed-size payloads
concurrently. The scenario verifies every while-connected subscriber received
every publication exactly once with the original route and bytes. It does not
expect replay after disconnect because Notice is ephemeral.

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
It requires every selected domain to make progress on every client and fails if
any selected-domain client operation errors.

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

Run both with separate isolated stacks:

```sh
npm run destroy -- all --scale smoke --clients 4
```

## Load sizes

| Scale | Durable families | Entries / live operations | Payload bytes | Live concurrency |
| --- | ---: | ---: | ---: | ---: |
| `smoke` | 2 | 20 | 256 | 8 |
| `standard` | 10 | 1,000 | 1,024 | 64 |
| `large` | 10 | 5,000 | 1,024 | 128 |

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
npm run destroy -- notice-fanout --scale standard --clients 8
npm run destroy -- rpc-pressure --scale standard --clients 8
npm run destroy -- rpc-stream-hose --scale standard --clients 4
npm run destroy -- connection-storm --scale standard --clients 8
npm run destroy -- domain-pressure --domains queue,notice --clients 8 --phase-ms 5000
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
failure, the stack is deliberately left intact for inspection and the CLI prints
the exact cleanup command. Pass `--keep` to preserve a successful stack too.

Run artifacts are written to `artifacts/<run-id>/` and include:

- `events.ndjson` with phase timings and counts
- `summary.json` with the final verdict and configuration
- `compose.log` with timestamped Fitz and Sqrzl logs
- `compose-ps.json` with final container state
- per-fault logs captured before killed containers are removed

The harness publishes Fitz only on `127.0.0.1`. Sqrzl is reachable only inside
the Compose network.

## Options

```text
fitz-destroyer <clean-restart|cache-loss|chaos|notice-fanout|rpc-pressure|rpc-stream-hose|connection-storm|domain-pressure|all> [options]

  --scale <smoke|standard|large>  Workload preset (default: smoke)
  --resources <n>                 Families per durable domain
  --entries <n>                   Entries per family
  --payload-bytes <n>             Value/body size
  --seed <n>                      Deterministic unsigned 32-bit seed
  --port <n>                      Loopback Fitz HTTP port (default: 4390)
  --startup-timeout-ms <n>        `/readyz` deadline (default: 180000)
  --clients <n>                   Bombard client replicas (default: 4)
  --phase-ms <n>                  Healthy traffic time around faults (default: 5000)
  --concurrency <n>               Live operations per producer/caller (scale default)
  --handler-delay-ms <n>          Live consumer/worker delay (scale default)
  --domains <list>                Bombard domains (default: all seven)
  --rpc-stream-calls <n>          Streaming RPC calls per caller (scale default)
  --rpc-stream-frames <n>         Response frames per streaming call (scale default)
  --rpc-stream-frame-bytes <n>    Bytes per streaming response frame (scale default)
  --rpc-stream-reader-delay-ms <n> Delay after each received frame (scale default)
  --reuse-images                  Skip builds and reuse existing local images
  --keep                          Preserve a successful Compose stack
```

Use `--reuse-images` for rapid repeated runs only after both local images have
been built from the source you intend to test. The default rebuild remains the
safe choice after changing Fitz or the harness client.

For live-domain scenarios, `--clients N` deliberately creates `2N` client
containers and connections. Notice uses separate publisher and subscriber
fleets; RPC uses separate caller and worker fleets. `--entries` controls the
number of publications per publisher or calls per caller, while
`--concurrency` controls each producer/caller's maximum in-flight operations.
`connection-storm` runs both fleet pairs together, creating `4N` connections per
wave and repeatedly proving that all live state drains before the next wave.
`rpc-stream-hose` uses its dedicated options above because calls, response
frames, and frame bytes are independent destruction dimensions.

Use `--domains` with `chaos` to isolate a noisy domain or test cross-domain
interference, for example `--domains queue` or `--domains queue,notice`.
