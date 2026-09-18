# Log Replication: the sink's snapshot lease

This document is for whoever operates, upgrades or debugs Log Replication (LR) snapshot sync.
It describes what the snapshot lease is, which bounds it enforces, how to size and monitor it, and
what happens during an upgrade from the version that used the checkpointer freeze token.

## Why it exists

A snapshot sync first transfers the source's data into shadow streams on the sink and then applies
them to the regular streams. Shadow streams are not checkpointed, so until the apply has read them
they must not be trimmed. The sink therefore keeps its checkpointer from starting a new cycle for
the duration of a snapshot sync.

In the previous protocol the source drove that freeze: every snapshot sync it started froze the
checkpointer through a freeze token, and only a clean end of that sync unfroze it. The token's only
safety net was a two hour patience that restarted with every new freeze. A snapshot sync that kept
failing and being restarted (timeouts, network disruption, restarts on either side) therefore kept
the checkpointer frozen for as long as the failures lasted. Nothing was checkpointed, nothing was
trimmed, and the sink's log grew until the disks filled.

The lease turns this around. **The sink owns the decision.** It decides whether a snapshot sync may
start, for how long it may keep the checkpointer frozen, when that protection is released, and what
has to happen before the next snapshot sync is admitted. None of these depend on the source behaving
well, or on the source being alive at all.

## How a snapshot sync runs

The lease is one durable record in the sink's store (`CorfuSystem$SnapshotSyncLeaseTable`). The LR
leader of the sink cluster drives it; the source only reads it, as part of the status the sink
already reports.

| Phase | Meaning | Checkpointer |
|---|---|---|
| `NOT_READY` | This node does not lead, or has not acquired the record yet. | as recorded |
| `READY` | Admission is open. | free |
| `PREPARING` | An attempt is reserved; the sink prepares its writers and notifies the plugin. | frozen |
| `TRANSFERRING` | The source sends snapshot data into the shadow streams. | frozen |
| `APPLYING` | The transfer is durable; the sink applies it on its own thread. | frozen |
| `ABORTING` / `RELEASING` | The attempt ended (abandoned / completed); the sink cleans up. | frozen |
| `FAULTED` | Cleanup exceeded its bound and is retried; an alarm is raised. | frozen |
| `RECOVERING` | Protection is released; the next snapshot waits for a checkpoint and trim. | free |

1. The source polls the sink's status. When the lease is `READY` it proposes a snapshot sync
   (`SNAPSHOT_START`) that quotes the lease's admission epoch.
2. The sink reserves the attempt: it records the attempt's identity, its **deadline** (now plus the
   budget) and the log position everything after which is protected. From this transaction on the
   checkpointer does not start a new cycle. A cycle that was already running with an older cut is
   unaffected and may still trim up to that position.
3. While the sink prepares, the source is told to come back (`BUSY`). It repeats the same proposal
   and is then answered `SNAPSHOT_START_ACCEPTED` with the attempt's generation. Only now does the
   source send data; every message carries the attempt's identity and generation.
4. `SNAPSHOT_END` moves the lease to `APPLYING` and marks the sink's data inconsistent in the same
   transaction. The sink applies the snapshot by itself. A transient failure is retried by the sink
   within the same budget. During the transfer, a write that the sink's sequencer refused (it failed
   over, for instance) is simply sent again by the source; any other write failure abandons the
   attempt, because the batch may have been written and must not be written twice.
5. Completion, the applied marker and the data consistent flag commit in one transaction. The sink
   installs its incremental writer and the source, which follows the lease through its status polls,
   moves on to incremental sync.
6. The sink releases the protection and the lease moves to `RECOVERING`. Incremental sync is not
   affected by this phase. A new **snapshot** is admitted once the conditions under
   [The recovery gate](#the-recovery-gate) hold.

Anything the sink cannot process at the moment is answered with a typed `BUSY` reply that carries
the lease, never by dropping the message: `ADMISSION_CLOSED` (come back later), `STALE_ATTEMPT`
(this attempt is over, start again from the status), `OVERLOADED`, `UNSUPPORTED_PROTOCOL`.

## What bounds the freeze

Every row is enforced independently of the others.

| What goes wrong | What ends the freeze | Default |
|---|---|---|
| The source disappears during preparation or transfer (crash, failover, partition). | The sink abandons an attempt that received no snapshot traffic for the idle interval. | 5 min |
| The source is alive but makes no progress, or the transfer or apply is simply too slow. | The attempt's budget. It is fixed at admission. No traffic, progress, retry or restart renews it. | 90 min |
| The apply keeps failing. | Retries stop at the retry limit or at the budget, whichever comes first. A failure that cannot succeed by retrying (the shadow data is gone) ends the attempt at once. | 10 retries |
| The source cancels or stops (it tells the sink), the topology or the cluster role changes, LR leadership moves, the sink restarts. | The attempt is abandoned by the sink (the next leader does it when it takes the record over). An abandoned attempt keeps its original deadline. | immediate |
| The sink is hung, or the sink cluster has no LR leader at all. | The checkpointer itself stops honoring protection that is older than its deadline plus a grace. No writer of the attempt can commit after the deadline, so what it wrote is garbage by then. | deadline + 30 min |
| Snapshot syncs fail back to back. | The recovery gate: between two attempts one checkpoint cycle must complete and the log must be trimmed. | one cycle |

The worst case for a single attempt is therefore **budget + grace** (two hours with the defaults),
and consecutive attempts cannot chain, because each one owes a completed checkpoint and trim before
the next is admitted.

### The recovery gate

After every attempt, successful or not, the lease records a recovery cut (the sink's log position
when the protection was released). The next snapshot is admitted when all of these hold:

* the protection has been released for at least `snapshot_lifecycle_min_recovery_ms`;
* a compaction cycle that started after the cut has `COMPLETED`;
* the log has been trimmed past the cut.

The sink does not wait for the compactor's own schedule: it requests a cycle with trim as soon as it
enters `RECOVERING`. If a cycle ends without satisfying the gate it asks again, but not back to back:
each further request waits twice as long as the previous one (one minute, up to fifteen), so that a
cycle that keeps failing is not run over and over. It does not ask at all while a satisfying cycle
has completed and only its trim or the minimum interval is pending.

The gate is skipped when no checkpointer can run at all, because waiting would then keep replication
closed forever without protecting anything: when the compaction manager record does not exist (no
compaction service is configured; a configured one creates the record within seconds of starting,
even while frozen) or when an operator disabled compaction.

## Configuration

Sink side, in the LR configuration file (`corfu_replication_config.properties`). A value that would
make a bound meaningless falls back to its default.

| Key | Default | Meaning |
|---|---|---|
| `snapshot_lifecycle_max_duration_ms` | 5400000 | Budget of one attempt: preparation, transfer, apply and apply retries. Must be positive. |
| `snapshot_lifecycle_transfer_idle_ms` | 300000 | Abandon an admitted attempt after this long without accepted snapshot traffic. Zero or less disables it. |
| `snapshot_lifecycle_max_apply_retries` | 10 | Retries of a transiently failing apply, inside the same budget. |
| `snapshot_lifecycle_min_recovery_ms` | 60000 | Minimum time protection stays released before the next snapshot is admitted. |
| `snapshot_lifecycle_recovery_alarm_ms` | 1800000 | How long cleanup or recovery may take before `SNAPSHOT_RECOVERY_BLOCKED` is raised. Must be positive. |

Checkpointer side, a JVM system property of the Corfu server and of the checkpointer JVMs:

| Property | Default | Meaning |
|---|---|---|
| `corfu.snapshot.lease.expiry.grace.ms` | 1800000 | How long past its deadline held protection is still honored. It must comfortably exceed the clock skew between cluster nodes. |

**Sizing the budget.** The budget must cover the largest snapshot the deployment carries, end to
end. Too small and large snapshots can never complete (each attempt is abandoned at the deadline and
`SNAPSHOT_SYNC_FAILING` is raised); too large and a stuck attempt delays checkpointing for longer.
Use the `logreplication.snapshot.lease.phase.duration` timer of successful syncs and leave headroom;
revisit it when the data set grows. The sink's log must be able to absorb budget + grace of writes
without a trim.

## Monitoring

The health monitor only exists in the Corfu server process. The log replication process, which
notices most of these conditions first, can only log them and expose gauges. The lease is durable
and shared, so the **compactor leader of the sink cluster** raises the health issues below from the
record, on every pass (ten seconds), under the `Compactor` component. They follow the compactor
leader when it moves.

| Issue | Raised when | What to do |
|---|---|---|
| `SNAPSHOT_SYNC_FAILING` | Three snapshot attempts in a row were abandoned. Cleared by the next completed snapshot. | The description carries the last failure. `Snapshot resource deadline expired` repeatedly means the budget is too small for the data set or the link. `No snapshot traffic accepted` means the source keeps going away: look at the source cluster. |
| `SNAPSHOT_RECOVERY_BLOCKED` | Cleanup or the recovery gate has taken longer than thirty minutes, the cleanup is `FAULTED`, or protection was left held past its deadline and the grace. | For a blocked recovery, fix the compactor: the sink's log names the tables of a failed cycle (`SNAPSHOT_RECOVERY_BLOCKED: ...`). Snapshot admission reopens by itself once a cycle completes and trims; incremental sync is not affected. If replication must proceed while compaction stays broken, disabling compaction skips the gate. For protection left held, the sink has no log replication leader or its leader is hung; checkpointing already ignores the record. |
| `CHECKPOINT_FROZEN` | Protection is still held five minutes past its deadline and the checkpointer still honors it, or the freeze token is older than an hour. | For the lease: the sink LR leader is hung or missing; the freeze ends when the grace does. For the token: somebody froze the checkpointer by hand or an old component wrote it; it expires two hours after it was first written. |
| `CHECKPOINT_STALLED` | A compaction cycle has tables waiting and no checkpoint has made progress for five minutes. | Look at the checkpointer processes. |

The sink LR leader logs the same conditions at `ERROR` (`SNAPSHOT_RECOVERY_BLOCKED: ...`,
`SNAPSHOT_SYNC_FAILING: ...`) with more detail, and exposes these metrics:
`logreplication.snapshot.lease.budget.remaining.ms`,
`logreplication.snapshot.lease.consecutive.aborts`, `logreplication.snapshot.sync.failing`,
`logreplication.snapshot.recovery.blocked`, and the timer
`logreplication.snapshot.lease.phase.duration` tagged with `phase`. On the source, the replication
status table carries `consecutiveFailures` for the snapshot sync in progress: status alone stays
`ONGOING` during a restart loop.

To look at the record itself, use the store browser of `corfudb-tools`
(`org.corfudb.browser.CorfuStoreBrowserEditorMain`) against a node of the sink cluster:

```
--host=<sink node> --port=<corfu port> --operation=showTable --namespace=CorfuSystem --tablename=SnapshotSyncLeaseTable
```

`phase`, `outcome`, `failure`, `deadlineMs`, `protectionHeld`, `recoveryCut` and `consecutiveAborts`
answer most questions. The sink's log says why an attempt was abandoned
(`Abandoning snapshot attempt generation=... : <reason>`).

## The snapshot sync plugin

`ISnapshotSyncPlugin` has two hooks that the sink calls around the protected window:
`acquireSnapshot` after an attempt is reserved and `releaseSnapshot` when its protection is about to
be released. Both default to doing nothing. They must be idempotent and bounded in time, and they
are told which attempt they belong to.

**A plugin must not freeze the checkpointer itself.** The lease does that, with bounds. The old
hooks `onSnapshotSyncStart` and `onSnapshotSyncEnd` are deprecated and never called; an
implementation that wrote the freeze token there no longer has any effect and should be deleted.

## The freeze token

Log replication no longer writes the freeze token, but the checkpointer still honors it for
operators and tools. One thing changed: **a repeated freeze no longer restarts the two hour
patience.** The patience is counted from when the freeze began, so a caller that keeps asking can no
longer keep checkpointing frozen for as long as it keeps asking. To freeze for longer on purpose,
unfreeze and freeze again.

## Role changes

Whoever holds the log replication lock of a cluster drives that cluster's lease, whatever the
cluster's role. When a sink becomes the source in the middle of a snapshot sync, the lock holder
abandons the attempt and releases the protection within seconds, also if the node that led at the
time of the switch is gone. A cluster that is not a sink admits nothing and does not ask for
checkpoints; its record simply returns to `READY`.

## Upgrading from the freeze token protocol

The lease is always on: there is no flag and no fallback protocol. The order is the usual one: **the
sink (standby) cluster first, then the source**.

While the sink cluster is rolling:

* The first upgraded node that becomes LR leader creates the lease record. If the last snapshot sync
  had completed, the record is created `READY` with that snapshot as its completed state:
  incremental sync continues and nothing is restarted. A freeze token found in this situation is not
  LR's and is left alone.
* If a snapshot sync of the old protocol was still unfinished (this includes a cluster that is in
  the middle of the incident described above), nothing in the new version can finish it. It is
  superseded: the leftover freeze token is deleted, which unfreezes the checkpointer at once, and the
  record is created `RECOVERING`, so the garbage of the old attempt is checkpointed and trimmed
  before a new snapshot is admitted. The source starts a fresh snapshot sync afterwards.
* A source that is not upgraded yet does not know the lease. An upgraded sink leader refuses it
  (`UNSUPPORTED_PROTOCOL`); the old source logs an unknown message, times out and retries its
  negotiation. **Replication is paused from the moment an upgraded node leads the sink until the
  source cluster is upgraded**, and the source accumulates the backlog. Plan the two upgrades close
  together. No data is lost: the source resumes from the sink's persisted positions, with incremental
  sync if its log still holds the backlog and with a snapshot sync otherwise.
* LR leadership can move back to a node that is not upgraded yet. That node ignores the record and
  speaks the old protocol with the old source, freeze token included. When an upgraded node leads
  again it recognizes a snapshot sync the old node left unfinished and supersedes it as above. This
  can only happen until the last sink node is upgraded.
* Compaction is not stopped during the upgrade. A checkpointer or compactor leader that is not
  upgraded yet does not know the lease either, but no snapshot sync is admitted by the lease until
  an upgraded sink leader exists, and an old source cannot get one admitted at all, so there is
  nothing for it to violate before the source cluster is upgraded, by which time the sink cluster
  is fully upgraded.

Once the source cluster is upgraded, negotiation finds the lease and replication resumes.

**Rollback.** Rolling the sink back while the source is already upgraded stops replication: an
upgraded source never sends anything to a sink that reports no lease, and retries its negotiation
once a second until the sink is upgraded again. Rolling both back returns to the old protocol; the
lease record stays in the store, is ignored by the old version and is picked up again, conservatively,
by the next upgrade.
