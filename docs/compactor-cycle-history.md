# Compactor Cycle History Tracking

## Overview

This document describes the **Compaction Cycle History** feature - a lightweight tracking mechanism that records essential information about completed compactor cycles in CorfuDB.

## Background

The Compactor is designed as a distributed service where there is one leader at any point in time which triggers compaction cycles (typically every 15 minutes) by writing to compactor system tables. Corfu checkpointers running on all nodes monitor these tables and start checkpointing when a cycle is triggered. The bookkeeping is managed through Corfu tables to track which node checkpoints which table.

Previously, the `CompactionManagerTable` only tracked the **current** cycle status, which was overwritten with each new cycle. The `CheckpointStatusTable` was cleared at the start of each cycle. This meant historical information about past cycles was lost.

## Solution

A new **CompactionCycleHistoryTable** has been introduced to persistently track completed compaction cycles with **essential information only**:
- **Cycle Count**: Unique identifier for the cycle
- **Status**: COMPLETED or FAILED
- **Start Time**: When the cycle began (milliseconds)
- **End Time**: When the cycle completed (milliseconds)

**Design Philosophy**: Keep it lightweight and fast. Detailed per-table information can be found in logs if needed for debugging.

## Implementation Details

### 1. Protobuf Messages

Two simple messages have been added to `runtime/proto/corfu_compactor_management.proto`:

#### CompactionCycleKey
```protobuf
message CompactionCycleKey {
  int64 cycle_count = 1;
}
```
Used as the key for the history table, uniquely identifying each cycle.

#### CompactionCycleHistory
```protobuf
message CompactionCycleHistory {
  int64 cycle_count = 1;
  CheckpointingStatus.StatusType status = 2;  // COMPLETED or FAILED
  int64 start_time_ms = 3;
  int64 end_time_ms = 4;
}
```
Lightweight history record for each completed cycle - only the essentials.

### 2. CompactorMetadataTables Updates

A new table field has been added:
```java
private Table<CompactionCycleKey, CompactionCycleHistory, Message> compactionCycleHistoryTable;
public static final String COMPACTION_CYCLE_HISTORY_TABLE_NAME = "CompactionCycleHistoryTable";
```

The table is opened in the constructor alongside other compactor metadata tables.

### 3. CompactorLeaderServices Updates

#### Recording Cycle History

The `finishCompactionCycle()` method has been enhanced to:
1. Determine cycle status (COMPLETED or FAILED) from table statuses
2. Build a lightweight `CompactionCycleHistory` record with only:
   - Cycle count
   - Status
   - Start and end timestamps
3. Persist the history record atomically with the manager status update

#### Internal Implementation

The history recording is fully automatic with no public query APIs currently exposed. History is:
- Recorded automatically when each cycle completes
- Pruned automatically to keep the last 100 cycles
- Stored in the `CompactionCycleHistoryTable` for future access if needed

## Data Model

### Table Structure

**Table Name:** `CompactionCycleHistoryTable`
- **Namespace:** `CORFU_SYSTEM_NAMESPACE`
- **Key Type:** `CompactionCycleKey` (cycle_count)
- **Value Type:** `CompactionCycleHistory`

### Key Properties

- **Unique Key:** Each cycle is uniquely identified by its `cycle_count`
- **Persistence:** History records are persisted across restarts
- **Growth:** The table grows with each completed cycle; use `pruneOldCycleHistories()` to manage size
- **Consistency:** History is recorded within the same transaction as the manager status update

## Integration Points

### 1. Cycle Initialization
In `initCompactionCycle()`:
- Captures `cycleStartTime` when cycle begins
- Stores `cycleMinCheckpointToken` for history record

### 2. Cycle Completion
In `finishCompactionCycle()`:
- Collects all table checkpoint statuses
- Builds comprehensive history record
- Persists history atomically with manager status update

### 3. Leader Services
The `CompactorLeaderServices` class provides all query and management APIs for accessing cycle history.

## Automatic Retention Management

The history table includes **automatic retention logic** to prevent unbounded growth:

### Fixed Retention Policy

- **Automatic Pruning**: After each cycle completes, old history records are automatically pruned
- **Retention Count**: 100 most recent cycles (covers ~25 hours at 15-min intervals)
- **Non-Blocking**: Pruning errors are logged but don't affect cycle completion
- **Lightweight**: With only 4 fields per record, 100 cycles use minimal storage

### How It Works

1. When a cycle completes in `finishCompactionCycle()`, automatic pruning is triggered
2. The pruning logic keeps the most recent 100 cycles
3. Older cycles beyond 100 are deleted
4. Pruning is best-effort: failures are logged but don't block cycle completion

## Monitoring and Operations

### Automatic Operation

The cycle history feature operates completely automatically:

1. **Automatic Recording:** History is recorded after each cycle completes
   - No manual intervention required
   - Lightweight: only 4 fields per record (cycle_count, status, start_time, end_time)

2. **Automatic Retention:** History is automatically pruned to keep the last 100 cycles
   - No configuration needed - works out of the box
   - Prevents unbounded growth

3. **Failure Analysis:** Check logs for detailed cycle and per-table information
   - History table provides cycle-level status
   - Detailed diagnostics available in server logs

## Retention Policy Details

### Why Automatic Retention?

Without automatic retention, the history table would grow indefinitely, consuming storage and impacting performance. Key considerations:

1. **Unbounded Growth**: At 15-min intervals, 4 cycles/hour × 24 hours = 96 cycles/day = ~3000 cycles/month
2. **Storage Impact**: Each cycle record contains per-table details, potentially 100s of KB per cycle
3. **Query Performance**: Large tables slow down range scans and queries
4. **Operational Simplicity**: Automatic retention requires no manual intervention

### Fixed Retention: 100 Cycles

The retention of 100 cycles provides:
- **Time Coverage**: ~25 hours at 15-min intervals (more than 1 day)
- **Debugging Window**: Sufficient history for troubleshooting recent issues
- **Storage Efficiency**: With only 4 fields per record, storage impact is minimal (~3-4KB for 100 records)
- **Performance**: Fast reads and writes with small record size

## Summary

The Compaction Cycle History feature provides **lightweight, automatic tracking** of compactor operations:

**What It Does:**
- Records essential cycle information: cycle_count, status, start_time, end_time
- Automatically prunes to keep the last 100 cycles
- Stores history for future programmatic access if needed

**Design Benefits:**
- **Lightweight**: Minimal storage and performance impact (4 fields per record)
- **Automatic**: No configuration or manual intervention needed
- **Bounded**: Fixed 100-cycle retention prevents unbounded growth
- **Fast**: Small records enable quick writes with minimal overhead
- **Simple**: Essential fields only; detailed diagnostics remain in logs

The implementation is fully automatic, non-invasive, and backward-compatible.

