package org.corfudb.infrastructure.compaction;

import com.google.protobuf.Timestamp;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.corfudb.infrastructure.compaction.proto.LogCompaction.SafeSnapshotMsg;

import java.time.Instant;

@Data
@Builder
public final class SafeSnapshot {
    private final boolean committed;
    private final long address;
    private final Instant recordTime;

    public static SafeSnapshot fromMessage(SafeSnapshotMsg safeSnapshotMsg) {
        return SafeSnapshot.builder()
                .committed(safeSnapshotMsg.getCommitted())
                .address(safeSnapshotMsg.getAddress())
                .recordTime(Instant.ofEpochSecond(safeSnapshotMsg.getRecordTime().getSeconds(),
                        safeSnapshotMsg.getRecordTime().getNanos()))
                .build();
    }

    public SafeSnapshotMsg convertToMessage() {
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(this.recordTime.getEpochSecond())
                .setNanos(this.recordTime.getNano())
                .build();

        return SafeSnapshotMsg.newBuilder()
                .setCommitted(this.committed)
                .setAddress(this.address)
                .setRecordTime(timestamp)
                .build();
    }
}
