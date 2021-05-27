package org.corfudb.infrastructure.compaction;


import lombok.Builder;
import lombok.Data;


@Data
@Builder(toBuilder = true)
public class CheckpointOptions {

    @Builder.Default
    private final boolean isDiskBacked = false;

    @Builder.Default
    private final boolean isSkipped = false;
}
