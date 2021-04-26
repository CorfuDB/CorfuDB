package org.corfudb.infrastructure.compaction;


import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.corfudb.runtime.collections.CorfuDynamicKey;


@Data
@Builder(toBuilder = true)
public class CheckpointOptions {

    @Builder.Default
    private final boolean isDiskBacked = false;

    @Builder.Default
    private final boolean isSkipped = false;
}
