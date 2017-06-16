package org.corfudb.runtime.view.metadata;

import java.util.List;
import java.util.UUID;

/**
 * This class defines metadata associated with a checkpoint
 */
public class Checkpoint {
    private UUID checkpointStream;
    private long startAddress;
    private long endAddress;
    List<Long> checkpointRecords;
}
