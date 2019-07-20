package org.corfudb.runtime.view;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fast Globally Unique Identity (GUID) generator that returns compact ids
 * having a notion of a universal comparable ordering.
 *
 * Created by Sundar Sridharan on 5/22/19.
 */
public class SnowflakeGuidGenerator implements OrderedGuidGenerator {
    private static final short NODE_MAX_VAL = 1023;
    private static final int NODE_SHIFT_LONG = 10;
    private static final int NODE_SHIFT_UUID = 53;
    private static final short SEQ_MAX_VAL = 4095;
    private static final int SEQ_SHIFT = 12;

    private final short node;
    private final AtomicInteger sequence;

    /**
     * Please pass something unique to this node.
     * For example:
     *      UUID nodeId = UUID.fromString("mac_address_of_machine");
     *      SnowflakeGuidGenerator guidGenerator = new SnowflakeGuidGenerator(nodeId.node());
     * @param node - Simple integer representing the current node.
     */
    public SnowflakeGuidGenerator(long node) {
        this.node = (short)(node & NODE_MAX_VAL);
        sequence = new AtomicInteger(0);
    }

    /**
     * Fast best effort generation scheme for compact snowflake generation.
     *    ______________________________________________
     *   | timestamp lower 42 bits | Node Id | Sequence |
     *    ------------------------- --------- ----------
     *    <------ 42 bits ---------><-10bits-><-12bits-->
     * @return The next 64-bit integer.
     */
    @Override
    public long nextLong() {
        long currentTime = System.currentTimeMillis();
        long counter = this.sequence.incrementAndGet() & SEQ_MAX_VAL;
        return currentTime << NODE_SHIFT_LONG << SEQ_SHIFT
                | node << SEQ_SHIFT
                | counter;
    }

    /**
     * Fast best effort generation scheme for snowflake generation.
     *    ____________________________________________________
     *   | timestamp 8 bytes      | Node Id  | Sequence       |
     *    ------------------------- --------------------------
     *    <------ 64 bits --------><-10bits--><----54 bits---->
     * @return a higher resolution unique identifier.
     */
    @Override
    public UUID nextUUID() {
        long currentTime = System.currentTimeMillis();
        long counter = this.sequence.incrementAndGet();
        counter |= node << NODE_SHIFT_UUID;
        return new UUID(currentTime, counter);
    }
}
