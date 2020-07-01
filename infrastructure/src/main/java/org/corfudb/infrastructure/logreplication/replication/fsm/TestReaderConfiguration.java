package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.Builder;
import lombok.Data;
import lombok.Setter;

/**
 * This is a test class which contains configuration parameters shared from the test with
 * Readers and Listeners for testing purposes.
 */
@Builder
@Data
public class TestReaderConfiguration {

    private int numEntries;

    private String payloadFormat;

    private String endpoint;

    private String streamName;

    // Batch Size for Reader
    @Setter
    private int batchSize;
}
