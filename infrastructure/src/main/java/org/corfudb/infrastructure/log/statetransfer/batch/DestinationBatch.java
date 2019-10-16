package org.corfudb.infrastructure.log.statetransfer.batch;

import lombok.Getter;

import java.util.List;

@Getter
public class DestinationBatch extends Batch {
    private final String destination;

    public DestinationBatch(List<Long> addresses, long latency, String destination) {
        super(addresses, latency);
        this.destination = destination;
    }

}
