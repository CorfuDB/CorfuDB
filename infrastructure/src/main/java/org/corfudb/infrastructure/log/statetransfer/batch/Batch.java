package org.corfudb.infrastructure.log.statetransfer.batch;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class Batch {
    private final List<Long> addresses;
    private final long scheduledLatency;
}
