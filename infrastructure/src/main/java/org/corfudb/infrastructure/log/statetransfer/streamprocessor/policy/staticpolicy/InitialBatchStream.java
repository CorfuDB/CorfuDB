package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;

import java.util.stream.Stream;

@AllArgsConstructor
@Getter
public class InitialBatchStream {
    private final Stream<Batch> initialStream;
    private final long initialBatchStreamSize;
}
