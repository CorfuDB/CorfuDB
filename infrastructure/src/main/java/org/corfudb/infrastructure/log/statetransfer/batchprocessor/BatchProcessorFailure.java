package org.corfudb.infrastructure.log.statetransfer.batchprocessor;


import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;

import javax.annotation.Nullable;
import java.util.List;

/**
 * An exception that is propagated to the caller after all the retries.
 */
@Getter
public class BatchProcessorFailure extends StateTransferException {

    private final String endpoint;

    private final List<Long> addresses;


    public BatchProcessorFailure(@Nullable String endpoint, @Nullable List<Long> addresses) {
        super();
        this.endpoint = endpoint;
        this.addresses = addresses;
    }

    public BatchProcessorFailure(String msg, @Nullable String endpoint, @Nullable List<Long> addresses) {
        super(msg);
        this.endpoint = endpoint;
        this.addresses = addresses;
    }

    public BatchProcessorFailure(Throwable throwable, @Nullable String endpoint, @Nullable List<Long> addresses){
        super(throwable);
        this.endpoint = endpoint;
        this.addresses = addresses;
    }




}
