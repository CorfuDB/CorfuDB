package org.corfudb.infrastructure.log.statetransfer.batchprocessor;


import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;

import java.util.List;

/**
 * An exception that is propagated to the caller after all the retries.
 */
@Getter
public class StateTransferFailure extends StateTransferException {

    private final String endpoint;

    private final List<Long> addresses;

    public StateTransferFailure(String endpoint, List<Long> addresses) {
        super();
        this.endpoint = endpoint;
        this.addresses = addresses;
    }

    public StateTransferFailure(String msg, String endpoint, List<Long> addresses) {
        super(msg);
        this.endpoint = endpoint;
        this.addresses = addresses;
    }

    public StateTransferFailure(Throwable throwable, String endpoint, List<Long> addresses){
        super(throwable);
        this.endpoint = endpoint;
        this.addresses = addresses;
    }




}
