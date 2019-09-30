package org.corfudb.infrastructure.log.statetransfer.exceptions;


/**
 * An exception that is propagated to the caller after all the retries.
 */
public class StateTransferFailure extends StateTransferException {
    public StateTransferFailure() {
        super();
    }

    public StateTransferFailure(String msg) {
        super(msg);
    }


}
