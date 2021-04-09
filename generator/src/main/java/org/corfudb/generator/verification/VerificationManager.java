package org.corfudb.generator.verification;

import lombok.Builder;
import lombok.NonNull;
import org.corfudb.generator.operations.Operation;

import java.util.concurrent.BlockingQueue;

@Builder
public class VerificationManager {
    @NonNull
    private final BlockingQueue<Operation> completedOperations;

    public boolean verify() {
        while (!completedOperations.isEmpty()) {
            Operation op = completedOperations.poll();

            switch (op.getOpType()) {
                case READ:
                    new ReadOperationVerification(op.getState(), op.getContext()).verify();
                    break;
                case REMOVE:
                    break;
                case SLEEP:
                    break;
                case WRITE:
                    break;
                case TX_NESTED:
                    break;
                case TX_OPTIMISTIC:
                    break;
                case TX_SNAPSHOT:
                    break;
                case TX_WAW:
                    break;
            }
        }

        return true;
    }
}
