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

        }

        return true;
    }
}
