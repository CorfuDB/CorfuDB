package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Empty operation that delays execution for a timeout
 */
@Slf4j
public class SleepOperation extends Operation {

    private static final Random RANDOM = new Random();

    public SleepOperation(State state) {
        super(state, Operation.Type.SLEEP);
    }

    @Override
    public void execute() {

        int sleepTime = RANDOM.nextInt(50);
        try {
            TimeUnit.MILLISECONDS.sleep(sleepTime);
        } catch (InterruptedException e) {
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    @Override
    public Context getContext() {
        throw new UnsupportedOperationException("Sleep operation doesn't have context");
    }
}
