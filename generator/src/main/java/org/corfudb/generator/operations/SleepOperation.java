package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.State;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class SleepOperation extends Operation {
    private static final Random RANDOM = new Random();

    public SleepOperation(State state) {
        super(state, "Sleep");
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
}
