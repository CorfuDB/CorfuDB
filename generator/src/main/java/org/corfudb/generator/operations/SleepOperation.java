package org.corfudb.generator.operations;

import java.util.Random;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.State;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class SleepOperation extends Operation {

    public SleepOperation(State state) {
        super(state);
    }

    @Override
    public void execute() {
        Random rand = new Random();

        int  sleepTime = rand.nextInt(50);
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
