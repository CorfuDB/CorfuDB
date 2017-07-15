package org.corfudb.generator;

import java.util.Random;

/**
 * Created by maithem on 7/14/17.
 */
public class SleepOperation extends Operation {

    public SleepOperation(State state) {
        super(state);
    }

    @Override
    public void execute() {
        Random rand = new Random();

        int  sleepTime = rand.nextInt(2000) + 1;
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
