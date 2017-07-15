package org.corfudb.generator;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.corfudb.generator.operations.CheckpointOperation;
import org.corfudb.generator.operations.Operation;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import static java.util.concurrent.Executors.newWorkStealingPool;

/**
 * Created by maithem on 7/14/17.
 */
public class main {
    public static void main(String[] args) {
        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();
        State state = new State(100, 20000, rt);

        Runnable cpTrimTask = () -> {
            Operation op = new CheckpointOperation(state);
            op.execute();
        };

        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(cpTrimTask, 1, 1, TimeUnit.MINUTES);

        int numThreads = 8;
        ExecutorService appWorkers = newWorkStealingPool(numThreads);

        Runnable app = () -> {
            List<Operation> operations = state.getOperations().sample(3000000);
            for (Operation operation : operations) {
                try {
                    operation.execute();
                } catch (TransactionAbortedException e) {
                    System.out.println(e.getCause());
                }
            }
        };

        Future[] appsFutures = new Future[numThreads];

        for (int x = 0; x < numThreads; x++) {
            appsFutures[x] = appWorkers.submit(app);
        }

        for (int x = 0; x < numThreads; x++) {
            try {
                appsFutures[x].get();
            } catch (Exception e) {
                System.out.println("App Exception: " + e);
            }
        }
    }
}
