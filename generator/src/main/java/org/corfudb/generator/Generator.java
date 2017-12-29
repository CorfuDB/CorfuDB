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

import static java.util.concurrent.Executors.newWorkStealingPool;

/**
 * Generator is a program that generates synthetic workloads that try to mimic
 * real applications that consume the CorfuDb client. Application's access
 * patterns can be approximated by setting different configurations like
 * data distributions, concurrency level, operation types, time skews etc.
 *
 *
 * Created by maithem on 7/14/17.
 */
public class Generator {
    public static void main(String[] args) {
        String endPoint = args[0];
        int numStreams = Integer.parseInt(args[1]);
        int numKeys = Integer.parseInt(args[2]);
        long cpPeriod = Long.parseLong(args[3]);
        int numThreads = Integer.parseInt(args[4]);
        int numOperations = Integer.parseInt(args[5]);

        CorfuRuntime rt = new CorfuRuntime(endPoint).connect();

        State state = new State(numStreams, numKeys, rt);

        Runnable cpTrimTask = () -> {
            Operation op = new CheckpointOperation(state);
            op.execute();
        };

        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(cpTrimTask, 30, cpPeriod, TimeUnit.SECONDS);

        ExecutorService appWorkers = newWorkStealingPool(numThreads);

        Runnable app = () -> {
            List<Operation> operations = state.getOperations().sample(numOperations);
            for (Operation operation : operations) {
                try {
                    operation.execute();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
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
                throw new RuntimeException(e);
            }
        }
    }
}
