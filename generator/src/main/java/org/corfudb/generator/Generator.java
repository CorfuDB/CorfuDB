package org.corfudb.generator;

import org.corfudb.generator.correctness.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Operations;
import org.corfudb.generator.distributions.Streams;
import org.corfudb.generator.operations.CheckpointOperation;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.state.CorfuTablesGenerator;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.CorfuRuntime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newWorkStealingPool;

/**
 * Generator is a program that generates synthetic workloads that try to mimic
 * real applications that consume the CorfuDb client. Application's access
 * patterns can be approximated by setting different configurations like
 * data distributions, concurrency level, operation types, time skews etc.
 * <p>
 * <p>
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

        Streams streams = new Streams(numStreams);
        Keys keys = new Keys(numKeys);
        streams.populate();
        keys.populate();

        CorfuTablesGenerator tablesManager = new CorfuTablesGenerator(rt, streams);
        tablesManager.openObjects();

        State state = new State(streams, keys);
        Operations operations = new Operations(state, tablesManager, new Correctness());
        operations.populate();

        Runnable cpTrimTask = () -> {
            Operation op = new CheckpointOperation(state, tablesManager);
            op.execute();
        };

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(cpTrimTask, 30, cpPeriod, TimeUnit.SECONDS);

        ExecutorService appWorkers = newWorkStealingPool(numThreads);

        Runnable app = () -> {
            List<Operation.Type> ops = operations.sample(numOperations);
            for (Operation.Type opType : ops) {
                try {
                    Operation operation = operations.create(opType);
                    operation.execute();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        };

        List<Future<?>> appsFutures = new ArrayList<>(numThreads);

        for (int i = 0; i < numThreads; i++) {
            appsFutures.add(appWorkers.submit(app));
        }

        for (Future<?> future : appsFutures) {
            try {
                future.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
