package org.corfudb.generator.replayer;

import org.corfudb.runtime.CorfuRuntime;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A sample app for replaying a collection of recorded events
 * Created by Sam Behnam on 1/18/18.
 */
@SuppressWarnings({"checkstyle:printLine"})
public class ReplayerApp {
    public static void main(String[] args) throws InterruptedException {
        final List<String> pathsToEventQueues =
                Arrays.asList("./test/src/test/resources/replayerWorkloads/corfuMapTestWorkload.list");
        final String endPoint = "localhost:9000";
        final CorfuRuntime runtime = new CorfuRuntime(endPoint).connect();

        System.out.println(String.format("Starting to replay on runtime: %s", runtime));
        long executionTime = Replayer.replayEventList(pathsToEventQueues, runtime);
        System.out.println("Replayed in:" + TimeUnit.SECONDS.convert(executionTime, TimeUnit.NANOSECONDS));
    }
}
