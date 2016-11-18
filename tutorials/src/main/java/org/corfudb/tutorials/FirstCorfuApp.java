package org.corfudb.tutorials;

/**
 * Created by dmalkhi on 11/2/16.
 *
 * This tutorial demonstrates a simple Corfu application.
 */

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;

import java.util.Map;

public class FirstCorfuApp {

    /**
     * First, the application needs to instantiate a CorfuRuntime, which is a Java object that contains all of the Corfu utilities exposed to applications.
     * Internally, the runtime interacts with the CorfuDB service over TCP/IP sockets.
     *
     * @param configurationString The configuration string has format "hostname:port", for example, "localhost:9090".
     * @return
     */
    private static CorfuRuntime getRuntimeAndConnect(String configurationString) {
        CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
        return corfuRuntime;
    }

    public static void main(String[] args) {
        /* invoke getRuntimeAndConnect to test if you can connect to a deployed Corfu service.
         * You will need to point it to a host and port which is running the service.
         * (See CorfuDB main page for instructions on how to deploy Corfu.)
         */
        CorfuRuntime runtime = getRuntimeAndConnect("localhost:8888");

        /* Next, we illustrate how to declare a Java object backed by a Corfu Stream. A Corfu Stream is a log dedicated specifically to the history of updates of one object.
         * We instantiate a stream by giving it a name, and then instantiate an object by specifying its class
         */
        Map<String,Integer> map = runtime.getObjectsView()
                .build()
                .setStreamName("A")
                .setType(SMRMap.class)
                .open();
        /*
         * The magic has aleady happened!
         * map is an in-memory view of a shared map, backed by the Corfu log.
         * The application can perform put and get on this map from different application instances, crash and restart applications, and so on.
         * The map will persist and be consistent across all applications.
         *
         * For example, try invoking FirstCorfuthe repeatedly in a sequence, in between run/exit, from multiple instances,
         * and see the different interleaving of values that result by the following simple commands:
         */
        Integer previous = map.get("a");
        if (previous == null) {
            System.out.println("This is the first time we were run!");
            map.put("a", 1);
        }
        else {
            map.put("a", ++previous);
            System.out.println("This is the " + previous + " time we were run!");
        }
    }

}
