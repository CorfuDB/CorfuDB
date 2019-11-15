package org.corfudb.samples;

import java.util.Map;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

/**
 * This tutorial demonstrates a simple Corfu application.
 *
 * Created by dalia on 12/30/16.
 */
public class HelloCorfu {
    private static final String USAGE = "Usage: HelloCorfu [-c <conf>]\n"
            + "Options:\n"
            + " -c <conf>     Set the configuration host and port  [default: localhost:9999]\n";

    /**
     * Internally, the corfuRuntime interacts with the CorfuDB service over TCP/IP sockets.
     *
     * @param configurationString specifies the IP:port of the CorfuService
     *                            The configuration string has format "hostname:port", for example, "localhost:9090".
     * @return a CorfuRuntime object, with which Corfu applications perform all Corfu operations
     */
    private static CorfuRuntime getRuntimeAndConnect(String configurationString) {

        CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
        return corfuRuntime;
    }

    @SuppressWarnings("checkstyle:printLine") // Sample code
    public static void main(String[] args) {
        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE)
                        .withVersion(GitRepositoryState.getRepositoryState().describe)
                        .parse(args);
        String corfuConfigurationString = (String) opts.get("-c");

        /**
         * First, the application needs to instantiate a CorfuRuntime,
         * which is a Java object that contains all of the Corfu utilities exposed to applications.
         */
        CorfuRuntime runtime = getRuntimeAndConnect(corfuConfigurationString);

        /**
         * Obviously, this application is not doing much yet,
         * but you can already invoke getRuntimeAndConnect to test if you can connect to a deployed Corfu service.
         *
         * Above, you will need to point it to a host and port which is running the service.
         * See {@link https://github.com/CorfuDB/CorfuDB} for instructions on how to deploy Corfu.
         */

        /**
         * Next, we will illustrate how to declare a Java object backed by a Corfu Stream.
         * A Corfu Stream is a log dedicated specifically to the history of updates of one object.
         * We will instantiate a stream by giving it a name "A",
         * and then instantiate an object by specifying its class
         */
        Map<String, Integer> map = runtime.getObjectsView()
                .build()
                .setStreamName("A")     // stream name
                .setTypeToken(new TypeToken<SMRMap<String, Integer>>() {})
                .open();                // instantiate the object!

        /**
         * The magic has already happened! map is an in-memory view of a shared map, backed by the Corfu log.
         * The application can perform put and get on this map from different application instances,
         * crash and restart applications, and so on.
         * The map will persist and be consistent across all applications.
         *
         * For example, try the following code repeatedly in a sequence, in between run/exit,
         * from multiple instances, and see the different interleaving of values that result.
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
