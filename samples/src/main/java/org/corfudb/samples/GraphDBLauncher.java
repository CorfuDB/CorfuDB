package org.corfudb.samples;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class GraphDBLauncher {
    private static final String USAGE = "Usage: GraphDBLauncher [-c <conf>]\n"
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

    public static void main(String[] args) {
        // Enabling logging
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.TRACE);

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
        System.out.println(runtime);

        GraphDB d = runtime.getObjectsView()
                .build()
                .setStreamName("A")     // stream name
                .setType(GraphDB.class) // object class backed by this stream
                .open();                // instantiate the object!

        d.addNode("Alice" + d.getNumNodes());
        d.addNode("Bob" + d.getNumNodes());
        d.addNode("Charlie" + d.getNumNodes());
        d.addNode("Dana" + d.getNumNodes());

        d.addEdge("Alice", "Bob");
        d.addEdge("Alice", "Charlie");
        d.addEdge("Charlie", "Dana");

        for (String friend : d.adjacent("Alice")) {
            System.out.println(friend);
        }
    }
}
