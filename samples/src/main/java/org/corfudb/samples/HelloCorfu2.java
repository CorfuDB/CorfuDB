package org.corfudb.samples;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.FGMap;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.VersionLockedObject;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This tutorial demonstrates a simple Corfu application.
 *
 * Created by dalia on 12/30/16.
 */
public class HelloCorfu2 {
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

    public static void main(String[] args) {
        // Parse the options given, using docopt.

        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.TRACE);
        Map<String, Object> opts =
                new Docopt(USAGE)
                        .withVersion(GitRepositoryState.getRepositoryState().describe)
                        .parse(args);
        String corfuConfigurationString = (String) opts.get("-c");

        CorfuRuntime runtime = getRuntimeAndConnect(corfuConfigurationString);

        Node myNode2 = runtime.getObjectsView()
                .build()
                .setStreamName("C")     // stream name
                .setType(Node.class)  // object class backed by this stream
                .open();                // instantiate the object!
        myNode2.setName("joshina");
        //myNode2.setAge(0);
        myNode2.setAge(myNode2.getAge()+1);
        System.out.println(myNode2.getName() + " is " + myNode2.getAge() + " years old now!");
    }

    @CorfuObject
    public static class Node {
        private String name;
        private int age;

        public Node() {
            name = "";
            age = 0;
        }

        @Accessor
        public String getName() {
            return name;
        }

        @Accessor
        public int getAge() {
            return age;
        }

        @MutatorAccessor(name = "setName")
        public void setName(String n) {
            name = n;
        }

        @MutatorAccessor(name = "setAge")
        public void setAge(int a) {
            age = a;
        }
    }
}

