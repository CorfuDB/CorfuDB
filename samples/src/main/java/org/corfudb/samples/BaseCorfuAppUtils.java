package org.corfudb.samples;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.util.Map;

/**
 * This class provides several recurring utility methods for Corfu applications.
 *
 * Created by dalia on 12/31/16.
 */
public abstract class BaseCorfuAppUtils {

    @Setter @Getter
    CorfuRuntime corfuRuntime;

    /**
     * A Corfu application needs to instantiate a CorfuRuntime in order to connect to the Corfu service.
     * CorfuRuntime is a Java object that contains all of the Corfu utilities exposed to applications,
     * including library classes for streams, objects and transactions.
     *
     * the application needs to point the runtime to a host and port which is running the Corfu service.
     * See http://github.com/CorfuDB/CorfuDB for instructions on how to deploy Corfu.
     *
     * Internally, the corfuRuntime interacts with the CorfuDB service over TCP/IP sockets.
     *
     * @param configurationString specifies the IP:port of the CorfuService
     *                            The configuration string has format "hostname:port", for example, "localhost:9090".
     * @return a CorfuRuntime object, with which Corfu applications perform all Corfu operations
     */
    private CorfuRuntime getRuntimeAndConnect(String configurationString) {
        CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
        return corfuRuntime;
    }

    private static final String USAGE = "Usage: HelloCorfu [-c <conf>]\n"
            + "Options:\n"
            + " -c <conf>     Set the configuration host and port  [default: localhost:9999]\n";

    /**
     * must override this method to initiate any activity
     */
    abstract void action();

    /**
     * boilerplate activity generator, to be invoked from an application's main().
     *
     * @param args are the args passed to main
     */
    void start(String[] args) {
        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE)
                        .withVersion(GitRepositoryState.getRepositoryState().describe)
                        .parse(args);
        String corfuConfigurationString = (String) opts.get("-c");

        /**
         * Must set up a Corfu runtime before everything.
         */
        setCorfuRuntime( getRuntimeAndConnect(corfuConfigurationString) );

        /**
         * Obviously, this application is not doing much yet,
         * but you can already invoke getRuntimeAndConnect to test if you can connect to a deployed Corfu service.
         *
         * Next, invoke a class-specific activity wrapper named 'action()'.
         */
        action();
    }

    /**
     * Utility method to instantiate a Corfu object
     *
     * A Corfu Stream is a log dedicated specifically to the history of updates of one object.
     * This method will instantiate a stream by giving it a name,
     * and then instantiate an object by specifying its class
     *
     * @param tClass is the object class
     * @param name is the name of the stream backing up the object
     * @param <T> the return class
     * @return an object instance of type T backed by a stream named 'name'
     */
    protected <T> T instantiateCorfuObject(Class<T> tClass, String name) {
        return getCorfuRuntime().getObjectsView()
                .build()
                .setStreamName(name)     // stream name
                .setType(tClass)        // object class backed by this stream
                .open();                // instantiate the object!
    }

    /**
     * Utility method to instantiate a Corfu object
     *
     * A Corfu Stream is a log dedicated specifically to the history of updates of one object.
     * This method will instantiate a stream by giving it a name,
     * and then instantiate an object by specifying its class
     *
     * @param tType is a TypeToken wrapping the (possibly generic) object class
     * @param name is the name of the stream backing up the object
     * @param <T> the return class
     * @return an object instance of type T backed by a stream named 'name'
     */
    protected <T> Object instantiateCorfuObject(TypeToken<T> tType, String name) {
        return getCorfuRuntime().getObjectsView()
                .build()
                .setStreamName(name)     // stream name
                .setTypeToken(tType)    // a TypeToken of the specified class
                .open();                // instantiate the object!
    }


    /**
     * Utility method to start a (default type) TX
     * Can be overriden by classes that require non-default transaction type.
     */
    protected void TXBegin() {
        getCorfuRuntime().getObjectsView().TXBuild()
                .begin();
    }

    /**
     * Utility method to end a TX
     */
    protected void TXEnd() {
        getCorfuRuntime().getObjectsView().TXEnd();
    }
}
