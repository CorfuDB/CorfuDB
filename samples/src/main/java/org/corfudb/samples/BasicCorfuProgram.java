package org.corfudb.samples;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.util.Map;

/**
 * Created by dalia on 12/31/16.
 */
public abstract class BasicCorfuProgram {
    private static final String USAGE = "Usage: HelloCorfu [-c <conf>]\n"
            + "Options:\n"
            + " -c <conf>     Set the configuration host and port  [default: localhost:9999]\n";

    @Setter @Getter
    CorfuRuntime corfuRuntime;

    /**
     * must override this method to initiate any activity
     */
    abstract void action();

    /**
     * @param configurationString specifies the IP:port of the CorfuService
     * @return a CorfuRuntime object, with which Corfu applications perform all Corfu operations
     */
    private CorfuRuntime getRuntimeAndConnect(String configurationString) {
        CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
        return corfuRuntime;
    }

    /**
     * boilerplate activity generator
     */
    void start(String[] args) {
        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE)
                        .withVersion(GitRepositoryState.getRepositoryState().describe)
                        .parse(args);
        String corfuConfigurationString = (String) opts.get("-c");


        /**
         * First, the application needs to instantiate a CorfuRuntime
         */
        setCorfuRuntime( getRuntimeAndConnect(corfuConfigurationString) );
        action();
    }

    /**
     * Utility method to start a (default type) TX
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

    /**
     * Utility method to instantiate a Corfu object
     */
    protected <T> T instantiateCorfuObject(Class<T> tClass, String name) {
        return getCorfuRuntime().getObjectsView()
                .build()
                .setStreamName(name)     // stream name
                .setType(tClass)        // object class backed by this stream
                .open();                // instantiate the object!
    }
}
