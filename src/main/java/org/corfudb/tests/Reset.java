package org.corfudb.tests;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.abstractions.SharedLog;

import org.corfudb.client.OutOfSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.corfudb.client.configmasters.IConfigMaster;

/**
 * Created by dmalkhi on 1/16/15.
 */
public class Reset {

    private static final Logger log = LoggerFactory.getLogger(Reset.class);

    /**

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        String masteraddress = null;

        if (args.length >= 1) {
            masteraddress = args[0]; // TODO check arg.length
        } else {
            // throw new Exception("must provide master http address"); // TODO
            masteraddress = "http://localhost:8002/corfu";
        }

        final int numthreads = 1;

        try (CorfuDBClient client = new CorfuDBClient(masteraddress))
        {
            client.startViewManager();
            client.waitForViewReady();
            IConfigMaster cm = (IConfigMaster) client.getView().getConfigMasters().get(0);
            cm.resetAll();
        }
        log.info("Successfully reset the configuration!");
        System.exit(0);

    }
}

