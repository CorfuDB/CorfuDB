package org.corfudb.tests;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.abstractions.SharedLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by dmalkhi on 1/16/15.
 */
public class CorfuHello {

    private static final Logger log = LoggerFactory.getLogger(CorfuHello.class);

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

        CorfuDBClient client = new CorfuDBClient(masteraddress);
        Sequencer s = new Sequencer(client);
        WriteOnceAddressSpace woas = new WriteOnceAddressSpace(client);
        SharedLog sl = new SharedLog(s, woas);
        client.startViewManager();

        log.info("Appending hello world into log...");
        long address = sl.append("hello world".getBytes());
        log.info("Successfully appended hello world into log position " + address);
        log.info("Reading back entry at address " + address);
        byte[] result = sl.read(address);
        log.info("Readback complete, result size=" + result.length);
        String sresult = new String(result, "UTF-8");
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("hello world"))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }

        log.info("Successfully completed test!");
        System.exit(0);

    }
}

