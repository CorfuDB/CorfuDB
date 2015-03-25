package org.corfudb.tests;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.abstractions.SharedLog;

import org.corfudb.client.abstractions.Stream;
import org.corfudb.client.OutOfSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import org.corfudb.client.entries.CorfuDBStreamEntry;
import org.corfudb.client.Timestamp;

/**
 * Created by dmalkhi on 1/16/15.
 */
public class StreamInspector {

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
        client.startViewManager();

        try (Stream s = new Stream(client, UUID.fromString(args[1])))
        {
            while (true)
            {
                CorfuDBStreamEntry cdse = s.readNextEntry();
                Timestamp t = cdse.getTimestamp();
                log.info("{} \t {} \t {}", t.pos, t.physicalPos, cdse.payload.getClass().toString());
            }
        }

    }
}

