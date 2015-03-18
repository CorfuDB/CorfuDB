package org.corfudb.tests;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.abstractions.Stream;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.abstractions.SharedLog;
import org.corfudb.client.configmasters.IConfigMaster;
import org.corfudb.client.Timestamp;
import org.corfudb.client.OutOfSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;

import java.util.UUID;
/**
 * Created by dmalkhi on 1/16/15.
 */
public class CorfuHopStreamTemporaryMultiHello {

    private static final Logger log = LoggerFactory.getLogger(CorfuHopStreamTemporaryMultiHello.class);

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

        UUID streamID = UUID.randomUUID();
        UUID streamID2 = UUID.randomUUID();
        UUID streamID3 = UUID.randomUUID();

        Timestamp address = null;
        Timestamp address2 = null;


        log.info("Stream 1={}", streamID);
        log.info("Stream 2={}", streamID2);
        log.info("Stream 3={}", streamID3);

        try (Stream s = new Stream(client, streamID))
        {
            try (Stream s2 = new Stream(client, streamID2))
            {
                try (Stream s3 = new Stream(client, streamID3))
                {
                    log.info("Appending hello world into all logs...");
                    try {
                        address = s.append("hello world from stream " + streamID.toString());
                        log.info("Successfully appended hello world into log position " + address + ", stream "+ streamID.toString());
                        address = s2.append("hello world from stream " + streamID2.toString());
                        log.info("Successfully appended hello world into log position " + address + ", stream "+ streamID2.toString());
                        address2 = s3.append("hello world from stream " + streamID3.toString());
                        log.info("Successfully appended hello world into log position " + address + ", stream "+ streamID3.toString());
                    }
                    catch (OutOfSpaceException oose)
                    {
                        log.error("Out of space during append!", oose);
                        System.exit(1);
                    }

               log.info("Reading back initial entries");
               String sresult = (String) s.readNextObject();
               log.info("Contents were: " + sresult);
               if (!sresult.toString().equals("hello world from stream " + streamID.toString()))
                        {
                            log.error("ASSERT Failed: String did not match!");
                            System.exit(1);
                        }
               sresult = (String) s2.readNextObject();
               log.info("Contents were: " + sresult);
               if (!sresult.toString().equals("hello world from stream " + streamID2.toString()))
                        {
                            log.error("ASSERT Failed: String did not match!");
                            System.exit(1);
                        }
               sresult = (String) s3.readNextObject();
               log.info("Contents were: " + sresult);
               if (!sresult.toString().equals("hello world from stream " + streamID3.toString()))
                        {
                            log.error("ASSERT Failed: String did not match!");
                            System.exit(1);
                        }

                log.info("Pulling stream  " + streamID3.toString() + ", " +  streamID2.toString() + " to stream " + streamID.toString() + " for 3 entries");
                List<UUID> streams = new ArrayList<UUID>();
                streams.add(streamID2);
                streams.add(streamID3);
                s.pullStream(streams, 3);
/*
                log.info("Waiting for pull to complete on all streams...");
                while (true)
                {
                    try {
                    s2.waitForEpochChange(address);
                    s3.waitForEpochChange(address2);
                    break;
                    } catch (InterruptedException ie) {}
                }
                log.info("Pull complete on all streams");
*/
        try {
            log.info("Appending to outer stream " + streamID.toString());
            address = s.append("hello world remote from outer stream " + streamID.toString());
            log.info("Successfully appended hello world into log position " + address + ", stream "+ streamID.toString());
            log.info("Appending to inner stream " + streamID2.toString());
            address = s2.append("hello world remote from inner stream " + streamID2.toString());
            log.info("Successfully appended hello world into log position " + address + ", stream "+ streamID2.toString());
            log.info("Appending to inner stream " + streamID3.toString());
            address = s3.append("hello world remote from inner stream " + streamID3.toString());
            log.info("Successfully appended hello world into log position " + address + ", stream "+ streamID3.toString());

            }
            catch (OutOfSpaceException oose)
            {
                log.error("Out of space during append!", oose);
                System.exit(1);
            }

        log.debug("Reading back 3 results from outer stream " + streamID.toString());
        sresult = (String) s.readNextObject();
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("hello world remote from outer stream " + streamID.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }
        sresult = (String) s.readNextObject();
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("hello world remote from inner stream " + streamID2.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }
        sresult = (String) s.readNextObject();
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("hello world remote from inner stream " + streamID3.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }


        log.debug("Reading back 3 results from inner stream " + streamID2.toString());
        sresult = (String) s2.readNextObject();
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("hello world remote from outer stream " + streamID.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }
        sresult = (String) s2.readNextObject();
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("hello world remote from inner stream " + streamID2.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }
        sresult = (String) s2.readNextObject();
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("hello world remote from inner stream " + streamID3.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }

        log.debug("Reading back 3 results from inner stream " + streamID3.toString());
        sresult = (String) s3.readNextObject();
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("hello world remote from outer stream " + streamID.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }
        sresult = (String) s3.readNextObject();
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("hello world remote from inner stream " + streamID2.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }
        sresult = (String) s3.readNextObject();
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("hello world remote from inner stream " + streamID3.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }

        try {
            log.info("Appending to detached stream " + streamID.toString());
            address = s.append("hello world from detached stream " + streamID.toString());
            log.info("Successfully appended hello world into log position " + address + ", stream "+ streamID.toString());
            log.info("Appending to detached stream " + streamID2.toString());
            address = s2.append("hello world from detached stream " + streamID2.toString());
            log.info("Successfully appended hello world into log position " + address + ", stream "+ streamID2.toString());
            log.info("Appending to detached stream " + streamID3.toString());
            address = s3.append("hello world from detached stream " + streamID3.toString());
            log.info("Successfully appended hello world into log position " + address + ", stream "+ streamID3.toString());

        }
        catch (OutOfSpaceException oose)
        {
            log.error("Out of space during append!", oose);
            System.exit(1);
        }

           log.info("Reading back detached entries");
           sresult = (String) s.readNextObject();
           log.info("Contents were: " + sresult);
           if (!sresult.toString().equals("hello world from detached stream " + streamID.toString()))
                    {
                        log.error("ASSERT Failed: String did not match!");
                        System.exit(1);
                    }
           sresult = (String) s2.readNextObject();
           log.info("Contents were: " + sresult);
           if (!sresult.toString().equals("hello world from detached stream " + streamID2.toString()))
                    {
                        log.error("ASSERT Failed: String did not match!");
                        System.exit(1);
                    }
           sresult = (String) s3.readNextObject();
           log.info("Contents were: " + sresult);
           if (!sresult.toString().equals("hello world from detached stream " + streamID3.toString()))
                    {
                        log.error("ASSERT Failed: String did not match!");
                        System.exit(1);
                    }

        System.exit(0);
        }
        }
}
    }
}

