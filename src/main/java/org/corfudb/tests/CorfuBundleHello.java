package org.corfudb.tests;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.abstractions.Stream;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.abstractions.SharedLog;
import org.corfudb.client.configmasters.IConfigMaster;
import org.corfudb.client.Timestamp;
import org.corfudb.client.OutOfSpaceException;
import org.corfudb.client.entries.CorfuDBStreamEntry;
import org.corfudb.client.entries.BundleEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import org.corfudb.client.LinearizationException;

import java.util.UUID;
import org.corfudb.client.abstractions.Bundle;

public class CorfuBundleHello {

    private static final Logger log = LoggerFactory.getLogger(CorfuBundleHello.class);

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


               log.info("Creating new bundle entry on stream " + streamID.toString());

               ArrayList<UUID> remoteStreams = new ArrayList<UUID>();
               remoteStreams.add(streamID2);
               remoteStreams.add(streamID3);
               Bundle b = new Bundle(s, remoteStreams, "Bundled Hello World!", true);
               Timestamp timestampBundle = b.apply();

               Timestamp s1_bundle;
               Timestamp s2_bundle;
               Timestamp s3_bundle;

               BundleEntry b2;
               BundleEntry b3;

                CorfuDBStreamEntry cdse;
                log.debug("Reading back bundled append.");
                cdse = s.readNextEntry();
                s1_bundle = cdse.getTimestamp();
                sresult = (String) cdse.payload;
                log.info("Contents were: " + sresult);
                if (!sresult.toString().equals("Bundled Hello World!"))
                {
                log.error("ASSERT Failed: String did not match!");
                System.exit(1);
                }
                b2 = (BundleEntry) s2.readNextEntry();
                s2_bundle = b2.getTimestamp();
                sresult = (String) b2.payload;
                log.info("Contents were: " + sresult);
                if (!sresult.toString().equals("Bundled Hello World!"))
                {
                log.error("ASSERT Failed: String did not match!");
                System.exit(1);
                }
                b3 = (BundleEntry) s3.readNextEntry();
                s3_bundle = b3.getTimestamp();
                sresult = (String) b3.payload;
                log.info("Contents were: " + sresult);
                if (!sresult.toString().equals("Bundled Hello World!"))
                {
                log.error("ASSERT Failed: String did not match!");
                System.exit(1);
                }

                log.debug("s1=" + s1_bundle.toString() + ", s2=" +s2_bundle.toString());
            log.info("Making sure addresses are all equal");
            checkAddressEquality(s1_bundle, s2_bundle);
            checkAddressEquality(s2_bundle, s3_bundle);

        /* Now we append to the bundles, and see if we can read back the same entry on all streams. */
        log.info("Appending to each bundle");
        Timestamp s2_bundleAppend = b2.writeSlot("Hello from bundle " + streamID2.toString());
        Timestamp s3_bundleAppend = b3.writeSlot("Hello from bundle " + streamID3.toString());

        Timestamp s1_firstAddress = null;
        Timestamp s1_secondAddress = null;
        Timestamp s2_firstAddress = null;
        Timestamp s2_secondAddress = null;
        Timestamp s3_firstAddress = null;
        Timestamp s3_secondAddress = null;

        log.debug("Reading back 2 results from outer stream " + streamID.toString());
        cdse = s.readNextEntry();
        s1_firstAddress = cdse.getTimestamp();
        sresult = (String) cdse.payload;
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("Hello from bundle " + streamID2.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }
        cdse = s.readNextEntry();
        s1_secondAddress = cdse.getTimestamp();
        sresult = (String) cdse.payload;
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("Hello from bundle " + streamID3.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }
        checkAddresses(s1_bundle, s1_firstAddress, s1_secondAddress);

        log.debug("Reading back 2 results from inner stream " + streamID2.toString());
        cdse = s2.readNextEntry();
        s2_firstAddress = cdse.getTimestamp();
        sresult = (String) cdse.payload;
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("Hello from bundle " + streamID2.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }
        cdse = s2.readNextEntry();
        s2_secondAddress = cdse.getTimestamp();
        sresult = (String) cdse.payload;
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("Hello from bundle " + streamID3.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }
        checkAddresses(s2_bundle, s2_firstAddress, s2_secondAddress);

        log.debug("Reading back 2 results from inner stream " + streamID2.toString());
        cdse = s3.readNextEntry();
        s3_firstAddress = cdse.getTimestamp();
        sresult = (String) cdse.payload;
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("Hello from bundle " + streamID2.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }
        cdse = s3.readNextEntry();
        s3_secondAddress = cdse.getTimestamp();
        sresult = (String) cdse.payload;
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("Hello from bundle " + streamID3.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }
        checkAddresses(s3_bundle, s3_firstAddress, s3_secondAddress);

        log.info("Checking for address equality on bundle");
        checkAddressEquality(s1_firstAddress, s2_firstAddress);
        checkAddressEquality(s2_firstAddress, s3_firstAddress);
        checkAddressEquality(s1_secondAddress, s2_secondAddress);
        checkAddressEquality(s2_secondAddress, s3_secondAddress);

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

           try (Stream s2_1 = new Stream(client, streamID2))
            {
                CorfuDBStreamEntry cdbse;
                log.info("Re-reading stream 2");
                log.info("Contents were: " + sresult);
                sresult = (String) s2_1.readNextObject();
                if (!sresult.toString().equals("hello world from stream " + streamID2.toString()))
                        {
                            log.error("ASSERT Failed: String did not match!");
                            System.exit(1);
                        }
                cdbse = s2_1.readNextEntry();
                s2_bundle = cdbse.getTimestamp();
                sresult = (String) cdbse.payload;
                log.info("Contents were: " + sresult);
                if (!sresult.toString().equals("Bundled Hello World!"))
                {
                log.error("ASSERT Failed: String did not match!");
                System.exit(1);
                }
                log.debug("Reading back 2 results from inner stream " + streamID2.toString());
                cdse = s2_1.readNextEntry();
                s2_firstAddress = cdse.getTimestamp();
                sresult = (String) cdse.payload;
                log.info("Contents were: " + sresult);
                if (!sresult.toString().equals("Hello from bundle " + streamID2.toString()))
                        {
                            log.error("ASSERT Failed: String did not match!");
                            System.exit(1);
                        }
                cdse = s2_1.readNextEntry();
                s2_secondAddress = cdse.getTimestamp();
                sresult = (String) cdse.payload;
                log.info("Contents were: " + sresult);
                if (!sresult.toString().equals("Hello from bundle " + streamID3.toString()))
                        {
                            log.error("ASSERT Failed: String did not match!");
                            System.exit(1);
                        }
                checkAddresses(s2_bundle, s2_firstAddress, s2_secondAddress);
            }

           try (Stream s3_1 = new Stream(client, streamID3))
            {
                CorfuDBStreamEntry cdbse;
                log.info("Re-reading stream 3");
                log.info("Contents were: " + sresult);
                sresult = (String) s3_1.readNextObject();
                if (!sresult.toString().equals("hello world from stream " + streamID3.toString()))
                        {
                            log.error("ASSERT Failed: String did not match!");
                            System.exit(1);
                        }
                cdbse = s3_1.readNextEntry();
                s3_bundle = cdbse.getTimestamp();
                sresult = (String) cdbse.payload;
                log.info("Contents were: " + sresult);
                if (!sresult.toString().equals("Bundled Hello World!"))
                {
                log.error("ASSERT Failed: String did not match!");
                System.exit(1);
                }
                log.debug("Reading back 2 results from inner stream " + streamID3.toString());
                cdse = s3_1.readNextEntry();
                s3_firstAddress = cdse.getTimestamp();
                sresult = (String) cdse.payload;
                log.info("Contents were: " + sresult);
                if (!sresult.toString().equals("Hello from bundle " + streamID2.toString()))
                        {
                            log.error("ASSERT Failed: String did not match!");
                            System.exit(1);
                        }
                cdse = s3_1.readNextEntry();
                s3_secondAddress = cdse.getTimestamp();
                sresult = (String) cdse.payload;
                log.info("Contents were: " + sresult);
                if (!sresult.toString().equals("Hello from bundle " + streamID3.toString()))
                        {
                            log.error("ASSERT Failed: String did not match!");
                            System.exit(1);
                        }
                checkAddresses(s3_bundle, s3_firstAddress, s3_secondAddress);
            }

        System.exit(0);
        }
        }
}
    }

    static void checkAddressEquality(Timestamp a1, Timestamp a2)
    {
        log.info("Checking that addresses are equal");
        log.info("Address 1 == Address 2");
        if (!(a1.equals(a2)))
        {
            log.error("ASSERT failed: Address 1 should EQUAL Address 2");
            System.exit(1);
        }
        log.info("Address 2 == Address 1");
        if (!(a2.equals(a1)))
        {
            log.error("ASSERT failed: Address 2 should EQUAL Address 1");
            System.exit(1);
        }
    }

    static void checkAddresses(Timestamp firstAddress, Timestamp secondAddress, Timestamp thirdAddress)
    {
            try {
                log.info("Checking that addresses are comparable");
                log.info("Address 1 == Address 1");
                if (!(firstAddress.compareTo(firstAddress) == 0))
                {
                    log.error("ASSERT failed: Address 1 should EQUAL Address 1, got {}", firstAddress.compareTo(firstAddress));
                    System.exit(1);
                }
                log.info("Address 2 == Address 2");
                if (!(secondAddress.compareTo(secondAddress) == 0))
                {
                    log.error("ASSERT failed: Address 2 should EQUAL Address 2, got {}", secondAddress.compareTo(secondAddress));
                    System.exit(1);
                }
                log.info("Address 3 == Address 3");
                if (!(thirdAddress.compareTo(thirdAddress) == 0))
                {
                    log.error("ASSERT failed: Address 3 should EQUAL Address 3, got {}", thirdAddress.compareTo(thirdAddress));
                    System.exit(1);
                }

                log.info("Address 1 < Address 2");
                if (!(firstAddress.compareTo(secondAddress) < 0))
                {
                    log.error("ASSERT failed: Address 1 should come BEFORE Address 2, got {}", firstAddress.compareTo(secondAddress));
                    System.exit(1);
                }
                log.info("Address 1 < Address 3");
                if (!(firstAddress.compareTo(thirdAddress) < 0))
                {
                    log.error("ASSERT failed: Address 1 should come BEFORE Address 3, got {}", firstAddress.compareTo(thirdAddress));
                    System.exit(1);
                }
                log.info("Address 2 > Address 1");
                if (!(secondAddress.compareTo(firstAddress) > 0))
                {
                    log.error("ASSERT failed: Address 2 should come AFTER Address 1, got {}", secondAddress.compareTo(firstAddress));
                    System.exit(1);
                }
                log.info("Address 2 < Address 3");
                if (!(secondAddress.compareTo(thirdAddress) < 0))
                {
                    log.error("ASSERT failed: Address 2 should come BEFORE Address 3, got {}", secondAddress.compareTo(thirdAddress));
                    System.exit(1);
                }
                log.info("Address 3 > Address 1");
                if (!(thirdAddress.compareTo(firstAddress) > 0))
                {
                    log.error("ASSERT failed: Address 3 should come AFTER Address 1, got {}", thirdAddress.compareTo(firstAddress));
                    System.exit(1);
                }
                log.info("Address 3 > Address 2");
                if (!(thirdAddress.compareTo(secondAddress) > 0))
                {
                    log.error("ASSERT failed: Address 3 should come AFTER Address 2, got {}", thirdAddress.compareTo(secondAddress));
                    System.exit(1);
                }
            }
            catch (ClassCastException cce)
            {
                log.error("ASSERT failed: all addresses of attached stream SHOULD be comparable", cce);
                System.exit(1);
            }

    }
}

