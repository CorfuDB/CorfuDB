package org.corfudb.sharedlog.examples;

import org.corfudb.sharedlog.ClientLib;
import org.corfudb.sharedlog.CorfuException;
import java.util.List;
import java.util.ArrayList;
import java.lang.StringBuilder;

import org.corfudb.infrastructure.thrift.UnitServerHdr;
import org.corfudb.infrastructure.thrift.ExtntInfo;
import org.corfudb.infrastructure.thrift.ExtntWrap;
import org.corfudb.infrastructure.thrift.ExtntMarkType;
import org.corfudb.infrastructure.thrift.ErrorCode;
import org.corfudb.infrastructure.thrift.UnitServerHdr;



/**
 * Created by mwei on 1/28/15.
 */

/* This service works by reading a log until it encounters a TEST token.
 *
 * Once this token is encountered, the service appends to the log for 3 minutes.
 * The first minute is the "warm up" period,
 * The second minute is the measurement period,
 * The third minute is the "cool down" period
 * The warm up and cool down periods are used to make sure that the service is
 * measuring appends only when all clients are actively writing to the log.
 *
 * After 3 minutes, the service writes a COMPLETED_{appends} entry,
 * where number refers to the number of the test, and appends refers to
 * how many sucessfully appends were completed in the second minute.
 */

public class CorfuTestService {

    private static String extentToString(ExtntWrap ew)
    {
        byte[] ew_bytes = new byte[ew.getCtnt().get(0).remaining()];
        ew.getCtnt().get(0).get(ew_bytes);
        return new String(ew_bytes);
    }
    private static void appendString(ClientLib cl, String str)
        throws CorfuException
    {
        byte[] bytes = new byte[cl.grainsize()];
        byte[] bytesstr = str.getBytes();
        System.arraycopy(bytesstr, 0, bytes, 0, bytesstr.length);

        cl.appendExtnt(bytes, bytes.length);
    }

    private static String writeTestPhase(int number, ClientLib cl)
    {
        long startTime = System.currentTimeMillis();
        long appends = 0;
        long failures = 0;
        while (System.currentTimeMillis() < startTime + 15000)
        {
            try {
                appendString(cl, "CORFUTEST");
                appends++;
            }
            catch (CorfuException e)
            {
                failures++;
            }
        }
        long endTime = System.currentTimeMillis();
        long appendsPerSec = Math.round((float)appends / ((endTime-startTime)/1000));
        System.out.println("Total Appends: " + appends + " (" + appendsPerSec + "/s), Total Failures: " + failures);
        return "W" + number + "_" +  appends + "_" + appendsPerSec + "_" + failures;
    }

    private static String readTestPhase(int number, ClientLib cl)
    {
        long startTime = System.currentTimeMillis();
        long reads = 0;
        long failures = 0;
        while (System.currentTimeMillis() < startTime + 5000)
        {
            try {
                extentToString(cl.readExtnt());
                reads++;
            }
            catch (CorfuException e)
            {
                failures++;
            }
        }
        long endTime = System.currentTimeMillis();
        long readsPerSec = Math.round((float)reads / ((endTime-startTime)/1000));
        System.out.println("Total Reads: " + reads + " (" + readsPerSec + "/s), Total Failures: " + failures);
        return "R" + number + "_" +  reads + "_" + readsPerSec + "_" + failures;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        String masteraddress = null;

        if (args.length >= 1) {
            masteraddress = args[0]; // TODO check arg.length
        } else {
            // throw new Exception("must provide master http address"); // TODO
            masteraddress = "http://localhost:8000/corfu";
        }

        try {
            ClientLib cl = new ClientLib(masteraddress);
            //move to the tail
            long tail = cl.querytail();
            System.out.println("Current tail = " + tail);
            cl.setMark(tail);
            while (true)
            {
                try {
                    if (extentToString(cl.readExtnt()).startsWith("TEST"))
                    {
                        System.out.println("Test token for test encountered, starting test.");
                        ArrayList<String> phaseString = new ArrayList<String>();
                        System.out.println("- Phase 1: WARM UP (15s)");
                        phaseString.add(writeTestPhase(1, cl));
                        System.out.println("- Phase 2: MEASUREMENT (15s)");
                        phaseString.add(writeTestPhase(2, cl));
                        System.out.println("- Phase 3: COOL DOWN (15s)");
                        phaseString.add(writeTestPhase(3, cl));

                        //set to 0, in the unfortunate case that we write WAY
                        //faster than we read.
                        System.out.println("- Phase 1: WARM UP (5s)");
                        phaseString.add(readTestPhase(1, cl));
                        System.out.println("- Phase 2: MEASUREMENT (5s)");
                        phaseString.add(readTestPhase(2, cl));
                        System.out.println("- Phase 3: COOL DOWN (5s)");
                        phaseString.add(readTestPhase(3, cl));

                        //oh dear, java doesn't have a string join
                        Boolean first = true;
                        StringBuilder sb = new StringBuilder();
                        sb.append("RESULT:");
                        for (String phase : phaseString)
                        {
                            if (first) { first = false; }
                            else {sb.append(",");}
                            sb.append(phase);
                        }
                        appendString(cl, sb.toString());
                        tail = cl.querytail();
                        System.out.println("Current tail = " + tail);
                        cl.setMark(tail);

                        System.out.println("Test done, new tail is at " + tail);
                    }
                } catch(CorfuException e) {
                    System.out.println("Waiting for TEST token.");
                    Thread.sleep(100);
                }
            }
        } catch(CorfuException e) {
          e.printStackTrace();
          throw new CorfuException("cannot initalize command parser with master " + masteraddress);
        }


    }
}
