package org.corfudb.sharedlog.examples;

import org.corfudb.sharedlog.ClientLib;
import org.corfudb.sharedlog.CorfuException;


import org.corfudb.infrastructure.thrift.UnitServerHdr;
import org.corfudb.infrastructure.thrift.ExtntInfo;
import org.corfudb.infrastructure.thrift.ExtntWrap;
import org.corfudb.infrastructure.thrift.ExtntMarkType;
import org.corfudb.infrastructure.thrift.ErrorCode;
import org.corfudb.infrastructure.thrift.UnitServerHdr;

/**
 * Created by mwei on 1/28/15.
 */

/* This service works by reading a log until it encounters a TEST_{number} token.
 *
 * Once this token is encountered, the service appends to the log for 3 minutes.
 * The first minute is the "warm up" period,
 * The second minute is the measurement period,
 * The third minute is the "cool down" period
 * The warm up and cool down periods are used to make sure that the service is
 * measuring appends only when all clients are actively writing to the log.
 *
 * After 3 minutes, the service writes a COMPLETED_{number}_{appends} entry,
 * where number refers to the number of the test, and appends refers to
 * how many sucessfully appends were completed in the second minute.
 */

public class CorfuTestClient {

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
            byte[] bytes = new byte[cl.grainsize()];
            byte[] bytesstr = "TEST_1234".getBytes();
            System.arraycopy(bytesstr, 0, bytes, 0, bytesstr.length);
            long offset = cl.appendExtnt(bytes, cl.grainsize());
            cl.setMark(offset);
            //the test will take about 5 minutes, so sleep until then
            //don't want to mess with the test...
            System.out.println("Waiting for test to complete...");
            Thread.sleep(60000);
            System.out.println("Waiting for test to complete...");
            Thread.sleep(60000);

            long reads  = 0;
            long appends = 0;
            //now read forward until we get to the tail.
            while (true)
            {
                try {
                    String result = extentToString(cl.readExtnt());
                    if (result.startsWith("RESULT:"))
                    {
                       System.out.println("Got result:" + result);
                       String[] res = result.split(":");
                       String[] phases = res[1].split(",");
                       //we should only care about phases 2 (write) and 4 (read)
                       String[] phase2 = phases[1].split("_");
                       String[] phase4 = phases[3].split("_");
                       appends += Integer.parseInt(phase2[2]);
                       reads += Integer.parseInt(phase4[2]);
                    }
                } catch(CorfuException e) {
                    System.out.println("Done!");
                    break;
                }
            }

            System.out.println("Total appends/s: " + appends + ", Total Reads/s: "+ reads);
            //test is done, trim
            long tail = cl.querytail();
            System.out.println("Current tail = " + tail);
            System.out.println("Test done, new tail is at " + tail);
            cl.trim(tail);
            }
         catch(CorfuException e) {
          e.printStackTrace();
          throw new CorfuException("cannot initalize command parser with master " + masteraddress);
        }


    }
}
