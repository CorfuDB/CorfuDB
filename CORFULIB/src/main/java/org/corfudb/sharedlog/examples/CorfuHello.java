package org.corfudb.sharedlog.examples;

import org.corfudb.sharedlog.ClientLib;
import org.corfudb.sharedlog.CorfuException;

/**
 * Created by dmalkhi on 1/16/15.
 */
public class CorfuHello {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        String masteraddress = null;
        ClientLib crf = null;

        if (args.length >= 1) {
            masteraddress = args[0]; // TODO check arg.length
        } else {
            // throw new Exception("must provide master http address"); // TODO
            masteraddress = "http://localhost:8000/corfu";
        }

        try {
            crf = new ClientLib(masteraddress);
        } catch (CorfuException e) {
            e.printStackTrace();
            throw new CorfuException("cannot initalize command parser with master " + masteraddress);
        }
    }
}
