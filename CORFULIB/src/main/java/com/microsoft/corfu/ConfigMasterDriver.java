package com.microsoft.corfu;

import java.io.FileNotFoundException;

/**
 * Created by dalia on 7/1/2014.
 */
public class ConfigMasterDriver {
    public static void main(String[] args) throws CorfuException, FileNotFoundException, InterruptedException {
        Runnable t;
        if (args.length > 0 && "-recover".equals(args[0]))
            t = new ConfigMasterService("./corfu.xml", true);
        else
            t = new ConfigMasterService("./corfu.xml", false);

        new Thread(t).start();
    }
}
