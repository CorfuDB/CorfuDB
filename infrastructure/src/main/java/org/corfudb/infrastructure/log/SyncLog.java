package org.corfudb.infrastructure.log;

import java.io.File;
import java.io.IOException;
import static java.lang.Thread.sleep;

//This API assume the log is in rotation mode. The older files will be deleted to save
//space at the src dir.
public class SyncLog {
    static public void sync(String srcDir, String dstDir, boolean deleteSrc) {
        File[] files = new File (srcDir).listFiles ();
        long latestTime = 0;

        if (files == null)
            return;

        for (File file : files) {
            if (latestTime < file.lastModified ()) {
                latestTime = file.lastModified ();
            }
        }

        try {
            String cmd = String.format ("rsync -at %s/ %s", srcDir, dstDir);
            System.out.println ("cmd: " + cmd);
            Process p = Runtime.getRuntime ().exec (cmd);
            p.waitFor ();
        } catch (Exception e) {
            System.out.println ("exception: " + e);
        }

        if (deleteSrc == false)
            return;

        //remove older files
        for (File file : files) {
            if (file.lastModified () < latestTime) {
                file.delete ();
                System.out.println ("delete" + file.getName () + " filetime: " +
                        "mtime:" + file.lastModified () + " latestTime:" + latestTime );
            }
        }

        System.out.println ("sync "+ "src:" + srcDir + " dst:" +dstDir);
    }

    public static void main(String args[]) throws IOException {
        String srcDir = args[0];
        String dstDir = args[1];
        while (true) {
            SyncLog.sync (srcDir, dstDir, true);
            try {
                sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace ();
            }
        }
    }
}