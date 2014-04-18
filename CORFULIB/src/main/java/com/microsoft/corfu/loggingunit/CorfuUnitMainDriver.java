package com.microsoft.corfu.loggingunit;

import com.microsoft.corfu.CorfuConfigManager;
import com.microsoft.corfu.CorfuNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by dalia on 4/17/2014.
 */
public class CorfuUnitMainDriver {
    private static Logger slog = LoggerFactory.getLogger(CorfuUnitMainDriver.class);

    /**
     * @param args see Usage string definition for command line arguments usage
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        int UNITCAPACITY; // taken from configuration: capacity in ENTRYSIZE units, i.e. UNITCAPACITY*ENTRYSIZE bytes
        int ENTRYSIZE;	// taken from configuration: individual log-entry size in bytes
        int PORT;		// taken from configuration: port number this unit listens on
        String DRIVENAME = null; // command line argument: where to persist data (unless rammode is on)
        boolean RAMMODE = false; // command line switch: work in memory (no data persistence)
        boolean RECOVERY = false; // command line switch: indicate whether we load log from disk on startup
        boolean REBUILD = false; String rebuildnode = null;

        CorfuConfigManager CM = new CorfuConfigManager(new File("./corfu.xml"));
        int sid = -1, rid = -1;
        ENTRYSIZE = CM.getGrain();
        UNITCAPACITY = CM.getUnitsize();
        String Usage = "\n Usage: " + CorfuUnitServer.class.getName() + "-unit <stripe:replica> " +
                "<-rammode> | <-drivename <name> [-recover | -rebuild <hostname:port> ] ";

        for (int i = 0; i < args.length; ) {
            if (args[i].startsWith("-recover")) {
                RECOVERY = true;
                slog.info("recovery mode");
                i += 1;
            } else if (args[i].startsWith("-rammode")) {
                RAMMODE = true;
                slog.info("working in RAM mode");
                i += 1;
            } else if (args[i].startsWith("-rebuild") && i < args.length-1) {
                REBUILD = true;
                rebuildnode = args[i+1];
                slog.info("rebuild from {}", rebuildnode);
                i += 2;
            } else if (args[i].startsWith("-unit") && i < args.length-1) {
                sid = Integer.parseInt(args[i+1].substring(0, args[i+1].indexOf(":")));
                rid = Integer.parseInt(args[i+1].substring(args[i+1].indexOf(":")+1));
                slog.info("stripe number: {} replica num: {}", sid, rid);
                i += 2;
            } else if (args[i].startsWith("-drivename") && i < args.length-1) {
                DRIVENAME = args[i+1];
                slog.info("drivename: " + DRIVENAME);
                i += 2;
            } else {
                slog.error(Usage);
                throw new Exception("unknown param: " + args[i]);
            }
        }

        if ((!RAMMODE && DRIVENAME == null) ||
                sid < 0 ||
                rid < 0 ||
                (RAMMODE && RECOVERY) ||
                (REBUILD && RECOVERY)) {
            slog.error(Usage);
            throw new Exception("bad command line parameters, see usage instructions");
        }
        if (CM.getNumGroups() <= sid) {
            slog.error("stripe # {} exceeds num of stripes in configuration {}; quitting", sid, CM.getNumGroups());
            throw new Exception("bad sunit #");
        }

        CorfuNode[] cn = CM.getGroupByNumber(sid);
        if (cn.length < rid) {
            slog.error("replica # {} exceeds replica group size {}; quitting", rid, cn.length);
            throw new Exception("bad sunit #");
        }
        PORT = cn[rid].getPort();

        slog.info("unit server {}:{} starting; port={}, entsize={} capacity={}",
                sid, rid, PORT, ENTRYSIZE, UNITCAPACITY);

        final CorfuUnitTask cu = new CorfuUnitTask();
        cu.RAMMODE = RAMMODE;
        cu.RECOVERY = RECOVERY;
        cu.REBUILD = REBUILD;
        cu.rebuildnode = rebuildnode;
        cu.PORT = PORT;
        cu.UNITCAPACITY = UNITCAPACITY;
        cu.ENTRYSIZE = ENTRYSIZE;
        cu.DRIVENAME = DRIVENAME;

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    cu.serverloop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }}).run();
    }

}
