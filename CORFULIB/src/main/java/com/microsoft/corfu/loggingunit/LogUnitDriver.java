package com.microsoft.corfu.loggingunit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by dalia on 4/17/2014.
 */
public class LogUnitDriver {
    private static Logger slog = LoggerFactory.getLogger(LogUnitDriver.class);
    static LogUnitTask cu;

    /**
     * @param args see Usage string definition for command line arguments usage
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String Usage = "\n Usage: " + LogUnitService.class.getName() + "<-port <port>> [-size <size>] [-grain <grain>]" +
                "<-rammode> | <-drivename <name> [-recover | -rebuild <hostname:port> ] ";

        LogUnitTask.Builder cb = new LogUnitTask.Builder();

        for (int i = 0; i < args.length; ) {
            if (args[i].startsWith("-port")) {
                cb.setPORT(Integer.parseInt(args[i+1]));
                slog.info("port: " + args[i+1]);
                i += 2;
            } else if (args[i].startsWith("-size")) {
                cb.setPORT(Integer.parseInt(args[i + 1]));
                slog.info("port: " + args[i+1]);
                i += 2;
//            } else if (args[i].startsWith("-grain")) {
//                cb.setGRAIN(Integer.getInteger(args[i+1]));
//                slog.info("grain: " + args[i+1]);
//                i += 1;
            } else if (args[i].startsWith("-recover")) {
                cb.setRECOVERY(true);
                slog.info("recovery mode");
                i += 1;
            } else if (args[i].startsWith("-rammode")) {
                cb.setRAMMODE(true);
                slog.info("working in RAM mode");
                i += 1;
            } else if (args[i].startsWith("-rebuild") && i < args.length-1) {
                cb.setREBUILD(true);
                cb.setRebuildnode(args[i + 1]);
                slog.info("rebuild from {}", args[i+1]);
                i += 2;
            } else if (args[i].startsWith("-drivename") && i < args.length-1) {
                cb.setDRIVENAME(args[i + 1]);
                slog.info("drivename: " + args[i+1]);
                i += 2;
            } else {
                slog.error(Usage);
                throw new Exception("unknown param: " + args[i]);
            }
        }
        cu = cb.build();

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
