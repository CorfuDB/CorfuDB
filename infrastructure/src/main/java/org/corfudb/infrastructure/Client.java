package org.corfudb.infrastructure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Client {
    private Tracer t;
    private boolean isTracing;

    public class Tracer {
        String cat;
        String name;
        int pid;
        long tid;
        long ts;
        String ph;
        String[] args;
        File log;

        public Tracer() {
            cat = "";
            name = "";
            pid = -1;
            tid = -1;
            ts = -1;
            ph = "";
            args = null;
            log = new File("performanceLog.json");
            try {
                if (!log.exists()) {
                    log.createNewFile();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void updateArgs(String c, String n, int p1, long t1, long t2, String p2, String[] a) {
            cat = c;
            name = n;
            pid = p1;
            tid = t1;
            ts = t2;
            ph = p2;
            args = a;
            writeToLog();
        }

        public void writeToLog() {
            try {
                FileWriter fw = new FileWriter(log.getAbsoluteFile(), true);
                BufferedWriter bw = new BufferedWriter(fw);

                bw.write("{");
                bw.write("\"cat\": " + "\"" + t.cat + "\",");
                bw.write("\"pid\": " + t.pid + ",");
                bw.write("\"tid\": " + t.tid + ",");
                bw.write("\"ts\": " + t.ts + ",");
                bw.write("\"ph\": " + "\""  + t.ph + "\",");
                bw.write("\"name\": " + "\""  + t.name + "\",");
                bw.write("\"args\": " + t.args + "},\n");

                bw.close();
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("Done");
        }
    }

    public Client() {
        t = new Tracer();
        isTracing = false;
    }

    public Tracer getTracer() {
        return t;
    }

    public boolean isTracing() {
        return true;
        //return isTracing;
    }

    public int cheapFn() {
        if (isTracing) {
            t.updateArgs("InitialTracerTest", "CheapFn", 0, Thread.currentThread().getId(),
                    System.currentTimeMillis(), "B", null);
        }
        // method body begins here
        int counter = 0;
        for (int i = 0; i < 10; i++) {
            counter += 1;
        }
        // end method body
        if (isTracing) {
            t.updateArgs("InitialTracerTest", "CheapFn", 0, Thread.currentThread().getId(),
                    System.currentTimeMillis(), "E", null);
        }
        return counter;
    }

    public int mediumFn() {
        if (isTracing) {
            t.updateArgs("InitialTracerTest", "MediumFn", 0, Thread.currentThread().getId(),
                    System.currentTimeMillis(), "B", null);
        }
        // begin method body
        int counter = 0;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                counter += 1;
            }
        }
        // end method body
        if (isTracing) {
            t.updateArgs("InitialTracerTest", "MediumFn", 0, Thread.currentThread().getId(),
                    System.currentTimeMillis(), "E", null);
        }
        return counter;
    }

    public int costlyFn() {
        if (isTracing) {
            t.updateArgs("InitialTracerTest", "CostlyFn", 0, Thread.currentThread().getId(),
                    System.currentTimeMillis(), "B", null);
        }
        // begin method body
        int counter = 0;
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 100; j++) {
                for (int k = 0; k < 100; k++) {
                    counter += 1;
                }
            }
        }
        // end method body
        if (isTracing) {
            t.updateArgs("InitialTracerTest", "CostlyFn", 0, Thread.currentThread().getId(),
                    System.currentTimeMillis(), "E", null);
        }
        return counter;
    }

    public void startMethodTrace() {
        isTracing = true;
    }

    public void endMethodTrace() {
        isTracing = false;
    }

    public static void main(String[] args) {
        // Nothing to do here
    }

    public void test() {
        Client n = new Client();

        n.startMethodTrace();
        n.cheapFn();
        n.endMethodTrace();

        n.startMethodTrace();
        n.mediumFn();
        n.endMethodTrace();

        n.startMethodTrace();
        n.costlyFn();
        n.endMethodTrace();
    }
}