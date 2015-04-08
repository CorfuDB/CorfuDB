package org.corfudb.tests.benchtests;

import gnu.getopt.Getopt;
import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.ITimestamp;
import org.corfudb.runtime.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;

/**
 * Created by crossbach on 4/3/15.
 */
public abstract class MicroBenchmark {

    static Logger dbglog = LoggerFactory.getLogger(MicroBenchmark.class);
    static final String DEFAULT_MASTER = "http://localhost:8002/corfu";
    static final int DEFAULT_RPCPORT = 9090;
    static final double DEFAULT_RW_RATIO = 0.75;
    static final int DEFAULT_OBJECTS = 10;
    static final int DEFAULT_OBJECT_SIZE = 256;
    static final int DEFAULT_COMMAND_SIZE = 16;
    static final boolean DEFAULT_USE_TRANSACTIONS = true;
    static final int DEFAULT_THREADS = 1;
    static final boolean DEFAULT_COLLECT_LATENCY_STATS = true;
    static final String DEFAULT_TEST = null;
    static final int DEFAULT_OPERATION_COUNT = 100;
    static final String DEFAULT_STREAM_IMPL = "DUMMY";
    static final String DEFAULT_RUNTIME = "TXRuntime";
    static final boolean DEFAULT_READ_YOUR_WRITES = true;
    static final boolean DEFAULT_OPAQUE = true;
    static List<String> s_tests = new ArrayList<String>();
    static Map<String, BenchmarkFactory> s_bncs = new HashMap<String, BenchmarkFactory>();
    static {
        registerBenchmark("CommandThroughput", new CommandThroughputMicroBenchmark.CommandThroughputMicroBenchmarkFactory());
    }

    protected Options m_options;
    protected AbstractRuntime m_rt;
    protected IStreamFactory m_sf;
    protected CorfuDBClient m_client;
    protected long m_exec;
    CyclicBarrier m_startbarrier;   // start barrier to ensure all threads finish init before tx loop
    CyclicBarrier m_stopbarrier;    // stop barrier to ensure no thread returns until all have finished
    MicroBenchmarkPartition[] m_partitions;
    public static boolean s_verbose = true;
    public static transient long[] s_commandlatencies;

    public static class MicroBenchmarkPartition implements Runnable {

        CyclicBarrier m_startbarrier;
        CyclicBarrier m_stopbarrier;
        MicroBenchmark m_mb;
        long m_start;
        long m_end;
        int m_nId;
        int m_workItems;
        int m_workIdx;

        /**
         * ctor
         * @param s
         * @param e
         * @param m
         * @param id
         */
        public
        MicroBenchmarkPartition(CyclicBarrier s, CyclicBarrier e, MicroBenchmark m, int id) {
            m_startbarrier = s;
            m_stopbarrier = e;
            m_nId = id;
            m_mb = m;
            int nThreads = m.m_options.getThreadCount();
            int nOps = m.m_options.getOperationCount();
            int nItemsPerWorker = nOps / nThreads;
            m_workIdx = nItemsPerWorker * id;
            m_workItems = (id == nThreads - 1) ? nOps - m_workIdx : nItemsPerWorker;
        }

        /**
         * run method (runnable)
         * wait for all worker threads to initialize
         * perform the specified number of random transactions
         * wait for all worker threads to complete the tx loop.
         */
        public void run()
        {
            awaitInit();
            m_mb.executeImpl(m_workIdx, m_workItems);
            awaitComplete();
        }

        /**
         * use the start barrier to wait for
         * all worker threads to initialize
         */
        private void awaitInit() {
            try {
                m_startbarrier.await();
            } catch(Exception bbe) {
                throw new RuntimeException(bbe);
            }
            inform("Entering run loop for tx list tester thread %d\n", m_nId);
            m_start = curtime();
        }

        /**
         * use the start barrier to wait for
         * all worker threads to complete the tx loop
         */
        private void awaitComplete() {
            m_end = curtime();
            try {
                m_stopbarrier.await();
            } catch(Exception bbe) {
                throw new RuntimeException(bbe);
            }
            inform("Leaving run loop for tx list tester thread %d\n", m_nId);
        }
    }

    public static class Options {

        protected String m_master;
        protected int m_rpcport;
        protected double m_rwpct;
        protected int m_objects;
        protected int m_objsize;
        protected int m_cmdsize;
        protected boolean m_usetx;
        protected int m_threads;
        protected int m_ops;
        protected boolean m_latstats;
        protected boolean m_verbose;
        protected boolean m_readmywrites;
        protected boolean m_opaque;
        protected String m_test;
        protected String m_streamimpl;
        protected String m_runtime;
        protected String m_rpchost;

        public boolean getOpacity() {
            return m_opaque;
        }

        public boolean getReadMyWrites() {
            return m_readmywrites;
        }

        public String getRuntime() {
            return m_runtime;
        }

        public String getMaster() {
            return m_master;
        }

        public String getStreamImplementation() {
            return m_streamimpl;
        }

        public int getRPCPort() {
            return m_rpcport;
        }

        public double getRWRatio() {
            return m_rwpct;
        }

        public int getObjectCount() {
            return m_objects;
        }

        public int getObjectSize() {
            return m_objsize;
        }

        public int getCommandSize() {
            return m_cmdsize;
        }

        public boolean getUseTransactions() {
            return m_usetx;
        }

        public int getThreadCount() {
            return m_threads;
        }

        public int getOperationCount() {
            return m_ops;
        }

        public boolean getCollectLatencyStats() {
            return m_latstats;
        }

        public String getTestScenario() {
            return m_test;
        }

        public boolean isVerbose() {
            return m_verbose;
        }

        /**
         * get the RPC host name
         * @return
         */
        public String getRPCHostName() {
            if(m_rpchost == null) {
                try {
                    m_rpchost = InetAddress.getLocalHost().getHostName();
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            }
            return m_rpchost;
        }

        /**
         * ctor
         */
        private Options() {
            m_master = DEFAULT_MASTER;
            m_rpcport = DEFAULT_RPCPORT;
            m_rwpct = DEFAULT_RW_RATIO;
            m_objects = DEFAULT_OBJECTS;
            m_objsize = DEFAULT_OBJECT_SIZE;
            m_cmdsize = DEFAULT_COMMAND_SIZE;
            m_usetx = DEFAULT_USE_TRANSACTIONS;
            m_threads = DEFAULT_THREADS;
            m_ops = DEFAULT_OPERATION_COUNT;
            m_latstats = DEFAULT_COLLECT_LATENCY_STATS;
            m_test = DEFAULT_TEST;
            m_verbose = false;
            m_streamimpl = DEFAULT_STREAM_IMPL;
            m_runtime = DEFAULT_RUNTIME;
            m_readmywrites = DEFAULT_READ_YOUR_WRITES;
            m_opaque = DEFAULT_OPAQUE;
            m_rpchost = null;
        }

        /**
         * toString
         * @return
         */
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("runtime:\t");
            sb.append(m_runtime);
            sb.append("\nstream impl:");
            sb.append(m_streamimpl);
            sb.append("\nuse tx:");
            sb.append(m_usetx);
            sb.append("\nopacity:\t");
            sb.append(m_opaque);
            sb.append("\nread-my-writes:");
            sb.append(m_readmywrites);
            sb.append("\nmaster:\t");
            sb.append(m_master);
            sb.append("\nRPC port:\t");
            sb.append(m_rpcport);
            sb.append("\nRPC host:\t");
            sb.append(getRPCHostName());
            sb.append("\nR/W ratio:\t");
            sb.append(m_rwpct);
            sb.append("\nstream count:");
            sb.append(m_objects);
            sb.append("\nobject size:");
            sb.append(m_objsize);
            sb.append("\ncmd size:");
            sb.append(m_cmdsize);
            sb.append("\noperations:");
            sb.append(m_ops);
            sb.append("\nscenario:");
            sb.append(m_test);
            sb.append("\nthreads:\t");
            sb.append(m_threads);
            sb.append("\nlatency stats:");
            sb.append(m_latstats);
            sb.append("\nverbose:\t");
            sb.append(m_verbose);
            return sb.toString();
        }


        /**
         * list all supported tests
         */
        public static void listTests() {
            System.err.println(listBncs());
        }

        /**
         * create an options object from command line args
         *
         * @param args
         * @return null on failure, otherwise, Micro-benchmark options.
         */
        public static Options getOptions(String[] args) {
            int c = 0;
            Options options = new Options();
            try {
                Getopt g = new Getopt("CorfuDBTester", args, "m:p:r:o:s:c:x:t:n:L:S:A:R:w:O:lhv");
                while ((c = g.getopt()) != -1) {
                    switch (c) {
                        case 'O':
                            options.m_opaque = Boolean.parseBoolean(g.getOptarg());
                            break;
                        case 'W':
                            options.m_readmywrites = Boolean.parseBoolean(g.getOptarg());
                            break;
                        case 'R':
                            options.m_runtime = g.getOptarg();
                            if (options.m_runtime == null)
                                throw new Exception("must provide valid runtime class name");
                            break;
                        case 'm':
                            options.m_master = g.getOptarg();
                            if (options.m_master == null)
                                throw new Exception("must provide master http address using -m flag");
                            break;
                        case 'p':
                            options.m_rpcport = Integer.parseInt(g.getOptarg());
                            break;
                        case 'r':
                            options.m_rwpct = Double.parseDouble(g.getOptarg());
                            break;
                        case 'o':
                            options.m_objects = Integer.parseInt(g.getOptarg());
                            break;
                        case 's':
                            options.m_objsize = Integer.parseInt(g.getOptarg());
                            break;
                        case 'c':
                            options.m_cmdsize = Integer.parseInt(g.getOptarg());
                            break;
                        case 'x':
                            options.m_usetx = Boolean.parseBoolean(g.getOptarg());
                            break;
                        case 't':
                            options.m_threads = Integer.parseInt(g.getOptarg());
                            if (options.m_threads < 1)
                                throw new Exception("need at least one thread!");
                            break;
                        case 'n':
                            options.m_ops = Integer.parseInt(g.getOptarg());
                            if (options.m_ops < 1)
                                throw new Exception("need at least one op!");
                            break;
                        case 'L':
                            options.m_latstats = Boolean.parseBoolean(g.getOptarg());
                            break;
                        case 'S':
                            options.m_streamimpl = g.getOptarg();
                            if(options.m_streamimpl == null)
                                throw new Exception("need a stream implementation parameter!");
                            if(options.m_streamimpl.compareToIgnoreCase("DUMMY") != 0 &&
                                    options.m_streamimpl.compareToIgnoreCase("HOP") != 0) {
                                throw new Exception("need a valid stream implementation parameter (HOP|DUMMY)!");
                            }
                            break;
                        case 'A':
                            options.m_test = g.getOptarg();
                            break;
                        case 'v':
                            options.m_verbose = true;
                            break;
                        case 'l':
                            listTests();
                            return null;
                        case 'h':
                            printUsage();
                            return null;
                    }
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
                e.printStackTrace(System.err);
                printUsage();
                return null;
            }
            if(options.m_test == null) {
                System.err.println("A test scenario must be specified with \"-A\"");
                printUsage();
                return null;
            }

            return options;
        }

        /**
         * print the usage
         */
        static void printUsage() {
            System.err.println("usage: java MicroBenchmark");
            System.err.println("\t[-m masternode] (default == \"http://localhost:8002/corfu\"");
            System.err.println("\t[-p rpcport] (default == 9090)");
            System.err.println("\t[-r read write pct (double, default=75% reads)]");
            System.err.println("\t[-o object/stream count] number of distinct objects/streams, (default = 10)");
            System.err.println("\t[-s object size] approximate serialized byte size of object (default = 256)");
            System.err.println("\t[-c command size] approximate serialized size of object commands (default = 16)");
            System.err.println("\t[-x true|false] use/don't use transactions (default is use-tx)");
            System.err.println("\t[-w true|false] support/don't support read-after-write consistency (default is support)");
            System.err.println("\t[-O true|false] support/don't support opaque transactions (default is support)");
            System.err.println("\t[-t number of threads] default == 1");
            System.err.println("\t[-n number of ops]");
            System.err.println("\t[-S DUMMY|HOP] stream implementation--default == DUMMY");
            System.err.println("\t[-R runtime] runtime implementation SIMPLE|TX");
            System.err.println("\t[-L collect latency stats] true|false (default is true) \n");
            System.err.println("\t[-A test scenario] default is 'basic'");
            System.err.println("\t[-l] list available test scenarios");
            System.err.println("\t[-h help] print this message\n");
            System.err.println("\t[-v verbose mode...]");
        }

    }

    /**
     * super-class ctor
     * @param options
     */
    public MicroBenchmark(Options options) {
        m_options = options;
        m_client = getClient();
        m_sf = StreamFactory.getStreamFactory(m_client, m_options.getStreamImplementation());
        m_rt = getRuntime();
        m_startbarrier = new CyclicBarrier(m_options.getThreadCount());
        m_stopbarrier = new CyclicBarrier(m_options.getThreadCount());
        m_partitions = new MicroBenchmarkPartition[m_options.getThreadCount()];
    }

    /**
     * get a new client object/connection
     * @return
     */
    protected CorfuDBClient getClient() {

        CorfuDBClient crf=new CorfuDBClient(m_options.getMaster());
        crf.startViewManager();
        crf.waitForViewReady();
        return crf;
    }

    /**
     * get a runtime object
     * @return a new object that implements abstract runtime
     */
    protected AbstractRuntime getRuntime() {
        long roid = DirectoryService.getUniqueID(m_sf);
        if(m_options.getRuntime().toUpperCase().contains("SIMPLE"))
            return new SimpleRuntime(m_sf, roid, m_options.getRPCHostName(), m_options.getRPCPort());
        if(m_options.getRuntime().toUpperCase().contains("TX"))
            return new TXRuntime(m_sf, roid,
                    m_options.getRPCHostName(), m_options.getRPCPort(),
                    m_options.getReadMyWrites());
        throw new RuntimeException("unknown runtime implementation requested: " + m_options.getRuntime());
    }

    /**
     * list all known microbenchmark test scenarios
     * @return
     */
    public static String listBncs() {
         return String.join("\n\t", s_bncs.keySet());
    }

    /**
     * get an object that implements a microbenchmark
     * @param options
     * @return
     */
    public static MicroBenchmark getBenchmark(Options options) {
        String test = options.getTestScenario();
        BenchmarkFactory factory = s_bncs.getOrDefault(test, null);
        if(factory == null) {
            String errorString = "Unknown microbenchmark: " + test;
            System.err.println(errorString);
            System.err.print("Known tests:\n\t");
            System.err.println(listBncs());
            throw new RuntimeException(errorString);
        }
        return factory.instantiate(options);
    }

    /**
     * execute
     */
    public void execute() {

        int numthreads = m_options.getThreadCount();
        try {
            Thread[] threads = new Thread[numthreads];
            for (int i = 0; i < numthreads; i++) {
                MicroBenchmarkPartition p = new MicroBenchmarkPartition(m_startbarrier, m_stopbarrier, this, i);
                m_partitions[i] = p;
                threads[i] = new Thread(p);
                threads[i].start();
            }
            for (int i = 0; i < numthreads; i++)
                threads[i].join();
            m_exec = getEndToEndLatency(m_partitions);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * report basic runtime, defer to subclass for
     * finer-grain information
     */
    public String report(boolean bShowHeaders) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("benchmark took %d ms\n", msec(m_exec)));
        reportImpl(sb, bShowHeaders);
        return sb.toString();
    }

    public abstract void initialize();
    public abstract void executeImpl(int nStartIdx, int nItems);
    public abstract void finalize();
    public abstract void reportImpl(StringBuilder sb, boolean bShowHeaders);

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        Options options = Options.getOptions(args);
        if(options == null) return;

        MicroBenchmark mb = getBenchmark(options);
        mb.initialize();
        mb.execute();
        mb.finalize();
        System.err.println(mb.report(true));
        System.exit(0);
    }

    /**
     * console logging for verbose mode.
     * @param strFormat
     * @param args
     */
    protected static void
    inform(
            String strFormat,
            Object... args
        )
    {
        if(s_verbose)
            System.out.format(strFormat, args);
    }


    /**
     * getEndToEndLatency
     * Execution time for the benchmark is the time delta between when the
     * first tester thread enters its transaction phase and when the last
     * thread exits the same. The run method tracks these per object.
     * @param testers
     * @return
     */
    static long
    getEndToEndLatency(MicroBenchmarkPartition[] testers) {
        long startmin = Long.MAX_VALUE;
        long endmax = Long.MIN_VALUE;
        for(MicroBenchmarkPartition tester : testers) {
            startmin = Math.min(startmin, tester.m_start);
            endmax = Math.max(endmax, tester.m_end);
        }
        return endmax - startmin;
    }

    // utilities...
    protected boolean BeginTX() { if(m_options.getUseTransactions()) { m_rt.BeginTX(); return true; } return false; }
    protected boolean EndTX() { if(m_options.getUseTransactions()) return m_rt.EndTX(); return true; }
    protected boolean AbortTX(boolean intx) { if(intx) m_rt.AbortTX(); return intx; }
    protected static long curtime() { return System.nanoTime(); }
    protected static long deltaUS(long start, long end) { return usec(end - start); }
    protected static long deltaMS(long start, long end) { return msec(end - start); }
    protected static long deltaSec(long start, long end) { return sec(end - start); }
    protected static long usec(long delta) { return (long) ((double)(delta) / 1000.0); }
    protected static long msec(long delta) { return (long) ((double)(delta) / 1000000.0); }
    protected static long sec(long delta) { return (long) ((double)(delta) / 1000000000.0); }
    public interface BenchmarkFactory { MicroBenchmark instantiate(Options options); }
    public static void registerBenchmark(String name, BenchmarkFactory factory) { s_bncs.put(name, factory); }

}
