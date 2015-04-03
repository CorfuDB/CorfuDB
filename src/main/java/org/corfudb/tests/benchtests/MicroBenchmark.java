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
import java.util.List;

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
    static final String DEFAULT_TEST = "basic";
    static final int DEFAULT_OPERATION_COUNT = 100;
    static final String DEFAULT_STREAM_IMPL = "DUMMY";
    static final String DEFAULT_RUNTIME = "TXRuntime";
    static final boolean DEFAULT_READ_YOUR_WRITES = true;
    static final boolean DEFAULT_OPAQUE = true;
    static List<String> s_tests = new ArrayList<String>();
    static {
        s_tests.add("basic");
    }

    protected Options m_options;
    protected AbstractRuntime m_rt;
    protected IStreamFactory m_sf;
    protected CorfuDBClient m_client;
    protected long m_start;
    protected long m_end;

    public static class OpaqueObject extends CorfuDBObject {
        byte[] m_payload;
        public OpaqueObject(AbstractRuntime tTR, long oid, int payloadBytes) {
            super(tTR, oid);
            m_payload = new byte[payloadBytes];
        }
        public void applyToObject(Object o, ITimestamp timestamp) {
            OpaqueCommand oc = (OpaqueCommand) o;
            int max = Math.min(oc.m_payload.length, m_payload.length);
            if(oc.m_read) {
                for(int i=0; i<max; i++)
                    m_payload[i] = oc.m_payload[i];
            } else {
                for(int i=0; i<max; i++)
                    oc.m_payload[i] = m_payload[i];
            }
            oc.setReturnValue(max);
        }
    }

    public static class OpaqueCommand extends CorfuDBObjectCommand {
        boolean m_read;
        byte[] m_payload;
        public OpaqueCommand(boolean isRead, int payloadBytes) {
            m_read = isRead;
            m_payload = new byte[payloadBytes];
        }
    }

    public static class BasicMicroBenchmark extends MicroBenchmark {
        OpaqueObject[] m_objects;
        public BasicMicroBenchmark(Options options) {
            super(options);
        }
        public void initialize() {
            m_objects = new OpaqueObject[m_options.getObjectCount()];
            for(int i=0; i<m_options.getObjectCount(); i++) {
                m_objects[i] = new OpaqueObject(m_rt, DirectoryService.getUniqueID(m_sf), m_options.getObjectSize());
            }
        }
        public void executeImpl() {
            for(int i=0; i<m_options.getOperationCount(); i++) {
                int idx = (int) Math.floor(Math.random()*m_objects.length);
                if(m_options.getUseTransactions())
                    m_rt.BeginTX();
                OpaqueObject o = m_objects[idx];
                OpaqueCommand cmd = new OpaqueCommand(Math.random() < m_options.getRWRatio(), m_options.getCommandSize());
                if(cmd.m_read)
                    m_rt.query_helper(o, null, cmd);
                else
                    m_rt.update_helper(o, cmd);
                if(m_options.getUseTransactions())
                    m_rt.EndTX();
            }
        }
        public void reportImpl() {}
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
            System.out.println("supported test scenarios:");
            for (String s : s_tests) {
                System.out.format("\t%s\n", s);
            }
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
                System.out.println(e.getMessage());
                e.printStackTrace();
                printUsage();
                return null;
            }
            return options;
        }

        /**
         * print the usage
         */
        static void printUsage() {
            System.out.println("usage: java MicroBenchmark");
            System.out.println("\t[-m masternode] (default == \"http://localhost:8002/corfu\"");
            System.out.println("\t[-p rpcport] (default == 9090)");
            System.out.println("\t[-r read write pct (double, default=75% reads)]");
            System.out.println("\t[-o object/stream count] number of distinct objects/streams, (default = 10)");
            System.out.println("\t[-s object size] approximate serialized byte size of object (default = 256)");
            System.out.println("\t[-c command size] approximate serialized size of object commands (default = 16)");
            System.out.println("\t[-x true|false] use/don't use transactions (default is use-tx)");
            System.out.println("\t[-w true|false] support/don't support read-after-write consistency (default is support)");
            System.out.println("\t[-O true|false] support/don't support opaque transactions (default is support)");
            System.out.println("\t[-t number of threads] default == 1");
            System.out.println("\t[-n number of ops]");
            System.out.println("\t[-S DUMMY|HOP] stream implementation--default == DUMMY");
            System.out.println("\t[-R runtime] runtime implementation SIMPLE|TX");
            System.out.println("\t[-L collect latency stats] true|false (default is true) \n");
            System.out.println("\t[-A test scenario] default is 'basic'");
            System.out.println("\t[-l] list available test scenarios");
            System.out.println("\t[-h help] print this message\n");
            System.out.println("\t[-v verbose mode...]");
        }

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
     * get an object that implements a microbenchmark
     * @param options
     * @return
     */
    public static MicroBenchmark getBenchmark(Options options) {
        return null;
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
    }

    /**
     * execute
     */
    public void execute() {
        m_start = System.currentTimeMillis();
        executeImpl();
        m_end = System.currentTimeMillis();
    }

    /**
     * report basic runtime, defer to subclass for
     * finer-grain information
     */
    public void report() {
        System.out.format("benchmark took %d ms\n", m_end - m_start);
        reportImpl();
    }

    public abstract void initialize();
    public abstract void executeImpl();
    public abstract void reportImpl();

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        Options options = Options.getOptions(args);
        if(options == null) return;
        MicroBenchmark mb = getBenchmark(options);
        mb.execute();
        mb.report();
    }
}
