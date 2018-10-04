package org.corfudb.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TokenType;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


@Slf4j
public class Simulator {

    final ThreadFactory workerThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat("Sim-Worker-%d")
            .build();

    final ThreadFactory managerThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat("Sim-manager-%d")
            .build();

    final CorfuRuntime rt;

    final SimulatorConfig config;

    final ExecutorService managerThread;

    final ExecutorService workerThreads;

    final AtomicInteger numRunningTasks;

    volatile boolean running = false;

    final Random rand = new Random();

    //------------Distributions------------//
    final AbstractRealDistribution payloadDist;
    final AbstractRealDistribution keySizeDist;
    final AbstractRealDistribution keysPerTxnDist;
    final AbstractRealDistribution txnPerPeriodDist;
    final AbstractRealDistribution tablesPerTxnDist;
    final AbstractRealDistribution jitterDist;
    final AbstractRealDistribution burstDist;


    public Simulator(CorfuRuntime rt, SimulatorConfig config) {
        this.rt = rt;
        this.config = config;
        managerThread = Executors.newSingleThreadExecutor(managerThreadFactory);
        workerThreads = Executors.newFixedThreadPool(config.getNumThreads(), workerThreadFactory);
        numRunningTasks = new AtomicInteger(0);

        // Initialize Distributions
        payloadDist = new NormalDistribution(config.getMedianTablesPerTxn(), config.getStdWriteSize());
        txnPerPeriodDist = new NormalDistribution(config.getMedianTxnPerPeriod(), config.getStdTxnPerPeriod());
        tablesPerTxnDist = new NormalDistribution(config.getMedianTablesPerTxn(), config.getStdTablesPerTxn());
        jitterDist = new NormalDistribution(5000, 3000);
        burstDist = new NormalDistribution(config.getMedianTxnPerPeriod(), config.getStdTxnPerPeriod());
        keysPerTxnDist = new NormalDistribution(config.getMedianKeysPerTxn(), config.getStdKeysPerTxn());
        keySizeDist = new NormalDistribution(config.getMedianKeySize(), config.getStdKeySize());
    }

    /**
     * Samples a payload
     */
    private byte[] samplePayload() {
        int payloadSize = Math.max((int) payloadDist.sample(), config.getMinWriteSize());
        return new byte[payloadSize];
    }

    /**
     *  Samples a key space
     */
    private byte[] sampleKey(int x) {
        int size = Math.max((int) keySizeDist.sample(), config.getMinKeysPerTxn());
        byte[] bytes = new byte[size + x];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }

    /**
     * Samples number of keys in a transaction
     */
    private int sampleNumKeys() {
        return Math.max((int) keysPerTxnDist.sample(), config.getMinKeysPerTxn());
    }

    /**
     * Samples the number of transactions per burst
     */
    private int sampleBurstSize() {
        return Math.max((int) burstDist.sample(), config.getMinTxnPerPeriod());
    }

    /**
     * Samples RPC latency-jitter
     */
    private long sampleJitter() {
        return Math.max(0, (long) jitterDist.sample());
    }

    /**
     * Samples the number of tables to touch in a transaction
     */
    private int sampleNumTables() {
        int numTables = Math.max((int) tablesPerTxnDist.sample(), config.getMinTablesPerTxn());
        return numTables;
    }

    /**
     * Fisher–Yates shuffle to sample without replacement.
     * This is needed because a transaction can't include
     * the same table twice in a single TxResolutionInfo
     * instance.
     * @param tables
     */
    private void shuffle(Table[] tables) {
        int index;
        for (int i = tables.length - 1; i > 0; i--) {
            index = rand.nextInt(i + 1);
            if (index != i) {
                Table temp = tables[index];
                tables[index] = tables[i];
                tables[i] = temp;
            }
        }
    }

    /**
     * Samples the table space
     */
    private Table[] sampleTables(Table[] tables) {
        shuffle(tables);
        int numTables = sampleNumTables();
        Table[] sample = new Table[numTables];
        for (int x = 0; x < numTables; x++) {
            sample[x] = tables[x];
        }
        return sample;
    }

    /**
     * Will start a manager thread that will initialize and run the simulation.
     */
    public synchronized void start() {
        if (running) {
            log.warn("Manager is already running!");
            return;
        }

        managerThread.submit(() -> {
            try {
                manager();
            } catch (Exception e) {
                log.error("Manager Failure", e);
            }
        });
        running = true;
    }

    public synchronized void stop() {
        managerThread.shutdownNow();
        workerThreads.shutdownNow();
        log.info("Stopping simulation");
    }

    /**
     * Create and initialize the tables.
     */
    private Table[] initializeTables(int n) {
        Table[] tables = new Table[n];
        StreamOptions options = new StreamOptions(true);
        for (int x = 0; x < n; x++) {
            IStreamView sv = rt.getStreamsView().get(UUID.randomUUID(), options);
            tables[x] = new Table(sv);
        }
        return tables;
    }

    /**
     * The managing logic that generates and forks tasks for the workers.
     * @throws Exception
     */
    private void manager() throws Exception {
        Table[] allTables = initializeTables(config.getNumTables());

        while (true) {
            int currentBurst = sampleBurstSize();
            int tasksToFork = currentBurst - numRunningTasks.get();

            if (tasksToFork <= 0) {
                log.info("Old tasks still running, not forking any new work...");
                continue;
            }

            log.info("Forking {} tasks", tasksToFork);

            for (int x = 0; x < tasksToFork; x++) {
                Table[] sampleTables = sampleTables(allTables);
                workerThreads.submit(() -> {
                    numRunningTasks.incrementAndGet();
                    try {
                        executeTransaction(sampleTables);
                    } finally {
                        numRunningTasks.decrementAndGet();
                    }
                });
            }

            log.info("Executed {} tasks for the current burst, sleeping for {}", tasksToFork, config.getBurstPeriod());
            Sleep.MILLISECONDS.sleepUninterruptibly(config.getBurstPeriod());
        }
    }

    /**
     * Executes a random transaction on a set of tables
     * @param tables The tables to execute the transaction on
     */
    private void executeTransaction(Table ... tables) {
        log.debug("Executing transactions on {} tables", tables.length);
        // Acquire global timestamp
        long timestamp = rt.getSequencerView().query().getTokenValue();

        // Sync table to simulate tail-reads
        for (Table table : tables) {
            table.sync(timestamp);
        }

        // Increment the tails of all streams and acquire a
        // transaction token to write
        UUID[] streamIds = getIds(tables);
        TxResolutionInfo txResolutionInfo = getTxResolutionInfo(timestamp, streamIds);
        TokenResponse tokenResponse = rt.getSequencerView().next(txResolutionInfo, streamIds);

        // Construct a payload
        byte[] payload = samplePayload();

        if (tokenResponse.getRespType() != TokenType.NORMAL) {
            log.error("Couldn't execute transaction, token {}", tokenResponse);
            return;
        }

        // Write transaction
        rt.getAddressSpaceView().write(tokenResponse, payload);
    }

    /**
     * Generates a fake transaction by randomly generating keys and conflict sets
     * @param timestamp timestamp of the transaction when it started
     * @param ids ids of the tables in this transaction
     * @return
     */
    private TxResolutionInfo getTxResolutionInfo(long timestamp, UUID ... ids) {
        UUID txnId = UUID.randomUUID();
        Map<UUID, Set<byte[]>> conflictParam = new HashMap<>();
        Map<UUID, Set<byte[]>> writeConflicts = new HashMap<>();
        for (UUID streamId : ids) {
            Set<byte[]> writeSet = new HashSet<>();
            int numKeysInTxn = sampleNumKeys();
            // Generate fake keys for this transaction
            for (int x = 0 ; x < numKeysInTxn; x++) {
                writeSet.add(sampleKey(x));
            }

            writeConflicts.put(streamId, writeSet);
        }
        return new TxResolutionInfo(txnId, timestamp, conflictParam, writeConflicts);
    }

    /**
     * Extract the stream view ids from a list of Tables
     * @param tables tables to extract the ids from
     * @return an array of stream UUIDs
     */
    private UUID[] getIds(Table ... tables) {
        UUID[] ids = new UUID[tables.length];
        for (int x = 0; x < tables.length; x++) {
            ids[x] = tables[x].getId();
        }
        return ids;
    }

    class Table {
        private final IStreamView streamView;

        private final Lock lock;

        public Table(IStreamView streamView) {
            this.streamView = streamView;
            lock = new ReentrantLock();
        }

        public UUID getId() {
            return streamView.getId();
        }

        /**
         * Sync to a particular time stamp. The stream view is not
         * thread safe, so we need to use a lock, while going back-and-forth
         * in time
         * @param timestamp the time stamp to sync to
         */
        public void sync(long timestamp) {
            lock.lock();
            try {
                long currentPos = streamView.getCurrentGlobalPosition();

                if (timestamp <= currentPos) {
                    streamView.nextUpTo(timestamp);
                } else {
                    while (streamView.getCurrentGlobalPosition() > timestamp) {
                        streamView.previous();
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
