package org.corfudb.infrastructure.logreplication.PgUtils;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.utils.lock.LockDataTypes.LockId;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class PgLeadershipService {
    private static final long CHECK_INTERVAL_MS = 5000;

    private final AtomicBoolean isRunning;
    private final AtomicBoolean isLeader;
    private final PostgresConnector postgresConnector;

    private final PgLeadershipCallback pgLeadershipCallback;
    private Thread monitorThread;

    public PgLeadershipService(PostgresConnector postgresConnector, PgLeadershipCallback pgLeadershipCallback) {
        this.isRunning = new AtomicBoolean(false);
        this.isLeader = new AtomicBoolean(false);
        this.postgresConnector = postgresConnector;
        this.pgLeadershipCallback = pgLeadershipCallback;

        evaluateLeadership();
    }

    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            monitorThread = new Thread(this::evaluateLeadership, "pg-leadership-monitor");
            monitorThread.setDaemon(true);
            monitorThread.start();
        }
    }

    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            try {
                if (monitorThread != null) {
                    monitorThread.interrupt();
                    monitorThread.join();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            log.info("Stopped pg leadership monitoring.");
        }

    }

    private void evaluateLeadership() {
        while (isRunning.get() && !Thread.currentThread().isInterrupted()) {
            try {
                if (getLocalPostgresInRecovery() && isLeader.get()) {
                    isLeader.set(false);
                    log.info("Local postgres has lost leadership.");
                    pgLeadershipCallback.lockRevoked(LockId.newBuilder().build());
                } else if (!getLocalPostgresInRecovery() && !isLeader.get()) {
                    isLeader.set(true);
                    log.info("Local postgres has acquired leadership.");
                    pgLeadershipCallback.lockAcquired(LockId.newBuilder().build());
                } else if (!getLocalPostgresInRecovery() && isLeader.get()) {
                    log.info("PG Leadership Re-verified.");
                    pgLeadershipCallback.lockRenewed();
                }

                TimeUnit.MILLISECONDS.sleep(CHECK_INTERVAL_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                try {
                    TimeUnit.MILLISECONDS.sleep(CHECK_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private boolean getLocalPostgresInRecovery() {
        return PostgresUtils.getPostgresInRecovery(postgresConnector);
    }
}
