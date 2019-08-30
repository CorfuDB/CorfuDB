package org.corfudb.common.providers;


import org.corfudb.common.Provider;
import org.corfudb.common.StatsLogger;
import org.corfudb.common.loggers.NullStatsLogger;

public class NullProvider implements Provider {
    public static Provider INSTANCE = new NullProvider();

    @Override
    public StatsLogger getLogger(String name) {
        return NullStatsLogger.INSTANCE;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}
