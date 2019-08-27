package org.corfudb.baseline.providers;

import org.corfudb.baseline.IProvider;
import org.corfudb.baseline.StatsLogger;
import org.corfudb.baseline.loggers.NullStatsLogger;

public class NullProvider implements IProvider {
    public static IProvider INSTANCE = new NullProvider();

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
