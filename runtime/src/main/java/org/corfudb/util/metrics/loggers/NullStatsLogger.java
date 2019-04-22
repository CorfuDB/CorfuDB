package org.corfudb.util.metrics.loggers;

import org.corfudb.util.metrics.Counter;
import org.corfudb.util.metrics.Gauge;
import org.corfudb.util.metrics.StatsLogger;
import org.corfudb.util.metrics.Timer;

public class NullStatsLogger implements StatsLogger {

    public static NullStatsLogger INSTANCE = new NullStatsLogger();

    private static NullCounter nullCounter = new NullCounter();
    private static NullTimer nullTimer = new NullTimer();
    private static NullTimeContext nullTimeContext = new NullTimeContext();

    @Override
    public Counter getCounter(String name) {
        return nullCounter;
    }

    @Override
    public Timer getTimer(String name) {
        return nullTimer;
    }

    @Override
    public StatsLogger scope(String name) {
        return this;
    }

    @Override
    public <T extends Number> void registerGauge(String name, Gauge<T> gauge) {
    }

    static class NullTimeContext implements Timer.Context {
        @Override
        public void stop() {
        }

        @Override
        public void close() {
        }
    }

    static class NullTimer implements Timer {

        @Override
        public Timer.Context getContext() {
            return nullTimeContext;
        }
    }

    static class NullCounter implements Counter {
        @Override
        public void inc() {
        }

        @Override
        public void dec() {
        }

        @Override
        public long getCount() {
            return 0;
        }
    }
}
