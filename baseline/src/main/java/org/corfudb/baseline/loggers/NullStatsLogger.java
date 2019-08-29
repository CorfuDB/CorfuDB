package org.corfudb.baseline.loggers;

import org.corfudb.baseline.StatsLogger;
import org.corfudb.baseline.metrics.Counter;
import org.corfudb.baseline.metrics.Gauge;
import org.corfudb.baseline.metrics.Timer;

public class NullStatsLogger implements StatsLogger {
    public static final NullStatsLogger INSTANCE = new NullStatsLogger();

    private static final NullCounter nullCounter = new NullCounter();
    private static final NullTimer nullTimer = new NullTimer();
    private static final NullTimeContext nullTimeContext = new NullTimeContext();

    @Override
    public Counter getCounter (String name) {
        return nullCounter;
    }

    @Override
    public Timer getTimer(String name) {
        return nullTimer;
    }

    @Override
    public <T extends Number> void registerGauge(String name, Gauge<T> gauge) {

    }

    @Override
    public StatsLogger scope(String name) {
        return this;
    }

    private static class NullCounter implements Counter {

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

    private static class NullTimeContext implements Timer.Context {
        @Override
        public void stop() {
        }

        @Override
        public void close() {
        }
    }

    private static class NullTimer implements Timer {

        @Override
        public Timer.Context getContext() {
            return nullTimeContext;
        }
    }
}
