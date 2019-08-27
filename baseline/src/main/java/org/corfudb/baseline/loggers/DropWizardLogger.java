package org.corfudb.baseline.loggers;

import com.codahale.metrics.MetricRegistry;
import lombok.Getter;
import org.corfudb.baseline.StatsLogger;
import org.corfudb.baseline.metrics.Counter;
import org.corfudb.baseline.metrics.Gauge;
import org.corfudb.baseline.metrics.Timer;

import static com.codahale.metrics.MetricRegistry.name;

public class DropWizardLogger implements StatsLogger {
    @Getter
    private final MetricRegistry metricRegistry;
    private final String loggerName;

    public DropWizardLogger(String name, MetricRegistry registry) {
        this.loggerName = name;
        this.metricRegistry = registry;
    }

    @Override
    public Counter getCounter(String counterName) {
        String metricName = name(this.loggerName, counterName);
        com.codahale.metrics.Counter counter = metricRegistry.counter(metricName);
        return new Counter() {
            @Override
            public void inc() {
                counter.inc();
            }

            @Override
            public void dec() {
                counter.dec();
            }

            @Override
            public long getCount() {
                return counter.getCount();
            }
        };
    }

    class TimerContext implements Timer.Context {
        final com.codahale.metrics.Timer.Context context;
        TimerContext(com.codahale.metrics.Timer timer) {
            this.context = timer.time();
        }

        @Override
        public void stop() {
            context.stop();
        }

        @Override
        public void close() {
            context.stop();
        }
    }

    @Override
    public Timer getTimer(String timerName) {
        String metricName = name(this.loggerName, timerName);
        com.codahale.metrics.Timer timer = metricRegistry.timer(metricName);
        return new Timer() {
            @Override
            public Context getContext() {
                return new TimerContext(timer);
            }
        };
    }

    @Override
    public <T extends Number> void registerGauge(String gaugeName, Gauge<T> gauge) {
        String metricName = name(this.loggerName, gaugeName);
        metricRegistry.register(metricName, new com.codahale.metrics.Gauge<T>() {
            @Override
            public T getValue() {
                return gauge.getValue();
            }
        });
    }

    @Override
    public StatsLogger scope(String name) {
        return new DropWizardLogger(name(this.loggerName, name), this.metricRegistry);
    }
}
