package org.corfudb.util.metrics.loggers;

import com.codahale.metrics.MetricRegistry;
import lombok.Getter;
import org.corfudb.util.metrics.Counter;
import org.corfudb.util.metrics.Gauge;
import org.corfudb.util.metrics.StatsLogger;
import org.corfudb.util.metrics.Timer;

import static com.codahale.metrics.MetricRegistry.name;

public class CodeHaleLogger implements StatsLogger {

    @Getter
    final private MetricRegistry registry;
    final private String name;

    public CodeHaleLogger(String name, MetricRegistry registry) {
        this.name = name;
        this.registry = registry;
    }

    @Override
    public Counter getCounter(String counterName) {
        com.codahale.metrics.Counter counter = registry.counter(name(name, counterName));
        return new Counter() {
            @Override
            public long getCount() {
                return counter.getCount();
            }

            @Override
            public void inc() {
                counter.inc();
            }

            @Override
            public void dec() {
                counter.dec();
            }
        };
    }

    class TimerContext implements Timer.Context {
        final com.codahale.metrics.Timer.Context context;
        public TimerContext(com.codahale.metrics.Timer timer) {
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
        com.codahale.metrics.Timer timer = registry.timer(name(name, timerName));
        return new Timer() {
            @Override
            public Context getContext() {
                return new TimerContext(timer);
            }
        };

    }

    @Override
    public StatsLogger scope(String childLoggerName) {
        return new CodeHaleLogger(name(this.name, childLoggerName), registry);
    }

    @Override
    public <T extends Number> void registerGauge(String gaugeName, Gauge<T> gauge) {
        String metricName = name(name, gaugeName);
        registry.register(metricName, new com.codahale.metrics.Gauge<T>() {
            @Override
            public T getValue() {
                return gauge.getValue();
            }
        });
    }
}
