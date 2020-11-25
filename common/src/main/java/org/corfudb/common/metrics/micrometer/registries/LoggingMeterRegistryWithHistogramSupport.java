package org.corfudb.common.metrics.micrometer.registries;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import io.micrometer.core.instrument.util.TimeUtils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.micrometer.core.instrument.util.DoubleFormat.decimalOrNan;
import static java.util.stream.Collectors.joining;

/**
 * The source code is copied from LoggingMeterRegistry with some additional modification added:
 * - Influx logging printer.
 * - Enable percentiles and histograms.
 */
public class LoggingMeterRegistryWithHistogramSupport extends StepMeterRegistry {
    private final LoggingRegistryConfig config;
    private final Consumer<String> loggingSink;
    private final Function<Meter, String> meterIdPrinter;

    public LoggingMeterRegistryWithHistogramSupport(LoggingRegistryConfig config, Consumer<String> loggingSink) {
        this(config, loggingSink, DataProtocol.DEFAULT);
    }

    public LoggingMeterRegistryWithHistogramSupport(LoggingRegistryConfig config,
                                                    Consumer<String> loggingSink,
                                                    DataProtocol dataProtocol) {
        super(config, Clock.SYSTEM);
        this.config = config;
        this.loggingSink = loggingSink;
        this.meterIdPrinter = getPrinter(dataProtocol);
        config().namingConvention(NamingConvention.dot);
        start(new NamedThreadFactory("logging-metrics-publisher"));
    }

    public enum DataProtocol {
        DEFAULT,
        INFLUX,
    }

    private Function<Meter, String> getPrinter(DataProtocol dataProtocol) {
        switch (dataProtocol) {
            case INFLUX:
                return influxMeterIdPrinter();
            case DEFAULT:
                return defaultMeterIdPrinter();
            default:
                return defaultMeterIdPrinter();
        }
    }

    private Function<Meter, String> defaultMeterIdPrinter() {
        return (meter) -> getConventionName(meter.getId()) + getConventionTags(meter.getId()).stream()
                .map(t -> t.getKey() + "=" + t.getValue())
                .collect(joining(",", "{", "}"));
    }

    private Function<Meter, String> influxMeterIdPrinter() {
        return meter -> getConventionName(meter.getId()) + "," + getConventionTags(meter.getId()).stream()
                .map(t -> t.getKey() + "=" + t.getValue()).collect(Collectors.joining(","));
    }

    @Override
    protected void publish() {
        if (config.enabled()) {
            getMeters().stream()
                    .sorted((m1, m2) -> {
                        int typeComp = m1.getId().getType().compareTo(m2.getId().getType());
                        if (typeComp == 0) {
                            return m1.getId().getName().compareTo(m2.getId().getName());
                        }
                        return typeComp;
                    })
                    .forEach(m -> {
                        Printer print = new Printer(m);
                        m.use(
                                gauge -> {
                                    loggingSink.accept(print.id() + " value=" + print.value(gauge.value()));
                                },
                                counter -> {
                                    double count = counter.count();
                                    if (!config.logInactive() && count == 0) return;
                                    loggingSink.accept(print.id() + " throughput=" + print.rate(count));
                                },
                                timer -> {
                                    HistogramSnapshot snapshot = timer.takeSnapshot();
                                    long count = snapshot.count();
                                    if (!config.logInactive() && count == 0) return;
                                    loggingSink.accept(print.id() + " throughput=" + print.unitlessRate(count) +
                                            " mean=" + print.time(snapshot.mean(getBaseTimeUnit())) +
                                            " max=" + print.time(snapshot.max(getBaseTimeUnit())));
                                },
                                summary -> {
                                    HistogramSnapshot snapshot = summary.takeSnapshot();
                                    long count = snapshot.count();
                                    if (!config.logInactive() && count == 0) return;
                                    loggingSink.accept(print.id() + " throughput=" + print.unitlessRate(count) +
                                            " mean=" + print.value(snapshot.mean()) +
                                            " max=" + print.value(snapshot.max()));
                                },
                                longTaskTimer -> {
                                    int activeTasks = longTaskTimer.activeTasks();
                                    if (!config.logInactive() && activeTasks == 0) return;
                                    loggingSink.accept(print.id() +
                                            " active=" + print.value(activeTasks) +
                                            " duration=" + print.time(longTaskTimer.duration(getBaseTimeUnit())));
                                },
                                timeGauge -> {
                                    double value = timeGauge.value(getBaseTimeUnit());
                                    if (!config.logInactive() && value == 0) return;
                                    loggingSink.accept(print.id() + " value=" + print.time(value));
                                },
                                counter -> {
                                    double count = counter.count();
                                    if (!config.logInactive() && count == 0) return;
                                    loggingSink.accept(print.id() + " throughput=" + print.rate(count));
                                },
                                timer -> {
                                    double count = timer.count();
                                    if (!config.logInactive() && count == 0) return;
                                    loggingSink.accept(print.id() + " throughput=" + print.rate(count) +
                                            " mean=" + print.time(timer.mean(getBaseTimeUnit())));
                                },
                                meter -> loggingSink.accept(writeMeter(meter, print))
                        );
                    });
        }
    }

    String writeMeter(Meter meter, Printer print) {
        return StreamSupport.stream(meter.measure().spliterator(), false)
                .map(ms -> {
                    String msLine = ms.getStatistic().getTagValueRepresentation() + "=";
                    switch (ms.getStatistic()) {
                        case TOTAL:
                        case MAX:
                        case VALUE:
                            return msLine + print.value(ms.getValue());
                        case TOTAL_TIME:
                        case DURATION:
                            return msLine + print.time(ms.getValue());
                        case COUNT:
                            return "throughput=" + print.rate(ms.getValue());
                        default:
                            return msLine + decimalOrNan(ms.getValue());
                    }
                })
                .collect(joining(", ", print.id() + " ", ""));
    }

    @Override
    protected TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    class Printer {
        private final Meter meter;

        Printer(Meter meter) {
            this.meter = meter;
        }

        String id() {
            return meterIdPrinter.apply(meter);
        }

        String time(double time) {
            return TimeUtils.format(Duration.ofNanos((long) TimeUtils.convert(time, getBaseTimeUnit(), TimeUnit.NANOSECONDS)));
        }

        String rate(double rate) {
            return humanReadableBaseUnit(rate / (double) config.step().getSeconds()) + "/s";
        }

        String unitlessRate(double rate) {
            return decimalOrNan(rate / (double) config.step().getSeconds()) + "/s";
        }

        String value(double value) {
            return humanReadableBaseUnit(value);
        }

        // see https://stackoverflow.com/a/3758880/510017
        String humanReadableByteCount(double bytes) {
            int unit = 1024;
            if (bytes < unit || Double.isNaN(bytes)) return decimalOrNan(bytes) + " B";
            int exp = (int) (Math.log(bytes) / Math.log(unit));
            String pre = "KMGTPE".charAt(exp - 1) + "i";
            return decimalOrNan(bytes / Math.pow(unit, exp)) + " " + pre + "B";
        }

        String humanReadableBaseUnit(double value) {
            String baseUnit = meter.getId().getBaseUnit();
            if (BaseUnits.BYTES.equals(baseUnit)) {
                return humanReadableByteCount(value);
            }
            return decimalOrNan(value) + (baseUnit != null ? " " + baseUnit : "");
        }
    }
}
