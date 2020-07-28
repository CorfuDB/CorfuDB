package org.corfudb.common.metrics;

import lombok.Getter;

public class LongGauge implements Gauge<Long> {

    public interface GaugeValue {
        long get();
    }

    @Getter
    private final String name;

    private GaugeValue gaugeValue;

    public LongGauge(String name, GaugeValue gaugeValue) {
        this.name = name;
        this.gaugeValue = gaugeValue;
    }

    @Override
    public Long getValue() {
        return gaugeValue.get();
    }
}
