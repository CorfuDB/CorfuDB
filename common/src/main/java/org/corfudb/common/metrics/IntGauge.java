package org.corfudb.common.metrics;

import lombok.Getter;

public class IntGauge implements Gauge<Integer> {

    public interface GaugeValue {
        int get();
    }

    @Getter
    private final String name;

    private final GaugeValue gaugeValue;

    public IntGauge(String name, GaugeValue gaugeValue) {
        this.name = name;
        this.gaugeValue = gaugeValue;
    }

    @Override
    public Integer getValue() {
        return gaugeValue.get();
    }
}
