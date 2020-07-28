package org.corfudb.common.metrics;

import lombok.Getter;

public class DoubleGauge implements Gauge<Double> {

    public interface GaugeValue {
        double get();
    }

    @Getter
    private final String name;

    private final GaugeValue gaugeValue;

    public DoubleGauge(String name, GaugeValue gaugeValue) {
        this.name = name;
        this.gaugeValue = gaugeValue;
    }

    @Override
    public Double getValue() {
        return gaugeValue.get();
    }

}
