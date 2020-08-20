package org.corfudb.common.stats;

/**
 *
 * Created by Maithem on 7/8/20.
 */
public interface Gauge<T> extends Metric {

    T getValue();
}