package org.corfudb.common.metrics;

/**
 *
 * Created by Maithem on 7/8/20.
 */
public interface Gauge<T> {
    T getValue();
}