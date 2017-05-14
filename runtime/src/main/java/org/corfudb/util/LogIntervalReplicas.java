package org.corfudb.util;

import com.brein.time.timeintervals.intervals.IInterval;

import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * Store sets of log intervals that are stored by a replica set.
 */

@AllArgsConstructor
public class LogIntervalReplicas {
    @Getter
    Set<IInterval<Long>> logIntervalSet;
    @Getter
    Set<String> replicaSet;
}
