package org.corfudb.util;

import com.brein.time.timeintervals.intervals.IInterval;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * Store state for individual worker threads as they scan
 * small intervals of the global log.
 */

@AllArgsConstructor
public class ScannerWorkStatus {
    // String workerName;        // Name for worker, hopefully unique system-wide but not mandatory
    @Getter
    LocalDateTime startTime;  // Starting time of the worker
    @Getter
    @Setter
    LocalDateTime updateTime; // Time this record was updated
    @Getter
    IInterval<Long> interval; // Interval that we're working on
    @Getter
    @Setter
    long scanCount;           // Total items scanned in this interval
}
