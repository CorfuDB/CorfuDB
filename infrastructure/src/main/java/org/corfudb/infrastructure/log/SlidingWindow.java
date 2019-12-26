package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SlidingWindow {
    private long cntWindow;
    private long timeWindow;
    private long threshVal; //this is in seconds
    private long startTime = 0;

    @Getter
    ArrayList<Long> intervals;

    public SlidingWindow(long cntWindow, long timeWindow, long threshval) {
        this.cntWindow = cntWindow;
        this.timeWindow = timeWindow;
        this.threshVal = threshval;
        startTime = 0;
        intervals = new ArrayList<>();
    }

    public void update(long value) {
        if (value < threshVal && intervals.size()!= 0) {
            startTime = 0;
            intervals.clear();
        } else {
            if (intervals.size() == 0)
                startTime = System.nanoTime();
            if (intervals.size() == cntWindow)
                intervals.remove (0);
            intervals.add(value);
        }
    }

    public boolean report() {
        long dur = 0;

        if (startTime != 0)
            dur = TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);

        if (intervals.size() == cntWindow || dur >= threshVal) {
            log.warn("Report event interval size {} ==  {} or dur {} >= threshVal.",
                    intervals.size(), cntWindow, dur, threshVal);
            return true;
        }
        else
            return false;
    }
}
