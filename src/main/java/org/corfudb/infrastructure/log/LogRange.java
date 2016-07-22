package org.corfudb.infrastructure.log;

import lombok.Data;

/**
 * Created by maithem on 7/20/16.
 */

@Data
public class LogRange {
    public final long start;
    public final long end;
}
