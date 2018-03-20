package org.corfudb.logReader;

import lombok.Getter;
import org.corfudb.format.Types.LogEntry;

/**
 * Created by kjames88 on 3/2/17.
 */

public class LogEntryExtended {
    LogEntryExtended(final LogEntry entry, final int length, final int cksum) {
        entryBody = entry;
        bytesLength = length;
        checksum = cksum;
    }
    @Getter
    private LogEntry entryBody;
    @Getter
    private int bytesLength;
    @Getter
    private int checksum;
}
