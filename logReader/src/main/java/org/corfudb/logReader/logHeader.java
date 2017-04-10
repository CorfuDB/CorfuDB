package org.corfudb.logReader;

import lombok.Getter;
import lombok.Setter;

/**
 * Created by kjames88 on 3/1/17.
 */
public class logHeader {
    logHeader() {
        checksum = 0;
        verifyChecksum = false;
        version = 0;
        length = 0;
    }
    @Setter
    @Getter
    private int checksum;
    @Setter
    @Getter
    private boolean verifyChecksum;
    @Setter
    @Getter
    private int version;
    @Setter
    @Getter
    private int length;
}
