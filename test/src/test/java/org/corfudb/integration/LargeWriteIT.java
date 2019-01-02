package org.corfudb.integration;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A set integration tests that exercise failure modes related to
 * large writes.
 */

public class LargeWriteIT  {

    @Test
    public void largeStreamWrite() throws Exception {

        final String streamName = "s1";
        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();
        CorfuRuntime rt2 = new CorfuRuntime("localhost:9000").connect();

        Map<String, String> map = rt.getObjectsView().build().setStreamName("s1").setType(CorfuTable.class).open();
        Map<String, String> map2 = rt2.getObjectsView().build().setStreamName("s1").setType(CorfuTable.class).open();

        for (;;) {
            map.put("s1", "S2");
            Thread.sleep(3000);
        }
    }
}
