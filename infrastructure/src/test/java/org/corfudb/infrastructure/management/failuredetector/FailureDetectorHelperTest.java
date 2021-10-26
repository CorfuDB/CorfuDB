package org.corfudb.infrastructure.management.failuredetector;

import org.corfudb.infrastructure.LayoutBasedTestHelper;
import org.corfudb.infrastructure.NodeNames;
import org.corfudb.runtime.view.Layout;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class FailureDetectorHelperTest extends LayoutBasedTestHelper {

    @Test
    public void canHandleReconfigurations() {
        Layout layout = buildSimpleLayout();
        FailureDetectorHelper helper = new FailureDetectorHelper(layout, NodeNames.A);
        assertTrue(helper.canHandleReconfigurations());

        helper = new FailureDetectorHelper(layout, NodeNames.NOT_IN_CLUSTER_NODE);
        assertFalse(helper.canHandleReconfigurations());
    }

    @Test
    public void handleReconfigurationsAsyncCompleted() {
        Layout layout = buildSimpleLayout();
        FailureDetectorHelper helper = new FailureDetectorHelper(layout, NodeNames.A);
        Layout layout2 = helper.handleReconfigurationsAsync().join();

        assertEquals(layout, layout2);
    }

    @Test
    public void handleReconfigurationsAsyncError() {
        Layout layout = buildSimpleLayout();
        FailureDetectorHelper helper = new FailureDetectorHelper(layout, NodeNames.NOT_IN_CLUSTER_NODE);
        assertThrows(IllegalStateException.class, () -> {
            try {
                helper.handleReconfigurationsAsync().join();
            } catch (Exception ex) {
                throw ex.getCause();
            }
        });
    }
}
