package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/6/16.
 */
public class LayoutViewTest extends AbstractViewTest {
    @Test
    public void canGetLayout() {
        CorfuRuntime r = getDefaultRuntime().connect();
        Layout l = r.getLayoutView().getCurrentLayout();
        assertThat(l.asJSONString())
                .isNotNull();
    }

    @Test
    public void canSetLayout()
            throws Exception {
        CorfuRuntime r = getDefaultRuntime().connect();
        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addSequencer(9000)
                .buildSegment()
                    .buildStripe()
                        .addLogUnit(9000)
                    .addToSegment()
                .addToLayout()
                .build();
        l.setRuntime(r);
        r.getLayoutView().updateLayout(l, 1L);
        r.invalidateLayout();
        assertThat(r.getLayoutView().getLayout().epoch)
                .isEqualTo(1L);
    }

    @Test
    public void canTolerateLayoutServerFailure()
            throws Exception {
        addServer(9000);
        addServer(9001);

        bootstrapAllServers(new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addLayoutServer(9001)
                .addSequencer(9000)
                .buildSegment()
                    .buildStripe()
                        .addLogUnit(9000)
                    .addToSegment()
                .addToLayout()
                .build());

        CorfuRuntime r = getRuntime().connect();

        // Fail the network link between the client and test server
        addServerRule(9001, new TestRule()
                            .always()
                            .drop());

        r.invalidateLayout();

        r.getStreamsView().get(CorfuRuntime.getStreamID("hi")).check();
    }

    @Test
    public void canAvoidNullException()
            throws Exception {
        CorfuRuntime r = getDefaultRuntime().connect();
        System.err.printf("\nYo 1\n");
        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addSequencer(9000)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addToSegment()
                .addToLayout()
                .build();
        System.err.printf("\nYo 2\n");
        l.setRuntime(r);
        System.err.printf("\nYo 3\n");
        r.getLayoutView().updateLayout(l, 1L);
        System.err.printf("\nYo 4\n");

        Layout l2 = new TestLayoutBuilder()
                .setEpoch(2L)
                .addLayoutServer(9000)
                .addSequencer(9000)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addToSegment()
                .addToLayout()
                .build();
        System.err.printf("\nYo 5a\n");
        System.err.printf("getLayout qq1 = %s\n", r.getLayoutView().getLayout());
        r.invalidateLayout();
        try {
            System.err.printf("getLayout qq2 = %s\n", r.getLayoutView().getLayout());
            r.getLayoutView().updateLayout(l2, 1L);
        } finally {
            System.err.printf("\nYo5zzzz\n");
        }
        try {
            System.err.printf("\nYo 6\n");
            // Bug: second attempt to call updateLayout() with same layout fails.  Bug fixed?
            r.getLayoutView().updateLayout(l2, 1L);
        } finally {
            System.err.printf("\nYo 7\n");
        }
    }

}
