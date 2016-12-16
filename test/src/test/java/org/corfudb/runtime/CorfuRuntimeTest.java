package org.corfudb.runtime;

import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Created by maithem on 6/21/16.
 */
public class CorfuRuntimeTest extends AbstractViewTest {

    @Test
    public void checkValidLayout() throws Exception {

        CorfuRuntime rt = getDefaultRuntime().connect();

        // Check that access to the CorfuRuntime layout is always valid. Specifically, access to the layout
        // while a new layout is being fetched/set concurrently.

        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LARGE, (v) -> {
            rt.invalidateLayout();

        });

        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LARGE, (v) -> {
            assertThat(rt.layout.get().getRuntime()).isEqualTo(rt);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_TWO, PARAMETERS.TIMEOUT_LONG);

    }

    @Test
    public void canInstantiateRuntimeWithoutTestRef() throws Exception {

        addSingleServer(SERVERS.PORT_0);

        CorfuRuntime rt = new CorfuRuntime(SERVERS.ENDPOINT_0);
        rt.connect();

    }
}
