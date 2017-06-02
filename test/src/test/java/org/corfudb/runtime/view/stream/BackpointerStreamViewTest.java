package org.corfudb.runtime.view.stream;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the BackpointerStreamView
 * <p>
 * Created by zlokhandwala on 5/24/17.
 */
public class BackpointerStreamViewTest extends AbstractViewTest {

    /**
     * Tests the hasNext functionality of the streamView.
     */
    @Test
    public void testGetHasNext() {
        CorfuRuntime runtime = getDefaultRuntime();

        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        sv.append("hello world".getBytes());

        assertThat(sv.hasNext()).isTrue();
        sv.next();
        assertThat(sv.hasNext()).isFalse();
    }

    @Test
    public void testReadQueue() {
        CorfuRuntime runtime = getDefaultRuntime();
        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        final int ten = 10;

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++)
            sv.append(String.valueOf(i).getBytes());

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            assertThat(sv.hasNext()).isTrue();
            byte[] payLoad = (byte[]) sv.next().getPayload(runtime);
            assertThat(new String(payLoad).equals(String.valueOf(i)) )
                    .isTrue();
            assertThat(sv.getCurrentGlobalPosition()).isEqualTo(i);

            if (i % ten == 1) {
                for (int j = 0; j < PARAMETERS.NUM_ITERATIONS_VERY_LOW; j++)
                    sv.append(String.valueOf(i).getBytes());

            }
        }

        for (int i = PARAMETERS.NUM_ITERATIONS_LOW-1; i >= 0; i--) {
            byte[] payLoad = (byte[]) sv.current().getPayload(runtime);
            assertThat(new String(payLoad).equals(String.valueOf(i)) )
                    .isTrue();
            assertThat(sv.getCurrentGlobalPosition()).isEqualTo(i);
            sv.previous();

            if (i % ten == 1) {
                for (int j = 0; j < PARAMETERS.NUM_ITERATIONS_VERY_LOW; j++)
                    sv.append(String.valueOf(i).getBytes());

            }
        }
    }
}
