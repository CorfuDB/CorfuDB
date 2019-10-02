package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.test.CorfuServerRunner;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

/**
 * Created by rmichoud on 2/12/17.
 */
public class SealIT extends AbstractIT {

    static String corfuSingleNodeHost;
    static int corfuSingleNodePort;

    @Before
    public void loadProperties() {
        corfuSingleNodeHost = (String) PROPERTIES.get("corfuSingleNodeHost");
        corfuSingleNodePort = Integer.parseInt((String) PROPERTIES.get("corfuSingleNodePort"));
    }

    @Test
    public void RuntimeWithWrongEpochGetUpdated() throws Exception {
        Process corfuProcess = new CorfuServerRunner()
                .setHost(corfuSingleNodeHost)
                .setPort(corfuSingleNodePort)
                .runServer();
        CorfuRuntime cr1 = createDefaultRuntime();
        CorfuRuntime cr2 = createDefaultRuntime();

        long beforeAddress = cr2.getSequencerView().next().getToken().getSequence();

        /* We will trigger a Paxos round, this is what will happen:
         *   1. Set our layout (same than before) with a new Epoch
         *   2. Seal the server(s)
         *   3. Propose the new layout by driving paxos.
         */

        Layout currentLayout = new Layout(cr1.getLayoutView().getCurrentLayout());
        /* 1 */
        currentLayout.setEpoch(currentLayout.getEpoch() + 1);
        /* 2 */
        cr1.getLayoutView().getRuntimeLayout(currentLayout).sealMinServerSet();
        /* 3 */
        cr1.getLayoutView().updateLayout(currentLayout, 0);

        cr1.invalidateLayout();
        cr1.getLayoutView().getLayout();

        cr1.getLayoutView().getRuntimeLayout()
                .getSequencerClient(corfuSingleNodeHost + ":" + corfuSingleNodePort)
                .bootstrap(1L, Collections.emptyMap(), currentLayout.getEpoch(), true)
                .get();


        /* Now cr2 is still stuck in the previous epoch. The next time it will ask for a token,
         * emit a read or a write (msgs type that validate the epoch on the server side) it should
         * receive a WrongEpochException. This exception is taken care of by the AbstractView class in
         * the layoutHelper function.
         *
         * Upon receiving a wrong epoch, the new epoch will be set internally, and it will invalidate
         * the layout.
         *
         * These steps get cr2 in the new epoch.
         */
        Long afterAddress = cr2.getSequencerView().next().getToken().getSequence();
        assertThat(cr2.getLayoutView().getCurrentLayout().getEpoch()).
            isEqualTo(cr1.getLayoutView().getCurrentLayout().getEpoch());
        assertThat(afterAddress).isEqualTo(beforeAddress+1);

        assertThat(shutdownCorfuServer(corfuProcess)).isTrue();

        cr1.shutdown();
        cr2.shutdown();
    }
}
