package org.corfudb.integration;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.view.Layout;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rmichoud on 2/12/17.
 */
public class SealIT extends AbstractIT{
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

        Long beforeAddress = cr2.getSequencerView().nextToken(new HashSet<>(),1).getToken().getTokenValue();

        /* We will trigger a Paxos round, this is what will happen:
         *   1. Set our layout (same than before) with a new Epoch
         *   2. Seal the server(s)
         *   3. Propose the new layout by driving paxos.
         */

        Layout currentLayout = new Layout(cr1.getLayoutView().getCurrentLayout());
        currentLayout.setRuntime(cr1);
        /* 1 */
        currentLayout.setEpoch(currentLayout.getEpoch() + 1);
        /* 2 */
        currentLayout.moveServersToEpoch();
        /* 3 */
        cr1.getLayoutView().updateLayout(currentLayout, 0);

        cr1.invalidateLayout();
        cr1.layout.get();

        cr1.getRouter(corfuSingleNodeHost + ":" + corfuSingleNodePort)
                .getClient(SequencerClient.class)
                .bootstrap(1L, Collections.EMPTY_MAP, currentLayout.getEpoch())
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
        Long afterAddress = cr2.getSequencerView().nextToken(new HashSet<>(),1).getToken().getTokenValue();
        assertThat(cr2.getLayoutView().getCurrentLayout().getEpoch()).
            isEqualTo(cr1.getLayoutView().getCurrentLayout().getEpoch());
        assertThat(afterAddress).isEqualTo(beforeAddress+1);

        assertThat(shutdownCorfuServer(corfuProcess)).isTrue();
    }
}
