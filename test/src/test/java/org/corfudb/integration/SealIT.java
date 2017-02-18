package org.corfudb.integration;

import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rmichoud on 2/12/17.
 */
public class SealIT {
    static String layoutServers;
    static Properties properties;

    @BeforeClass
    static public void getLayoutServers() throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("CorfuDB.properties");
        properties = new Properties();
        properties.load(input);
        layoutServers = (String) properties.get("layoutServers");
    }

    @Test
    public void RuntimeWithWrongEpochGetUpdated() throws Exception {
        CorfuRuntime cr1 = new CorfuRuntime(layoutServers).connect();
        CorfuRuntime cr2 = new CorfuRuntime(layoutServers).connect();

        Long beforeAddress = cr2.getSequencerView().nextToken(new HashSet<>(),1).getToken();

        /* We will trigger a Paxos round, this is what will happen:
         *   1. Set our layout (same than before) with a new Epoch
         *   2. Seal the server(s)
         *   3. Propose the new layout by driving paxos.
         */

        Layout currentLayout = cr1.getLayoutView().getCurrentLayout();
        /* 1 */
        currentLayout.setEpoch(currentLayout.getEpoch() + 1);
        /* 2 */
        currentLayout.moveServersToEpoch();
        /* 3 */
        cr1.getLayoutView().updateLayout(currentLayout, 0);


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
        Long afterAddress = cr2.getSequencerView().nextToken(new HashSet<>(),1).getToken();
        assertThat(cr2.getLayoutView().getCurrentLayout().getEpoch()).
            isEqualTo(cr1.getLayoutView().getCurrentLayout().getEpoch());
        assertThat(afterAddress).isEqualTo(beforeAddress+1);
    }
}
