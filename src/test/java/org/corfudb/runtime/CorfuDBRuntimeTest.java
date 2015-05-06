package org.corfudb.runtime;

import org.corfudb.runtime.protocols.configmasters.IConfigMaster;
import org.corfudb.runtime.protocols.configmasters.MemoryConfigMasterProtocol;
import org.corfudb.runtime.protocols.logunits.MemoryLogUnitProtocol;
import org.corfudb.runtime.protocols.sequencers.MemorySequencerProtocol;
import org.corfudb.runtime.view.ConfigurationMaster;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.assertj.core.api.Assertions.*;
import org.corfudb.runtime.view.CorfuDBView;

/**
 * Created by mwei on 4/30/15.
 */
public class CorfuDBRuntimeTest {
    @Test
    public void MemoryCorfuDBRuntimeHasComponents() {
        CorfuDBRuntime runtime = new CorfuDBRuntime("memory");
        CorfuDBView view = runtime.getView();
        assertNotNull(view);
        assertThat(view.getConfigMasters().get(0))
                .isInstanceOf(MemoryConfigMasterProtocol.class);
        assertThat(view.getSequencers().get(0))
                .isInstanceOf(MemorySequencerProtocol.class);
        assertThat(view.getSegments().get(0).getGroups().get(0).get(0))
                .isInstanceOf(MemoryLogUnitProtocol.class);
    }

    @Test
    public void MemoryCorfuDBViewChangeTest() {
        CorfuDBRuntime runtime = new CorfuDBRuntime("memory");
        CorfuDBView view = runtime.getView();
        assertNotNull(view);
        ConfigurationMaster cm = new ConfigurationMaster(runtime);
        cm.resetAll();
        view = runtime.getView();
        assertNotNull(view);
    }
}
