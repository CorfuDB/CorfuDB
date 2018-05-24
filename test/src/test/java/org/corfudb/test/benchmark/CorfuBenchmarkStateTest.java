package org.corfudb.test.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.UUID;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

public class CorfuBenchmarkStateTest extends AbstractCorfuTest {

    /** Checks if the benchmark state can access the Corfu sequencer. */
    @Test
    public void benchmarkStateCanConnectAndAcquireToken() {
        CorfuBenchmarkState bs = new CorfuBenchmarkState();
        bs.initializeTrial();
        bs.initializeIteration();

        CorfuRuntime rt = bs.getNewRuntime();
        rt.connect();

        UUID stream = new UUID(0, 0);
        TokenResponse tr = rt.getSequencerView().nextToken(Collections.singleton(stream), 1);
        assertThat(tr.getTokenValue())
            .isEqualTo(rt.getSequencerView().nextToken(Collections.emptySet(), 0)
                .getTokenValue());

        bs.teardownIteration();
        bs.teardownTrial();
    }
}
