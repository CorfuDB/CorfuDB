package org.corfudb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

public class CorfuSingleNodeBenchmarkStateTest extends AbstractCorfuTest {

    /**
     * Checks if the benchmark state can access the Corfu sequencer.
     *
     * Created by zlokhandwala on 5/3/19.
     */
    @Test
    public void benchmarkStateCanConnectAndAcquireToken() {
        CorfuSingleNodeBenchmarkState benchmarkState = new CorfuSingleNodeBenchmarkState();
        benchmarkState.initializeTrial();
        benchmarkState.initializeIteration();

        CorfuRuntime rt = benchmarkState.getNewRuntime();

        UUID stream = new UUID(0, 0);
        TokenResponse tr = rt.getSequencerView().next(stream);
        assertThat(tr.getToken()).isEqualTo(rt.getSequencerView().query().getToken());

        benchmarkState.teardownIteration();
        benchmarkState.teardownTrial();
    }
}
