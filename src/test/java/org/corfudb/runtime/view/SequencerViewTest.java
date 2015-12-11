package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuRuntime;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/10/15.
 */
public class SequencerViewTest {

    CorfuRuntime runtime;

    @Before
    public void setupRuntime() {
        runtime = new CorfuRuntime()
                    .addLayoutServer("localhost:9999")
                    .connect();
    }

    @Test
    public void tokensOnSameStreamIncrement() {
        UUID streamA = UUID.randomUUID();
        long token = runtime.getSequencerView().nextToken(Collections.singleton(streamA), 1);
        long token2 = runtime.getSequencerView().nextToken(Collections.singleton(streamA), 1);
        assertThat(token)
                .isLessThan(token2);
    }
}
