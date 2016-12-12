package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.SequencerClient;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/8/16.
 */
public class ReplexStreamViewTest extends AbstractCorfuTest {

    /** Replex tests are disabled until unit tests stabilize
     * TODO: Restore & fix up test as of commit 3567e2ee6b
     */

    @Test
    public void replexTestsAreDisabled() {
        testStatus = "TODO";
    }
}
