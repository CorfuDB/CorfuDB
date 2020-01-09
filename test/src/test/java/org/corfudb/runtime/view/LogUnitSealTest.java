package org.corfudb.runtime.view;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests log unit server sealing behavior
 *
 * Created by WenbinZhu on 11/8/18.
 */
public class LogUnitSealTest extends AbstractViewTest {

    private RuntimeLayout getRuntimeLayout(long epoch) {
        addServer(SERVERS.PORT_0);

        Layout l = new TestLayoutBuilder()
                .setEpoch(epoch)
                .addLayoutServer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();

        bootstrapAllServers(l);
        CorfuRuntime corfuRuntime = getRuntime(l).connect();
        RuntimeLayout runtimeLayout = corfuRuntime.getLayoutView().getRuntimeLayout(l);

        getManagementServer(SERVERS.PORT_0).shutdown();

        return runtimeLayout;
    }

    /**
     * Test that log unit server seals correctly.
     * Check operations in the queue that come after the seal operation, but stamped with
     * an older epoch, are completed exceptionally with a WrongEpochException.
     */
    @Test
    public void checkOperationWithOldEpochFailedAfterSeal() {
        long epoch = 1L;
        RuntimeLayout runtimeLayout = getRuntimeLayout(epoch);
        Layout layout = runtimeLayout.getLayout();
        LogUnitClient client = runtimeLayout.getLogUnitClient(SERVERS.ENDPOINT_0);
        List<CompletableFuture<Boolean>> successOpFutures2 = new ArrayList<>();
        List<CompletableFuture<Boolean>> failedOpFutures = new ArrayList<>();

        final int numOp = 3;
        long address = 0L;

        for (int i = 0; i < numOp; i++) {
            LogData ld = LogData.getHole(address++);
            client.write(ld).join();
        }

        // Seal server
        layout.setEpoch(layout.getEpoch() + 1);
        runtimeLayout.sealMinServerSet();

        // Intentionally reset router epoch to bypass epoch check on router.
        // We expect that the test server router's epoch is reset, but the
        // batchWriter should reject update its epoch with a smaller epoch.
        assertThatThrownBy(() -> getLayoutServer(SERVERS.PORT_0)
                .getServerContext().getServerRouter().setServerEpoch(epoch))
                .isExactlyInstanceOf(WrongEpochException.class);

        // Still using the old client to send messages stamped with old epoch
        for (int i = 0; i < numOp; i++) {
            LogData ld = LogData.getHole(address++);
            failedOpFutures.add(client.write(ld));
        }

        // Set the router epoch to new epoch and get the new client
        getLayoutServer(SERVERS.PORT_0).getServerContext().getServerRouter().setServerEpoch(layout.getEpoch());
        runtimeLayout = new RuntimeLayout(layout, getRuntime(layout));
        client = runtimeLayout.getLogUnitClient(SERVERS.ENDPOINT_0);

        // Using the new client to send messages stamped with epoch 2
        for (int i = 0; i < numOp; i++) {
            LogData ld = LogData.getHole(address++);
            successOpFutures2.add(client.write(ld));
        }

        // Before sealing, all operations should complete normally
        // After sealing, operations stamped with old epoch should fail
        failedOpFutures.forEach(f -> assertThatThrownBy(f::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseExactlyInstanceOf(WrongEpochException.class));
        // After sealing, operations stamped with new epoch should still complete normally
        successOpFutures2.forEach(f -> assertThatCode(f::get).doesNotThrowAnyException());
    }
}
