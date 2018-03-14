package org.corfudb.runtime.view.replication;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Test the chain replication protocol.
 *
 * Created by mwei on 4/11/17.
 */
public class ChainReplicationProtocolTest extends AbstractReplicationProtocolTest {

    /** {@inheritDoc} */
    @Override
    IReplicationProtocol getProtocol() {
        return new
                ChainReplicationProtocol(new
                AlwaysHoleFillPolicy());
    }

    /** {@inheritDoc} */
    @Override
    void setupNodes() {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        bootstrapAllServers(new TestLayoutBuilder()
                .addLayoutServer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .setReplicationMode(Layout.ReplicationMode.CHAIN_REPLICATION)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build());
    }

    /** Check to see that a writer correctly
     * completes a failed write from another client.
     */
    @Test
    public void failedWriteIsPropagated()
            throws Exception {
        setupNodes();
        //begin tests
        final CorfuRuntime r = getDefaultRuntime();
        final IReplicationProtocol rp = getProtocol();
        final RuntimeLayout runtimeLayout = r.getLayoutView().getRuntimeLayout();

        LogData failedWrite = getLogData(0, "failed".getBytes());
        LogData incompleteWrite = getLogData(0, "incomplete".getBytes());

        // Write the incomplete write to the head of the chain
        runtimeLayout.getLogUnitClient(SERVERS.ENDPOINT_0).write(incompleteWrite);

        // Attempt to write using the replication protocol.
        // Should result in an overwrite exception
        assertThatThrownBy(() -> rp.write(runtimeLayout, failedWrite))
                .isInstanceOf(OverwriteException.class);

        // At this point, a direct read of the tail should
        // reflect the -other- clients value
        ILogData readResult = runtimeLayout.getLogUnitClient(SERVERS.ENDPOINT_0)
                .read(0).get().getAddresses().get(0L);

        assertThat(readResult.getPayload(r))
            .isEqualTo("incomplete".getBytes());
    }


    /** Check to see that a read correctly
     * completes a failed write from another client.
     */
    @Test
    public void failedWriteCanBeRead() {
        setupNodes();
        //begin tests
        final CorfuRuntime r = getDefaultRuntime();
        final IReplicationProtocol rp = getProtocol();
        final RuntimeLayout runtimeLayout = r.getLayoutView().getRuntimeLayout();

        LogData incompleteWrite = getLogData(0, "incomplete".getBytes());

        // Write the incomplete write to the head of the chain
        runtimeLayout.getLogUnitClient(SERVERS.ENDPOINT_0).write(incompleteWrite);

        // At this point, a read
        // reflect the -other- clients value
        ILogData readResult = rp.read(runtimeLayout, 0);

        assertThat(readResult.getPayload(r))
                .isEqualTo("incomplete".getBytes());
    }

    private void removeLogunit(Layout currentLayout, String endpoint) throws Exception {
        CorfuRuntime corfuRuntime = getRuntime(currentLayout).connect();
        Layout layout = new Layout(corfuRuntime.getLayoutView().getLayout());
        layout.setEpoch(layout.getEpoch() + 1);
        layout.getLayoutServers().add(endpoint);
        layout.getSegment(0L).getStripes().get(0).getLogServers().remove(endpoint);
        corfuRuntime.getLayoutView().getRuntimeLayout(layout).moveServersToEpoch();
        corfuRuntime.getLayoutView().updateLayout(layout, 1L);
    }

    /**
     * Sets up 3 log unit nodes N0, N1 and N2 at epoch 1.
     * Write data to N0 ONLY.
     * Now using the chain replication protocol view, the same data is read back.
     * The read would now fail and would resort to hole filling which would cause the hole filling
     * client to attempt continuing the writes to N1 and N2 with the same epoch 1.
     * These new hole fill writes should be rejected by the chain as the cluster has been sealed
     * and moved to a new epoch 2.
     */
    @Test
    public void cannotWriteAcrossEpochs() throws Exception {
        setupNodes();
        final CorfuRuntime r = getDefaultRuntime();
        Layout layout = new Layout(r.getLayoutView().getLayout());
        RuntimeLayout runtimeLayout = r.getLayoutView().getRuntimeLayout();
        final IReplicationProtocol rp = getProtocol();

        LogData incompleteWrite = getLogData(0, "incomplete".getBytes());

        // Write the incomplete write to the head of the chain
        runtimeLayout.getLogUnitClient(SERVERS.ENDPOINT_0).write(incompleteWrite);
        removeLogunit(layout, SERVERS.ENDPOINT_2);
        r.invalidateLayout();
        r.getLayoutView().getLayout();
        runtimeLayout = r.getLayoutView().getRuntimeLayout();

        try {
            // Trigger hole fill.
            rp.read(r.getLayoutView().getRuntimeLayout(layout), 0L);
            fail("No wrong epoch on write.");
        } catch (WrongEpochException we) {
            assertThat(we.getCorrectEpoch()).isEqualTo(1L);
        }

        LogData logData = runtimeLayout.getLogUnitClient(SERVERS.ENDPOINT_0).read(0L).get()
                .getAddresses().get(0L);
        assertThat(logData.getData()).containsExactly(incompleteWrite.getData());
        logData = runtimeLayout.getLogUnitClient(SERVERS.ENDPOINT_1).read(0L).get()
                .getAddresses().get(0L);
        assertThat(logData.getData()).isNullOrEmpty();
        logData = runtimeLayout.getLogUnitClient(SERVERS.ENDPOINT_2).read(0L).get()
                .getAddresses().get(0L);
        assertThat(logData.getData()).isNullOrEmpty();
    }
}
