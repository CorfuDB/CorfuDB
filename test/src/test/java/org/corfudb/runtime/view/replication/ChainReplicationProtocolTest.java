package org.corfudb.runtime.view.replication;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.WrongEpochException;
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
        final Layout layout = r.getLayoutView().getLayout();

        LogData failedWrite = getLogData(0, "failed".getBytes());
        LogData incompleteWrite = getLogData(0, "incomplete".getBytes());

        // Write the incomplete write to the head of the chain
        r.getLogUnitClient(layout, SERVERS.ENDPOINT_0).write(incompleteWrite);

        // Attempt to write using the replication protocol.
        // Should result in an overwrite exception
        assertThatThrownBy(() -> rp.write(layout, failedWrite))
                .isInstanceOf(OverwriteException.class);

        // At this point, a direct read of the tail should
        // reflect the -other- clients value
        ILogData readResult = r.getLogUnitClient(layout, SERVERS.ENDPOINT_0)
                .read(0).get().getAddresses().get(0L);

        assertThat(readResult.getPayload(r))
            .isEqualTo("incomplete".getBytes());
    }


    /** Check to see that a read correctly
     * completes a failed write from another client.
     */
    @Test
    public void failedWriteCanBeRead()
            throws Exception {
        setupNodes();
        //begin tests
        final CorfuRuntime r = getDefaultRuntime();
        final IReplicationProtocol rp = getProtocol();
        final Layout layout = r.getLayoutView().getLayout();

        LogData incompleteWrite = getLogData(0, "incomplete".getBytes());

        // Write the incomplete write to the head of the chain
        r.getLogUnitClient(layout, SERVERS.ENDPOINT_0).write(incompleteWrite);

        // At this point, a read
        // reflect the -other- clients value
        ILogData readResult = rp.read(layout, 0);

        assertThat(readResult.getPayload(r))
                .isEqualTo("incomplete".getBytes());
    }

    private void removeLogunit(Layout currentLayout, String endpoint) throws Exception {
        CorfuRuntime corfuRuntime = getRuntime(currentLayout).connect();
        Layout layout = new Layout(corfuRuntime.getLayoutView().getLayout());
        layout.setEpoch(layout.getEpoch() + 1);
        layout.getLayoutServers().add(endpoint);
        layout.getSegment(0L).getStripes().get(0).getLogServers().remove(endpoint);
        layout.setRuntime(corfuRuntime);
        layout.moveServersToEpoch();
        corfuRuntime.getLayoutView().updateLayout(layout, 1L);
    }

    @Test
    public void cannotWriteAcrossEpochs() throws Exception {
        setupNodes();
        final CorfuRuntime r = getDefaultRuntime();
        Layout layout = new Layout(r.getLayoutView().getLayout());
        final IReplicationProtocol rp = getProtocol();
        layout.setRuntime(r);

        LogData incompleteWrite = getLogData(0, "incomplete".getBytes());

        // Write the incomplete write to the head of the chain
        r.getLogUnitClient(layout, SERVERS.ENDPOINT_0).write(incompleteWrite);
        removeLogunit(layout, SERVERS.ENDPOINT_2);
        r.invalidateLayout();
        r.getLayoutView().getLayout();

        try {
            // Trigger hole fill.
            rp.read(layout, 0L);
            fail("No wrong epoch on write.");
        } catch (WrongEpochException we) {
            assertThat(we.getCorrectEpoch()).isEqualTo(1L);
        }

        layout = new Layout(r.getLayoutView().getLayout());
        LogData logData = r.getLogUnitClient(layout, SERVERS.ENDPOINT_0).read(0L).get()
                .getAddresses().get(0L);
        assertThat(logData.getData()).containsExactly(incompleteWrite.getData());
        logData = r.getLogUnitClient(layout, SERVERS.ENDPOINT_1).read(0L).get()
                .getAddresses().get(0L);
        assertThat(logData.getData()).isNullOrEmpty();
        logData = r.getLogUnitClient(layout, SERVERS.ENDPOINT_2).read(0L).get()
                .getAddresses().get(0L);
        assertThat(logData.getData()).isNullOrEmpty();
    }
}
