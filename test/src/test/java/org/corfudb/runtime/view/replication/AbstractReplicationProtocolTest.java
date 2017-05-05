package org.corfudb.runtime.view.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** This test tests basic properties of the contract
 * which all replication protocols must meet.
 *
 * Created by mwei on 4/13/17.
 */
public abstract class AbstractReplicationProtocolTest extends AbstractViewTest {

    /** Return the replication protocol under test. */
    abstract IReplicationProtocol getProtocol();

    /** Setup the replication protocol nodes under test. */
    abstract void setupNodes();

    LogData getLogData(long globalAddress, byte[] payload)
    {
        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize(payload, b);
        LogData d = new LogData(DataType.DATA, b);
        d.setGlobalAddress(globalAddress);
        return d;
    }

    /** Check if we can write and then read the value
     * that was written.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void canWriteRead()
            throws Exception {
        setupNodes();

        //begin tests
        final CorfuRuntime r = getDefaultRuntime();
        final IReplicationProtocol rp = getProtocol();
        final Layout layout = r.getLayoutView().getLayout();

        LogData data = getLogData(0, "hello world".getBytes());
        rp.write(layout, data);
        ILogData read = rp.read(layout, 0);

        assertThat(read.getType())
                .isEqualTo(DataType.DATA);
        assertThat(read.getGlobalAddress())
                .isEqualTo(0);
        assertThat(read.getPayload(r))
                .isEqualTo("hello world".getBytes());
    }

    /** Check to make sure reads never return empty in
     * the case of an unwritten address.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void readOnlyCommitted()
            throws Exception {
        setupNodes();

        //begin tests
        final CorfuRuntime r = getDefaultRuntime();
        final IReplicationProtocol rp = getProtocol();
        final Layout layout = r.getLayoutView().getLayout();

        ILogData read = rp.read(layout, 0);

        assertThat(read.getType())
                .isNotEqualTo(DataType.EMPTY);

        read = rp.read(layout, 1);

        assertThat(read.getType())
                .isNotEqualTo(DataType.EMPTY);
    }


    /** Check to make sure that overwriting a previously
     * written entry results in an OverwriteException.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void overwriteThrowsException()
            throws Exception {
        setupNodes();

        //begin tests
        final CorfuRuntime r = getDefaultRuntime();
        final IReplicationProtocol rp = getProtocol();
        final Layout layout = r.getLayoutView().getLayout();

        LogData d1 = getLogData(0, "1".getBytes());
        LogData d2 = getLogData(0, "2".getBytes());
        rp.write(layout, d1);
        assertThatThrownBy(() -> rp.write(layout, d2))
                .isInstanceOf(OverwriteException.class);
    }
}
