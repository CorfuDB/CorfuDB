package org.corfudb.runtime.view.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.StreamData;
import org.corfudb.protocols.logprotocol.StreamedLogData;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.serializer.Serializers;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.UUID;

/**
 * Created by mwei on 4/6/17.
 */
@Slf4j
public class ChainReplicationProtocol extends AbstractReplicationProtocol {

    final Layout layout;

    public ChainReplicationProtocol(IHoleFillPolicy holeFillPolicy, Layout layout) {
        super(holeFillPolicy);
        this.layout = layout;
    }

    /** {@inheritDoc} */
    @Override
    public void write(long globalAddress,
                      @Nonnull Map<UUID, StreamData> entryMap) throws OverwriteException {
        int numUnits = layout.getSegmentLength(globalAddress);

        ByteBuf b = Unpooled.buffer();
        try {
            // To reduce the overhead of serialization, we serialize only the
            // first time we write, saving when we go down the chain.
            StreamedLogData sld = new StreamedLogData(entryMap);
            sld.getSerializedForm(Serializers.CORFU::serialize, b);

            for (int i = 0; i < numUnits; i++) {
                log.trace("Write[{}]: chain {}/{}", layout, i + 1, numUnits);
                // In chain replication, we write synchronously to every unit
                // in the chain.
                try {
                    CFUtils.getUninterruptibly(
                            layout.getLogUnitClient(globalAddress, i)
                                    // TODO: Currently, this call won't work until the log unit client is refactored.
                                    .write(globalAddress, null, null, b, null)
                            , OverwriteException.class);
                } catch (OverwriteException oe) {
                    // Some other wrote here (usually due to hole fill)
                    // We need to invoke the recovery protocol, in case
                    // the write wasn't driven to completion.
                    recover(globalAddress);
                    throw oe;
                }
            }
        } finally {
            b.release();
        }
    }

    /** {@inheritDoc} */
    @Override
    public ILogData peek(long globalAddress) {
        int numUnits = layout.getSegmentLength(globalAddress);
        log.trace("Read[{}]: chain {}/{}", globalAddress, numUnits, numUnits);
        // In chain replication, we read from the last unit, though we can optimize if we
        // know where the committed tail is.
        return CFUtils.getUninterruptibly(layout
                .getLogUnitClient(globalAddress, numUnits - 1)
                                    .read(globalAddress)).getReadSet()
                .getOrDefault(globalAddress, null);
    }

    /** Recover a failed write at the given global address,
     * driving it to completion by invoking the recovery
     * protocol.
     *
     * When this function returns the given globalAddress
     * is guaranteed to contain a committed value.
     *
     * If there was no data previously written at the address,
     * this function will throw a runtime exception. The
     * recovery protocol should -only- be invoked if we
     * previously were overwritten.
     *
     * @param globalAddress     The global address to drive
     *                          the recovery protocol
     *
     */
    protected void recover(long globalAddress) {
        // In chain replication, we started writing from the head,
        // and propagated down to the tail. To recover, we start
        // reading from the head, which should have the data
        // we are trying to recover
        int numUnits = layout.getSegmentLength(globalAddress);
        log.debug("Recover[{}]: read chain head {}/{}", globalAddress, 1, numUnits);
        ILogData ld = CFUtils.getUninterruptibly(layout.getLogUnitClient(globalAddress, 0)
                .read(globalAddress)).getReadSet().getOrDefault(globalAddress, null);
        // If nothing was at the head, this is a bug and we
        // should fail with a runtime exception, as there
        // was nothing to recover - if the head was removed
        // due to a reconfiguration, a network exception
        // would have been thrown and the client should have
        // retried it's operation (in this case of a write,
        // it should have read to determine whether the
        // write was successful or not.
        if (ld == null) {
            throw new RuntimeException();
        }
        // now we go down the chain and write, ignoring any overwrite exception we get.
        for (int i = 1; i < numUnits; i++) {
            log.debug("Recover[{}]: write chain {}/{}", layout, i + 1, numUnits);
            // In chain replication, we write synchronously to every unit
            // in the chain.
            try {
                CFUtils.getUninterruptibly(
                        layout.getLogUnitClient(globalAddress, i)
                                // TODO: Currently, this call won't work until the log unit client is refactored.
                                .write(globalAddress, null, null, ld, null)
                        , OverwriteException.class);
                // We successfully recovered a write to this member of the chain
                log.debug("Recover[{}]: recovered write at chain {}/{}", layout, i + 1, numUnits);
            } catch (OverwriteException oe) {
                // This member already had this data (in some cases, the write might have
                // been committed to all members, so this is normal).
                log.debug("Recover[{}]: overwritten at chain {}/{}", layout, i + 1, numUnits);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void holeFill(long globalAddress) {
        int numUnits = layout.getSegmentLength(globalAddress);
        for (int i = 0; i < numUnits; i++) {
            log.trace("fillHole[{}]: chain {}/{}", globalAddress, i + 1, numUnits);
            // In chain replication, we write synchronously to every unit in
            // the chain.
            try {
                CFUtils.getUninterruptibly(layout.getLogUnitClient(globalAddress, i)
                        .fillHole(globalAddress), OverwriteException.class);
            } catch (OverwriteException oe) {
                // The hole-fill failed. We must ensure the other writer's
                // value is adopted before returning.
                recover(globalAddress);
            }
        }
    }
}
