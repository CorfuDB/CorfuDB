package org.corfudb.runtime.view;

import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg.ReadResult;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Set;
import java.util.UUID;

/**
 * Created by mwei on 12/10/15.
 */
public class AddressSpaceView extends AbstractView {
    public AddressSpaceView(CorfuRuntime runtime)
    {
        super(runtime);
    }

    /**
     * Write the given object to an address and streams.
     *
     * @param address An address to write to.
     * @param stream  The streams which will belong on this entry.
     * @param data    The data to write.
     */
    public void write(long address, Set<UUID> stream, Object data) {
        layoutHelper(l -> {
           AbstractReplicationView.getReplicationView(l, l.getReplicationMode(address))
                   .write(address, stream, data);
           return null;
        });
    }

    /**
     * Read the given object from an address and streams.
     *
     * @param address An address to read from.
     */
    public ReadResult read(long address) {
        return layoutHelper(l -> AbstractReplicationView
                     .getReplicationView(l, l.getReplicationMode(address))
                    .read(address)
        );
    }

    /**
     * Fill a hole at the given address.
     * @param address An address to hole fill at.
     */
    public void fillHole(long address) {
        layoutHelper(l -> {AbstractReplicationView
                .getReplicationView(l, l.getReplicationMode(address))
                .fillHole(address);
                return null;}
        );
    }
}
