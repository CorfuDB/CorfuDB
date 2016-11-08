package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.corfudb.runtime.view.Layout;

/**
 * Request sent to bootstrap the server with a {@link Layout}.
 * Created by zlokhandwala on 11/8/16.
 */
@Data
@AllArgsConstructor
public class ManagementBootstrapRequest implements ICorfuPayload<ManagementBootstrapRequest> {
    private Layout layout;

    public ManagementBootstrapRequest(ByteBuf buf) {
        layout = ICorfuPayload.fromBuffer(buf, Layout.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, layout);
    }
}
