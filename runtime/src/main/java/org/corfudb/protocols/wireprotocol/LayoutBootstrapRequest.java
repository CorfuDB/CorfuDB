package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.AllArgsConstructor;
import lombok.Data;

import org.corfudb.runtime.view.Layout;


/**
 * Request sent to bootstrap the server with a {@link Layout}.
 * Created by mdhawan on 10/24/16.
 */
@Data
@AllArgsConstructor
public class LayoutBootstrapRequest implements ICorfuPayload<LayoutBootstrapRequest> {
    private Layout layout;

    public LayoutBootstrapRequest(ByteBuf buf) {
        layout = ICorfuPayload.fromBuffer(buf, Layout.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, layout);
    }
}