package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.AllArgsConstructor;
import lombok.Data;

import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;

/**
 * Request to merge the given layout segment.
 */
@Data
@AllArgsConstructor
public class MergeSegmentRequest implements ICorfuPayload<MergeSegmentRequest> {

    private LayoutSegment segment;

    public MergeSegmentRequest(ByteBuf buf) {
        segment = Layout.getParser().fromJson(ICorfuPayload.fromBuffer(buf, String.class),
                LayoutSegment.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, Layout.getParser().toJson(segment));
    }
}
