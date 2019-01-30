package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.corfudb.runtime.view.Layout;

/**
 * Phase2 data consists of rank and the proposed layout.
 * The container class provides a convenience to persist and retrieve
 * these two pieces of data together.
 */
@Data
@ToString
@AllArgsConstructor
public class Phase2Data {

    Rank rank;
    Layout layout;

    public void serialize(ByteBuf buf) {
        rank.serialize(buf);
        layout.serialize(buf);
    }

    public boolean equals(Object obj) {
        if (obj != null && getClass() == obj.getClass()) {
            Phase2Data other = (Phase2Data)obj;
            // This json conversion is expensive and shouldn't be
            // used on the critical path
            return rank.equals(other.getRank()) && layout.asJSONString().equals(other.getLayout().asJSONString());
        }
        return false;
    }

    public static Phase2Data deserialize(ByteBuf buf) {
        Rank rank = Rank.deserialize(buf);
        Layout layout = Layout.deserialize(buf);
        return new Phase2Data(rank, layout);
    }
}