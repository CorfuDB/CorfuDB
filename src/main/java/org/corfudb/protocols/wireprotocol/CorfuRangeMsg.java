package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.corfudb.util.serializer.Serializers;

import java.util.Set;

/**
 * Created by mwei on 2/10/16.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class CorfuRangeMsg extends CorfuMsg {

    RangeSet<Long> ranges;

    public CorfuRangeMsg(RangeSet<Long> ranges) {
        this.ranges = ranges;
    }

    public CorfuRangeMsg(CorfuMsgType type, RangeSet<Long> ranges) {
        this.msgType = type;
        this.ranges = ranges;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        Set<Range<Long>> ranges = this.ranges.asRanges();
        buffer.writeInt(ranges.size());
        for (Range i : ranges) {
            Serializers.getSerializer(Serializers.SerializerType.JAVA).serialize(i, buffer);
        }
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend CorfuMsg
     * should parse their fields in this method.
     *
     * @param buffer
     */
    @Override
    @SuppressWarnings("unchecked")
    public void fromBuffer(ByteBuf buffer) {
        super.fromBuffer(buffer);
        this.ranges = TreeRangeSet.create();
        int ranges = buffer.readInt();
        for (int i = 0; i < ranges; i++) {
            Range r = (Range) Serializers.getSerializer(Serializers.SerializerType.JAVA).deserialize(buffer, null);
            this.ranges.add(r);
        }
    }
}
