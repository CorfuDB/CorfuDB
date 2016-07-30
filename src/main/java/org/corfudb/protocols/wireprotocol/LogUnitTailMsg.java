package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeMap;
import com.google.common.collect.TreeRangeSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.serializer.Serializers;

import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * Created by mwei on 2/10/16.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class LogUnitTailMsg extends CorfuMsg {

    Long contiguousTail;

    RangeSet<Long> streamAddresses;

    public LogUnitTailMsg(Long contiguousTail, RangeSet<Long> streamAddresses) {
        this.msgType = CorfuMsgType.CONTIGUOUS_TAIL;
        this.contiguousTail = contiguousTail;
        this.streamAddresses = streamAddresses;
    }

    public LogUnitTailMsg(Long contiguousTail) {
        this.msgType = CorfuMsgType.CONTIGUOUS_TAIL;
        this.contiguousTail = contiguousTail;
        this.streamAddresses = TreeRangeSet.create();
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeLong(contiguousTail);
        Set<Range<Long>> ranges = streamAddresses.asRanges();
        buffer.writeInt(ranges.size());
        for (Range i : ranges)
        {
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
        contiguousTail = buffer.readLong();
        streamAddresses = TreeRangeSet.create();
        int ranges = buffer.readInt();
        for (int i = 0; i < ranges; i++)
        {
            Range r = (Range) Serializers.getSerializer(Serializers.SerializerType.JAVA).deserialize(buffer, null);
            streamAddresses.add(r);
        }
    }
}
