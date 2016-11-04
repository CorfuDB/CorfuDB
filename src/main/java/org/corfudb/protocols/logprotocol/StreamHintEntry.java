package org.corfudb.protocols.logprotocol;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.netty.buffer.ByteBuf;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.Serializers;

import java.util.Set;

/**
 * Created by mwei on 2/11/16.
 */
@ToString(callSuper = true)
@NoArgsConstructor
public class StreamHintEntry extends LogEntry {

    RangeSet<Long> ranges;

    public StreamHintEntry(RangeSet<Long> ranges) {
        super(LogEntryType.STREAM_HINT);
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
            Serializers.JAVA.serialize(i, buffer);
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
    public void deserializeBuffer(ByteBuf buffer, CorfuRuntime rt) {
        super.deserializeBuffer(buffer, rt);
        this.ranges = TreeRangeSet.create();
        int ranges = buffer.readInt();
        for (int i = 0; i < ranges; i++) {
            Range r = (Range) Serializers.JAVA.deserialize(buffer, null);
            this.ranges.add(r);
        }
    }
}
