package org.corfudb.protocols.logprotocol;

import com.codepoetics.protonpack.StreamUtils;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.SMREntryWithLocator;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.serializer.Serializers;



/**
 * This class captures a list of updates.
 * Its primary use case is to allow a single log entry to
 * hold a sequence of updates made by a transaction, which are applied atomically.
 */
@Deprecated // TODO: Add replacement method that conforms to style
@SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
@ToString
@Slf4j
public class MultiSMREntry extends LogEntry implements ISMRWithLocatorConsumable {

    public static class MultiSMREntryLocator implements ISMREntryLocator{
        /**
         * The globalAddress that the SMREntry is from in the global log.
         */
        @Getter
        private final long globalAddress;

        /**
         * Local position of one SMREntry inside one MultiSMREntry. The position starts from 0.
         */
        @Getter
        private final long index;

        public MultiSMREntryLocator(long globalAddress, long index) {
            this.globalAddress = globalAddress;
            this.index = index;
        }

        @Override
        public int compareTo(ISMREntryLocator other) {
            long otherAddress = other.getGlobalAddress();
            if (otherAddress == globalAddress) {
                if (other instanceof MultiSMREntry) {
                    return Long.compare(index, ((MultiSMREntryLocator) other).getIndex());
                }
                throw new RuntimeException("SMREntries of the same global address have different SMREntry type at "
                        + globalAddress);
            }
            return Long.compare(globalAddress, otherAddress);
        }
    }

    @Getter
    List<SMREntry> updates = Collections.synchronizedList(new ArrayList<>());

    public MultiSMREntry() {
        this.type = LogEntryType.MULTISMR;
    }

    public MultiSMREntry(List<SMREntry> updates) {
        this.type = LogEntryType.MULTISMR;
        this.updates = updates;
    }

    public void addTo(SMREntry entry) {
        getUpdates().add(entry);
    }

    public void mergeInto(MultiSMREntry other) {
        getUpdates().addAll(other.getUpdates());
    }

    /**
     * This function provides the remaining buffer.
     *
     * @param b The remaining buffer.
     */
    @Override
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);

        int numUpdates = b.readInt();
        updates = new ArrayList<>();
        for (int i = 0; i < numUpdates; i++) {
            updates.add(
                    (SMREntry) Serializers.CORFU.deserialize(b, rt));
        }
    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeInt(updates.size());
        updates.stream()
                .forEach(x -> Serializers.CORFU.serialize(x, b));
    }

    @Override
    public void setEntry(ILogData entry) {
        super.setEntry(entry);
        this.getUpdates().forEach(x -> x.setEntry(entry));
    }

    @Override
    public List<SMREntry> getSMRUpdates(UUID id) {
        return updates;
    }

    @Override
    public List<SMREntryWithLocator> getSMRWithLocatorUpdates(long globalAddress, UUID id) {
        return StreamUtils.zipWithIndex(updates.stream()).map(i -> {
            MultiSMREntryLocator locator = new MultiSMREntryLocator(globalAddress, (int) i.getIndex());
            return new SMREntryWithLocator(i.getValue(), locator);
        }).collect(Collectors.toList());
    }
}
