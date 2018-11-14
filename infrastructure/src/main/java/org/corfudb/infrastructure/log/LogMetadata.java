package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.view.Address;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A container object that holds log tail offsets and the global
 * log tail that has been seen. Note that holes don't belong to any
 * stream therefore the globalTail needs to be tracked separately.
 *
 * <p>Created by maithem on 10/15/18.
 */

@NotThreadSafe
@ToString
@Slf4j
public class LogMetadata {

    @Getter
    private volatile Token globalTail;

    @Getter
    private final Map<UUID, Token> streamTails;

    public LogMetadata() {
        this.globalTail = Token.UNINITIALIZED;
        this.streamTails = new HashMap();
    }

    public void update(List<LogData> entries) {
        for (LogData entry : entries) {
            update(entry);
        }
    }

    public void update(LogData entry) {
        long entryAddress = entry.getGlobalAddress();
        updateGlobalTail(entryAddress);
        for (UUID streamId : entry.getStreams()) {
            Token currentStreamTail = streamTails.getOrDefault(streamId, Token.UNINITIALIZED);
            streamTails.put(streamId, Token.max(currentStreamTail, globalTail));
        }

        // We should also consider checkpoint metadata while updating the tails.
        // This is important because there could be streams that have checkpoint
        // data on the checkpoint stream, but not entries on the regular stream.
        // If those streams are not updated, then clients would observe those
        // streams as empty, which is not correct.
        if (entry.hasCheckpointMetadata()) {
            UUID streamId = entry.getCheckpointedStreamId();
            Token streamTailAtCP = entry.getCheckpointedStreamStartLogAddress();

            if (Address.isAddress(streamTailAtCP.getSequence())) {
                // TODO(Maithem) This is needed to filter out checkpoints of empty streams,
                // if the map has an entry (streamId, Address.Non_ADDRESS), then
                // when the sequencer services queries on that stream it will
                // "think" that the tail is not empty and return Address.Non_ADDRESS
                // instead of NON_EXIST. The sequencer, should handle both cases,
                // but that can be addressed in another issue.
                Token currentStreamTail = streamTails.getOrDefault(streamId, Token.UNINITIALIZED);
                streamTails.put(streamId, Token.max(currentStreamTail, streamTailAtCP));
            }
        }
    }

    public void updateGlobalTail(Token newTail) {
        globalTail = Token.max(globalTail, newTail);
    }

}
