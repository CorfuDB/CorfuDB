package org.corfudb.runtime.object;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.Utils;

import java.util.Optional;
import java.util.UUID;

@EqualsAndHashCode
@ToString
public class StreamId {
    @Getter
    private final UUID id;

    private final Optional<String> name;

    /**
     * @param id id of the stream
     */
    private StreamId(UUID id, Optional<String> name) {
        this.name = name;
        this.id = id;
    }

    /**
     * Either of the variables should be defined to create a streamId.
     *
     * @param streamName name of a stream
     * @param uuid       id of a stream
     * @return streamId object
     * @throws IllegalStateException exception if none are defined
     */
    public static StreamId build(Optional<String> streamName, Optional<UUID> uuid) {
        StreamId streamId;

        if (streamName.isPresent() && uuid.isPresent()) {
            streamId = new StreamId(uuid.get(), streamName);
        } else if (streamName.isPresent()) {
            UUID id = CorfuRuntime.getStreamID(streamName.get());
            streamId = new StreamId(id, streamName);
        } else if (uuid.isPresent()) {
            streamId = new StreamId(uuid.get(), streamName);
        } else {
            throw new IllegalArgumentException("Either uuid or streamName should be defined");
        }

        return streamId;
    }

    public String getName() {
        return name.orElse(Utils.toReadableId(id));
    }
}
