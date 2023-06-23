package org.corfudb.runtime.collections;

import lombok.Getter;

import javax.annotation.Nullable;
import java.util.Collection;

public class ReceivedDeltaMessage {
    @Getter
    private final String sourceSite;

    @Getter
    @Nullable
    private final Collection<DeltaMessage> messages;

    public ReceivedDeltaMessage(String sourceSite, @Nullable Collection<DeltaMessage> messages) {
        this.sourceSite = sourceSite;
        this.messages = messages;
    }
}
