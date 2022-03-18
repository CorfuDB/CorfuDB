package org.corfudb.runtime.object;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.UUID;

@AllArgsConstructor
public class VersionedObjectIdentifier {

    @Getter
    @Setter
    @NonNull
    private UUID objectId;

    @Getter
    @Setter
    private long version;

    @Override
    public String toString() {
        return objectId + "@" + version;
    }
}
