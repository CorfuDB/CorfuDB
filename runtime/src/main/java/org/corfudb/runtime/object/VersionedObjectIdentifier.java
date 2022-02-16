package org.corfudb.runtime.object;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Objects;
import java.util.UUID;

@AllArgsConstructor
public class VersionedObjectIdentifier {

    @Getter
    @Setter
    @NonNull
    private UUID objectId;

    @Getter
    @Setter
    @NonNull
    private long version;

    @Override
    public String toString() {
        return objectId + "@" + version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VersionedObjectIdentifier that = (VersionedObjectIdentifier) o;
        return version == that.version && objectId.equals(that.objectId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId, version);
    }
}
