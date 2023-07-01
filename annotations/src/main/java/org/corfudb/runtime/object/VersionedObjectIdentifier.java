package org.corfudb.runtime.object;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Objects;
import java.util.UUID;

/**
 * VersionedObjectIdentifier.
 */
@AllArgsConstructor
public final class VersionedObjectIdentifier {

    /**
     * UUID.
     */
    @Getter
    @Setter
    @NonNull
    private UUID objectId;

    /**
     * Version.
     */
    @Getter
    @Setter
    @NonNull
    private long version;

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return objectId + "@" + version;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VersionedObjectIdentifier that = (VersionedObjectIdentifier) o;
        return version == that.version && objectId.equals(that.objectId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(objectId, version);
    }
}
