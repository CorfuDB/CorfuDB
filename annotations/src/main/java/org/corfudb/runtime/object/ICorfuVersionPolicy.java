package org.corfudb.runtime.object;

import org.corfudb.annotations.DontInstrument;

/**
 * This interface helps different Corfu components understand
 * what type of versioning update scheme the underlying object
 * supports.
 */
public interface ICorfuVersionPolicy {

    /**
     * The default (non-monotonic) version update policy.
     * This means that the object is allowed to move back and forth in time.
     */
    VersionPolicy DEFAULT = new VersionPolicy();

    /**
     * A more restrictive update policy that allows the object
     * to only move forward in time.
     */
    VersionPolicy MONOTONIC = new VersionPolicy();

    /**
     * Any sort of state associated with the version policy.
     */
    class VersionPolicy {
    }

    /**
     * Defines how the underlying object versioning should behave.
     *
     * @return The version policy
     */
    @DontInstrument
    default VersionPolicy getVersionPolicy() {
        return DEFAULT;
    }
}
