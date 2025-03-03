package org.corfudb.runtime.exceptions;

import lombok.RequiredArgsConstructor;

/**
 * Created by dalia on 4/8/17.
 */
@RequiredArgsConstructor
public enum AbortCause {
    //TODO(Maithem): these types are redundant and should be removed,
    // we should just expose the throwable
    CONFLICT,
    DEADLOCK,
    OVERWRITE, /** Aborted because of slow writer, i.e., continuously gets overwritten (hole filled by faster reader) */
    NEW_SEQUENCER,
    SIZE_EXCEEDED,
    USER,
    NETWORK,
    TRIM, /** Aborted because an access to this snapshot resulted in a trim exception. */
    SEQUENCER_OVERFLOW,
    SEQUENCER_TRIM,
    QUOTA_EXCEEDED,
    UNSUPPORTED,
    UNDEFINED;
}
