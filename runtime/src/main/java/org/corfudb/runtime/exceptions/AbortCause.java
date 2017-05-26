package org.corfudb.runtime.exceptions;

import lombok.RequiredArgsConstructor;

/**
 * Created by dalia on 4/8/17.
 */
@RequiredArgsConstructor
public enum AbortCause {
    CONFLICT,
    NEW_SEQUENCER,
    USER,
    NETWORK,
    UNDEFINED;
}
