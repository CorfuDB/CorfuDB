package org.corfudb.runtime.exceptions;

import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Created by mwei on 12/11/15.
 */
@ToString
@RequiredArgsConstructor
public class WrongEpochException extends Exception {
    final long correctEpoch;
}
