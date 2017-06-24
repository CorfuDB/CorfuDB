/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at http://www.apache.org/licenses/LICENSE-2.0
 */

package org.corfudb.util.retry;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * Parent class for all internal IRetry implementations
 *
 * <p>Created by Konstantin Spirov on 4/6/2017.
 */
@AllArgsConstructor
abstract class AbstractRetry<E extends Exception, F extends Exception,
        G extends Exception, H extends Exception, O, A extends IRetry>
        implements IRetry<E, F, G, H, O, org.corfudb.util.retry.ExponentialBackoffRetry> {

    @Getter
    final IRetryable<E, F, G, H, O> runFunction;

}
