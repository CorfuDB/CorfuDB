/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.runtime.view;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;

import java.util.function.Function;

/**
 * Defines the methods in StreamView specific for given replication mode.
 * Created by Konstantin Spirov on 1/30/2017.
 */
public interface IStreamViewDelegate {

    /**
     * Read the next item from the stream.
     * @param receiver  The StreamView that delegates to this object
     * @param maxGlobal Constraint for the max stream address, usable for integration tests
     * @return The next item from the stream.
     */
     ILogData read(StreamView receiver, long maxGlobal);


    /**
     * Read the items from the stream until a given position is reached
     * @param receiver  The StreamView that delegates to this object
     * @param pos  The highest position to read from (inclusive)
     * @return The next item from the stream.
     */
     ILogData[] readTo(StreamView receiver, long pos);

}
