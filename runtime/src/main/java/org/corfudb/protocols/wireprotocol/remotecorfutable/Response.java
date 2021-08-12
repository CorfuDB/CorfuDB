package org.corfudb.protocols.wireprotocol.remotecorfutable;

/**
 * Interface to be implemented for all remote corfu table responses.
 *
 * Created by nvaishampayan517 on 08/12/21
 */
public interface Response {
    /**
     * Returns the type of response.
     * @return The type of the response.
     */
    RemoteCorfuTableResponseType getType();
}
