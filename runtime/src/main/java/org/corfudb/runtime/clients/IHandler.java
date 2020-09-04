package org.corfudb.runtime.clients;

import java.util.UUID;

/**
 * Handler Clients to handle the responses form the server.
 *
 * <p>Created by zlokhandwala on 2/20/18.
 * @param <C> Type of Client.
 */
public interface IHandler<C extends AbstractClient> {

    /**
     * Fetches the sender client.
     *
     * @param epoch Epoch to stamp the sender client with.
     * @param clusterID Cluster ID the client wants to connect to.
     * @return Client.
     */
    C getClient(long epoch, UUID clusterID);
}
