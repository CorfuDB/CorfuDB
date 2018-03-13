package org.corfudb.runtime.clients;

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
     * @return Client.
     */
    C getClient(long epoch);
}
