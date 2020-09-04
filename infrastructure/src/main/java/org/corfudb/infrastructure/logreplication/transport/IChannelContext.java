package org.corfudb.infrastructure.logreplication.transport;

/**
 * This interface represents the shared context of a channel.
 *
 * It provides common abstractions required across the Client and Server Channel Adapters.
 *
 * @author amartinezman
 *
 */
public interface IChannelContext {

    /**
     * Return Local Node's Endpoint
     *
     * @return node's endpoint
     */
    String getEndpoint();
}
