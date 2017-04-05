package org.corfudb.runtime.exceptions;

import lombok.Getter;

import java.util.UUID;

/**
 * Wrong Cluster ID Exception
 * This is thrown when an incoming message has a cluster id not
 * matching the router cluster id.
 * <p>
 * Created by zlokhandwala on 4/4/17.
 */
public class WrongClusterIdException extends RuntimeException {
    @Getter
    final UUID clusterId;

    public WrongClusterIdException(UUID clusterId) {
        super("Wrong cluster ID. [expected=" + clusterId + "]");
        this.clusterId = clusterId;
    }
}
