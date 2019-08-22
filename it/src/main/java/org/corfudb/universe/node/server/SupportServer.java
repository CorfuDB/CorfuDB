package org.corfudb.universe.node.server;

import com.spotify.docker.client.exceptions.DockerException;
import org.corfudb.universe.node.Node;

public interface SupportServer extends Node {
    @Override
    SupportServer deploy();
}
