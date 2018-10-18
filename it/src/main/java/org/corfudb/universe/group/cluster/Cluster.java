package org.corfudb.universe.group.cluster;

import org.corfudb.universe.group.Group;

public interface Cluster extends Group {

    /**
     * Bootstrap a {@link Cluster}
     */
    void bootstrap();
}
