/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuRuntime;
import org.junit.Ignore;

import java.util.Collections;

/**
 * Created by Konstantin Spirov on 1/30/2017.
 */
@Ignore
public class QuorumReplicationViewTest extends ChainReplicationViewTest {

    final String defaultConfigurationString = getDefaultEndpoint();

    public CorfuRuntime getDefaultRuntime() {
        CorfuRuntime r = super.getDefaultRuntime().connect();
        try {
            Layout newLayout = r.layout.get();
            newLayout.getSegment(0L).setReplicationMode(Layout.ReplicationMode.CHAIN_REPLICATION);
            newLayout.setEpoch(1);
            r.getLayoutView().committed(1L, newLayout);
            r.invalidateLayout();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return r;
    }


}
