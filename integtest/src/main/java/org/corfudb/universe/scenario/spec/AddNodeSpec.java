package org.corfudb.universe.scenario.spec;

import lombok.Data;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.cluster.Cluster.ClusterParams;
import org.corfudb.universe.scenario.spec.Spec.AbstractSpec;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Check cluster size
 */
@Data
public class AddNodeSpec extends AbstractSpec<ClusterParams, Layout> {
    private int clusterSize;

    public AddNodeSpec(){
        super("A corfu node should be added into the cluster. Cluster size must be equal to expected clusterSize");
    }

    @Override
    public void check(ClusterParams input, Layout layout) {
        assertThat(layout.getAllActiveServers().size()).isEqualTo(clusterSize);
    }
}
