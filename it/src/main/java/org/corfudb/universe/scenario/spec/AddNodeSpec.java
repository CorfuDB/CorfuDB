package org.corfudb.universe.scenario.spec;

import lombok.Data;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.scenario.spec.Spec.AbstractSpec;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.Universe.UniverseParams;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Check the {@link Universe} size
 */
@Data
public class AddNodeSpec extends AbstractSpec<UniverseParams, Layout> {
    private int clusterSize;

    public AddNodeSpec() {
        super("A corfu node should be added into the universe. The universe size must be equal to expected clusterSize");
    }

    @Override
    public void check(UniverseParams input, Layout layout) {
        assertThat(layout.getAllActiveServers().size()).isEqualTo(clusterSize);
    }
}
