package org.corfudb.universe.scenario.action;

import com.google.common.collect.ImmutableList;
import lombok.Data;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.scenario.action.Action.AbstractAction;
import org.corfudb.universe.service.Service;

/**
 * Add a corfu server into the corfu cluster
 */
@Data
public class AddNodeAction extends AbstractAction<Layout> {
    private String serviceName;
    private Integer candidateIndex;

    @Override
    public Layout execute() {
        Service service = cluster.getService(serviceName);
        ImmutableList<CorfuServer> nodes = service.nodes(CorfuServer.class);
        CorfuServer mainServer = nodes.get(0);
        mainServer.connectCorfuRuntime();
        CorfuServer candidate = service.nodes(CorfuServer.class).get(candidateIndex);

        mainServer.addNode(candidate);
        return mainServer.getLayout().get();
    }
}
