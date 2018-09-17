package org.corfudb.universe.scenario.action;

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
    private String mainServerName;
    private String nodeName;

    @Override
    public Layout execute() {
        Service service = cluster.getService(serviceName);
        CorfuServer mainServer = service.getNode(mainServerName);
        mainServer.connectCorfuRuntime();

        CorfuServer candidate = service.getNode(nodeName);
        mainServer.addNode(candidate);

        return mainServer.getLayout();
    }
}
