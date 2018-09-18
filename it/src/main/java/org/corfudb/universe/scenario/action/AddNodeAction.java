package org.corfudb.universe.scenario.action;

import lombok.Data;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.LocalCorfuClient;
import org.corfudb.universe.scenario.action.Action.AbstractAction;
import org.corfudb.universe.universe.Universe;

/**
 * Add a corfu server into the corfu {@link Universe}
 */
@Data
public class AddNodeAction extends AbstractAction<Layout> {
    private String groupName;
    private String mainServerName;
    private String nodeName;

    @Override
    public Layout execute() {
        Group group = universe.getGroup(groupName);
        CorfuServer mainServer = group.getNode(mainServerName);

        LocalCorfuClient corfuClient = LocalCorfuClient.builder()
                .serverParams(mainServer.getParams())
                .build()
                .deploy();

        CorfuServer candidate = group.getNode(nodeName);
        corfuClient.add(candidate);

        return corfuClient.getLayout();
    }
}
