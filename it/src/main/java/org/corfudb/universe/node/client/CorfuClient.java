package org.corfudb.universe.node.client;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutView;
import org.corfudb.runtime.view.ManagementView;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.universe.node.Node;

/**
 * Represent a Corfu client implementation of {@link Node}.
 */
public interface CorfuClient extends Node {
    ClientParams getParams();

    /**
     * Create a default CorfuTable object for data-path
     * verification in scenario tests
     *
     * @param streamName stream name of the table
     * @return CorfuTable object created by runtime
     */
    CorfuTable createDefaultCorfuTable(String streamName);

    /**
     * See {@link CorfuRuntime#connect()}
     */
    void connect();

    /**
     * See {@link LayoutView#getLayout()}
     */
    Layout getLayout();

    /**
     * See {@link CorfuRuntime#getManagementView()}
     */
    ManagementView getManagementView();

    /**
     * See {@link CorfuRuntime#getObjectsView()}
     */
    ObjectsView getObjectsView();

    /**
     * See {@link CorfuRuntime#invalidateLayout()}
     */
    void invalidateLayout();

    /**
     * A handler which is invoked at any point when the Corfu client
     * attempts to make an RPC request to the Corfu cluster but is
     * unable to complete. This method can be helpful in tests where
     * the it is needed to assert that the cluster is unavailable
     */
    default void systemDownHandler() {
        throw new UnreachableClusterException("Cluster is unavailable");
    }

}
