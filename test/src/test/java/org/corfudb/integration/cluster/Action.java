package org.corfudb.integration.cluster;

import org.corfudb.integration.cluster.Node;
import org.corfudb.runtime.CorfuRuntime;

abstract public class Action {

    protected Node node;

    public abstract void run() throws Exception ;

}
