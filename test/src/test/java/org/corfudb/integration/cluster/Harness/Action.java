package org.corfudb.integration.cluster.Harness;


/**
 * Definition of an action that can be applied to a node.
 * <p>
 * <p>Created by maithem on 7/18/18.
 */

abstract public class Action {

    protected Node node;

    public abstract void run() throws Exception ;

}
