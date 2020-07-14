package org.corfudb.integration.cluster.Harness;

/**
 * Definition of an action that can be applied to a node.
 *
 * <p>
 *
 * <p>Created by maithem on 7/18/18.
 */
public abstract class Action {

  protected Node node;

  public abstract void run() throws Exception;
}
