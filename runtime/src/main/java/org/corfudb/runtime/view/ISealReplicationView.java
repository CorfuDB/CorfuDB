package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;

/**
 * Seals all the log unit servers in every stripe following a certain replication method.
 *
 * Created by zlokhandwala on 3/7/17.
 */
public interface ISealReplicationView {

    Boolean sealLayoutSegment(CorfuRuntime runtime, Layout.LayoutSegment layoutSegment, long sealEpoch) throws QuorumUnreachableException;
}
