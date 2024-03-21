package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ILogReplicationVersionAdapter;
import org.corfudb.runtime.collections.TxnContext;

/**
 * Rolling upgrade handling means cluster must function in a mode where not all
 * nodes are running the same codebase.
 * Here the newer code base must function in a backward compatible mode until all
 * the nodes have been upgraded to the same version.
 * Once all nodes have upgraded it may atomically migrate data, clean up old code and switch
 * completely to newer logic.
 *
 * To facilitate this in LR we add this module which would run once on startup as shown in the
 * following diagram.
 * In addition to running on startup, if cluster is in a mixed mode, all the callers who are
 * mutating data in a new format need to check if upgrade is on and mutate data in a backward
 * compatible manner.
 *
 * For anything post corfu-0.4.0.1 we consider the code base to be at "V2"
 * Anything prior to and including corfu-0.4.0.1 is V1
 * Any code shipped that uses Source/Sink can be thought of as "V2"
 *                  ┌─────────────────────────────────────────────┐
 *                  │ Start transaction to modify new format data │
 *                  └──────────────────┬──────────────────────────┘
 *                                     │
 *                     ┌───────────────▼──────────┐
 *                   ┌─┤  isRolling UpgradeON?    ├─┐
 *         Yes ┌─────┴─┴┐  (migrate if done)     ┌┴─┴─────────────┐ No
 *             │        └────────────────────────┘                │
 *             │                                                  │
 * ┌───────────▼────────────────────┐                     ┌───────▼───────────────────────┐
 * │ Write in both old & new format │                     │ write data in new format only │
 * └───────────┬────────────────────┘                     └───────┬───────────────────────┘
 *             │            ┌──────────────────────┐              │
 *             └───────────►│ end transaction      │◄─────────────┘
 *                          └──────────────────────┘
 */
@Slf4j
public class LRRollingUpgradeHandler {

    private volatile boolean isClusterAllAtV2 = false;
    ILogReplicationVersionAdapter versionAdapter;
    public LRRollingUpgradeHandler(ILogReplicationVersionAdapter versionAdapter) {
        this.versionAdapter = versionAdapter;
    }

    public boolean isLRUpgradeInProgress(TxnContext txnContext) {
        if (isClusterAllAtV2) {
            return false;
        }
        String nodeVersion = versionAdapter.getNodeVersion();
        /**
         * The ideal way to check the versions is to encapsulate the code version
         * into Corfu's Layout information so that even when nodes are down
         * or unresponsive it would be possible to determine if rolling upgrade
         * is running. But since that is a bigger change we resort to the
         * boolean check while ensuring migrateData() is idempotent and is a NO-OP
         * when invoked on a fully upgraded cluster.
         */
        String pinnedClusterVersion = versionAdapter.getPinnedClusterVersion(txnContext);
        boolean isClusterUpgradeInProgress = !nodeVersion.equals(pinnedClusterVersion);
        if (isClusterUpgradeInProgress) {
            return true;
        } // else implies cluster upgrade has completed

        log.info("LRRollingUpgrade upgrade completed to version {}", nodeVersion);
        migrateData(txnContext);
        isClusterAllAtV2 = true;
        return false;
    }

    /**
     * This is the primary function where data is migrated by
     * 1. reading the old format
     * 2. re-writing the data in new format
     * 3. deleting the data in the old format or dropping the old tables
     *
     * @param txnContext All of the above must execute in the same transaction passed in.
     */
    public void migrateData(TxnContext txnContext) {
        // Data migration to be added here.
    }
}
