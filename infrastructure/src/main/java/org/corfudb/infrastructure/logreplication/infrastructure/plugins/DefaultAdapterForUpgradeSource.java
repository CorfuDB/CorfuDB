package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.runtime.CorfuRuntime;

import java.lang.reflect.InvocationTargetException;

/**
 * This class is created for LR rolling upgrade tests to serve as the plugin for Source cluster to
 * get the set of streams to replicate.
 */
public class DefaultAdapterForUpgradeSource extends DefaultAdapterForUpgrade {
    public DefaultAdapterForUpgradeSource(CorfuRuntime runtime)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        super(runtime);
    }
}
