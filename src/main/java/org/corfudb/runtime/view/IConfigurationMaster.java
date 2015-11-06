package org.corfudb.runtime.view;

import org.corfudb.runtime.exceptions.NetworkException;

/**
 * Created by mwei on 5/1/15.
 */
public interface IConfigurationMaster {

    void resetAll();
    void requestReconfiguration(NetworkException e);
    void forceNewView(CorfuDBView v);
}
