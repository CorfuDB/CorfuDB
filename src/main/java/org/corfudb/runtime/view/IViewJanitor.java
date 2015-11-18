package org.corfudb.runtime.view;

import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;

import javax.json.JsonObject;

/**
 * Created by mwei on 5/1/15.
 */
public interface IViewJanitor {

    CorfuDBView getView();
    IServerProtocol isViewAccessible();
    void resetAll();
    void reconfig(NetworkException e);
    public void driveReconfiguration(JsonObject newLayout);
}
