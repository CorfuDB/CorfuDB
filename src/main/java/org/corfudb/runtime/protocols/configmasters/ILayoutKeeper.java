package org.corfudb.runtime.protocols.configmasters;

import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.view.CorfuDBView;

import javax.json.JsonObject;
import java.util.concurrent.CompletableFuture;

/**
 * Created by dalia on 11/11/15.
 */
public interface ILayoutKeeper extends IServerProtocol {

    public CompletableFuture<JsonObject> getCurrentView();
    public CompletableFuture<Boolean> proposeNewView(int rank, JsonObject jo);

        /**
         * Gets the current view from the configuration master.
         * @return  The current view.
         */
    CorfuDBView getView();

    /**
     * sets a bootstrap view at a particular MetaDataKeeper unit
     * @param initialView the initial view
     */
    void setBootstrapView(JsonObject initialView);
}
