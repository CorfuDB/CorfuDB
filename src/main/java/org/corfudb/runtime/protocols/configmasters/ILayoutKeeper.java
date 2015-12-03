package org.corfudb.runtime.protocols.configmasters;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.view.CorfuDBView;

import javax.json.JsonObject;
import java.util.concurrent.CompletableFuture;

/**
 * Created by dalia on 11/11/15.
 */
public interface ILayoutKeeper extends IServerProtocol {

    @Getter
    @Setter
    @AllArgsConstructor
    public class LayoutKeeperInfo {
        long epoch;
        long rank;
        JsonObject jo;
    }

    public CompletableFuture<JsonObject> getCurrentView();
    public CompletableFuture<LayoutKeeperInfo> proposeNewView(long rank, JsonObject jo);
    public CompletableFuture<LayoutKeeperInfo> collectView(long rank);

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
