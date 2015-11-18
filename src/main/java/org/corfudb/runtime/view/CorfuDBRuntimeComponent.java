package org.corfudb.runtime.view;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * Created by dalia on 11/15/15.
 */
public class CorfuDBRuntimeComponent {
    protected ICorfuDBInstance corfuInstance;
    protected UUID logID;
    private CorfuDBView view;
    protected Supplier<CorfuDBView> getView;

    CorfuDBRuntimeComponent(ICorfuDBInstance corfuInstance) {
        this.corfuInstance = corfuInstance;
        this.view = null;
        this.logID = null;

        this.getView = () -> {
            if (this.view == null) {
                view = corfuInstance.getView();
                this.logID = view.getLogID();
            }
            return this.view;
        };

    }
}
