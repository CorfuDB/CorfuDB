package org.corfudb.runtime.view;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * Created by dalia on 11/15/15.
 */
public class CorfuDBRuntimeComponent {
    protected ICorfuDBInstance corfuInstance;
    protected UUID logID;
    protected CorfuDBView view;
    protected Supplier<CorfuDBView> getView;

    CorfuDBRuntimeComponent(ICorfuDBInstance corfuInstance) {
        this.corfuInstance = corfuInstance;
        this.view = corfuInstance.getView();
        this.getView = () -> {
            return this.view;
        };
        this.logID = view.getLogID();

    }
}
