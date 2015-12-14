package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

/**
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class LayoutView extends AbstractView {

    public LayoutView(CorfuRuntime runtime)
    {
        super(runtime);
    }

    public Layout getLayout() {
        return layoutHelper(l -> {
            return l;
        });
    }


}
