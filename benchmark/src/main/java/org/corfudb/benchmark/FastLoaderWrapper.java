package org.corfudb.benchmark;

import com.google.common.reflect.TypeToken;
import org.corfudb.recovery.FastObjectLoader;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FastLoaderWrapper {

    public static void executeFastLoader(CorfuRuntime runtime, List<String> allTables) {

        log.info("Executing fast loader.");

        FastObjectLoader fastLoader = new FastObjectLoader(runtime)
                    .setBatchReadSize(runtime.getParameters().getBulkReadSize())
                    .setDefaultObjectsType(CorfuTable.class)
                    .setTimeoutInMinutesForLoading((int) runtime.getParameters().getFastLoaderTimeout().toMinutes());

        fastLoader.addStreamsToLoad(allTables);
        fastLoader.loadMaps();
    }
}
