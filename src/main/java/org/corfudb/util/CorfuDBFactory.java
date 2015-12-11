package org.corfudb.util;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.IStreamFactory;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.view.*;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

/**
 * Created by mwei on 5/1/15.
 */
public class CorfuDBFactory {

    Map<String, Object> options;

    public CorfuDBFactory(Map<String, Object> options)
    {
        this.options = options;
    }

    CorfuDBRuntime runtime;

    public CorfuDBRuntime getRuntime()
    {
        if (runtime == null)
        {
            runtime = CorfuDBRuntime.getRuntime((String) options.get("--master"));
        }
        return runtime;
    }





}
