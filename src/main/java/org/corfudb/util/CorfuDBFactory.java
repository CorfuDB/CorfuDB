package org.corfudb.util;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.IStreamFactory;
import org.corfudb.runtime.stream.ILog;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.SimpleLog;
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

    @SuppressWarnings("unchecked")
    public IWriteOnceAddressSpace getWriteOnceAddressSpace(CorfuDBRuntime cdr)
    {
        try {
            String type = (String) options.get("--address-space");
            Class<? extends IWriteOnceAddressSpace> addressSpaceClass = (Class<? extends IWriteOnceAddressSpace>) Class.forName("org.corfudb.runtime.view." + type);
            if (!Arrays.asList(addressSpaceClass.getInterfaces()).contains(IWriteOnceAddressSpace.class))
            {
                throw new Exception("Not a write once address space type");
            }
            return addressSpaceClass.getConstructor(CorfuDBRuntime.class).newInstance(cdr);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public IStreamingSequencer getStreamingSequencer(CorfuDBRuntime cdr)
    {
        return new StreamingSequencer(cdr);
    }

    public ILayoutMonitor getConfigurationMaster(CorfuDBRuntime cdr)
    {
        return new LayoutMonitor(cdr);
    }

    public ILog getLog(ISequencer sequencer, IWriteOnceAddressSpace woas)
    {
        return new SimpleLog(sequencer, woas);
    }

    public IStream getStream(UUID id, IStreamingSequencer sequencer, IWriteOnceAddressSpace woas)
    {
        try {
            String type = (String) options.getOrDefault("--stream-impl", "NewStream");
            Class<? extends IStream> streamClass;
            try {
                streamClass = (Class<? extends IStream>) Class.forName("org.corfudb.runtime.stream." + type);
            } catch(ClassNotFoundException cnfe) {
                streamClass = (Class<? extends IStream>) Class.forName("org.corfudb.runtime.stream.legacy." + type);
            }
            if (!Arrays.asList(streamClass.getInterfaces()).contains(IStream.class))
            {
                throw new Exception("Not a stream implementation type!");
            }
            Constructor ctor = streamClass.getConstructor(UUID.class, ISequencer.class, IWriteOnceAddressSpace.class, CorfuDBRuntime.class);
            return (IStream) ctor.newInstance(id, sequencer, woas, runtime);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public IStreamFactory getStreamFactory(IStreamingSequencer sequencer, IWriteOnceAddressSpace woas)
    {
        try {
            String type = (String) options.getOrDefault("--stream-impl", "NewStream");
            Class<? extends IStreamFactory> factoryClass;
            try {
                factoryClass = (Class<? extends IStreamFactory>) Class.forName("org.corfudb.runtime.stream." + type + "Factory");
            } catch(ClassNotFoundException cnfe) {
                factoryClass = (Class<? extends IStreamFactory>) Class.forName("org.corfudb.runtime.stream.legacy." + type + "Factory");
            }
            if (!Arrays.asList(factoryClass.getInterfaces()).contains(IStreamFactory.class))
            {
                throw new Exception("Not a stream implementation type!");
            }


            Constructor ctor = factoryClass.getConstructor(IWriteOnceAddressSpace.class, IStreamingSequencer.class, CorfuDBRuntime.class);
            return (IStreamFactory) ctor.newInstance(woas, sequencer, runtime);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

}
