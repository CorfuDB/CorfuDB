package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.view.CachedWriteOnceAddressSpace;

/**
 * Created by crossbach on 4/3/15.
 */
public class StreamFactory {

    public enum StreamImplType {
        DUMMY ("DUMMY"),
        HOP ("HOP"),
        MEMORY ("MEMORY");
        private final String name;
        StreamImplType(String s) { name = s; }
        private static StreamImplType[] s_vals = values();
        private static StreamImplType fromInt(int i) { return s_vals[i]; }
        private static StreamImplType fromString(String s) {
            for (StreamImplType t : s_vals)
                if(t.name.compareToIgnoreCase(s) == 0)
                    return t;
            throw new RuntimeException("unknown stream implementation");
        }
        public String toString() { return name; }
    }

    /**
     * get a new stream factory for the given
     * client, of the specified type
     * @param client
     * @param type
     * @return
     */
    public static IStreamFactory
    getStreamFactory(
            CorfuDBRuntime client,
            StreamImplType type
        ) {
        switch(type) {
            case DUMMY: return new IStreamFactoryImpl(new CachedWriteOnceAddressSpace(client), new CorfuStreamingSequencer(client));
            case HOP: return new HopAdapterIStreamFactoryImpl(client);
            case MEMORY: return new MemoryStreamFactoryImpl(false, false);
            default:
                throw new RuntimeException("unknown stream implementation");
        }
    }

    public static IStreamFactory getStreamFactory(CorfuDBRuntime c) { return getStreamFactory(c, StreamImplType.DUMMY); }
    public static IStreamFactory getStreamFactory(CorfuDBRuntime c, int i) { return getStreamFactory(c, StreamImplType.fromInt(i)); }
    public static IStreamFactory getStreamFactory(CorfuDBRuntime c, String s) { return getStreamFactory(c, StreamImplType.fromString(s)); }

}
