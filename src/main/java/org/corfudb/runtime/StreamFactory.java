package org.corfudb.runtime;

import org.corfudb.client.CorfuDBClient;

import java.util.HashMap;

/**
 * Created by crossbach on 4/3/15.
 */
public class StreamFactory {

    public enum StreamImplType {
        DUMMY ("DUMMY"),
        HOP ("HOP");
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
            CorfuDBClient client,
            StreamImplType type
        ) {
        switch(type) {
            case DUMMY: return new IStreamFactoryImpl(new CorfuLogAddressSpace(client, 0), new CorfuStreamingSequencer(client));
            case HOP: return new HopAdapterIStreamFactoryImpl(client);
            default:
                throw new RuntimeException("unknown stream implementation");
        }
    }

    public static IStreamFactory getStreamFactory(CorfuDBClient c) { return getStreamFactory(c, StreamImplType.DUMMY); }
    public static IStreamFactory getStreamFactory(CorfuDBClient c, int i) { return getStreamFactory(c, StreamImplType.fromInt(i)); }
    public static IStreamFactory getStreamFactory(CorfuDBClient c, String s) { return getStreamFactory(c, StreamImplType.fromString(s)); }

}
