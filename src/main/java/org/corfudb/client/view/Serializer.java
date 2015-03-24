package org.corfudb.client.view;

import com.esotericsoftware.kryo.Kryo;
import org.corfudb.client.entries.BundleEntry;
import org.corfudb.client.entries.CorfuDBStreamEntry;
import org.corfudb.client.entries.CorfuDBStreamHoleEntry;
import org.corfudb.client.entries.CorfuDBStreamMoveEntry;
import org.corfudb.client.entries.CorfuDBStreamStartEntry;
import org.corfudb.client.Timestamp;

import java.util.HashMap;
public class Serializer
{
    static void registerSerializer(Kryo k)
    {
        k.register(BundleEntry.class);
        k.register(CorfuDBStreamEntry.class);
        k.register(CorfuDBStreamHoleEntry.class);
        k.register(CorfuDBStreamMoveEntry.class);
        k.register(CorfuDBStreamStartEntry.class);
        k.register(Timestamp.class);
        k.register(HashMap.class);
    }

    public static ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            Serializer.registerSerializer(kryo);
            return kryo;
        };
    };

}
