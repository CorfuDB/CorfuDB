package org.corfudb.runtime.view;

import com.esotericsoftware.kryo.Kryo;
import org.corfudb.runtime.entries.BundleEntry;
import org.corfudb.runtime.entries.CorfuDBStreamEntry;
import org.corfudb.runtime.entries.CorfuDBStreamHoleEntry;
import org.corfudb.runtime.entries.CorfuDBStreamMoveEntry;
import org.corfudb.runtime.entries.CorfuDBStreamStartEntry;
import org.corfudb.runtime.stream.Timestamp;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.esotericsoftware.kryo.io.UnsafeInput;

import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Set;
import java.util.Map;
import java.util.HashSet;

import org.corfudb.runtime.smr.SMRCommandWrapper;
import org.corfudb.runtime.smr.TxDec;
import org.corfudb.runtime.smr.TxInt;
import org.corfudb.runtime.smr.TxIntWriteSetEntry;
import org.corfudb.runtime.smr.TxIntReadSetEntry;
import org.corfudb.runtime.stream.ITimestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import org.corfudb.runtime.smr.Triple;
import org.corfudb.runtime.smr.Pair;

/** This class provides helpers for serialization.
 */
public class Serializer
{
    /** Register classes for serialization. Add the classes you use here to increase the speed
     * of serialization/deserialization.
     */
    static void registerSerializer(Kryo k)
    {
        k.register(BundleEntry.class);
        k.register(CorfuDBStreamEntry.class);
        k.register(CorfuDBStreamHoleEntry.class);
        k.register(CorfuDBStreamMoveEntry.class);
        k.register(CorfuDBStreamStartEntry.class);
        k.register(Timestamp.class);
        k.register(HashMap.class);
        k.register(UUID.class);
        k.register(AtomicLong.class);
        k.register(Set.class);
        k.register(Map.class);
        k.register(SMRCommandWrapper.class);
        k.register(ITimestamp.class);
        k.register(LinkedList.class);
        k.register(TxDec.class);
        k.register(TxInt.class);
        k.register(TxIntWriteSetEntry.class);
        k.register(TxIntReadSetEntry.class);
        k.register(ArrayList.class);
        k.register(Triple.class);
        k.register(HashSet.class);
        k.register(Pair.class);
    }

    /** Get a thread local kryo instance for desrialization */
    public static ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            Serializer.registerSerializer(kryo);
            return kryo;
        };
    };

    /** Deserialize a byte array.
     *
     * @param data      The array to be deserialized.
     *
     * @return          The deserialized object.
     */
    public static Object deserialize(byte[] data)
        throws IOException, ClassNotFoundException
    {
        Object o;
        Kryo k = kryos.get();
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data))
        {
            try (UnsafeInput i = new UnsafeInput(bis, 16384))
            {
                o = k.readClassAndObject(i);
                return o;
            }
        }
    }

    /** Serialize a object.
     *
     * @param o      The object to be serialized.
     *
     * @return       The serialized object byte array.
     */
    public static byte[] serialize (Object o)
        throws IOException
    {
        Kryo k = kryos.get();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream())
        {
            try (UnsafeOutput output = new UnsafeOutput(baos, 16384))
            {
                k.writeClassAndObject(output, o);
                output.flush();
                return baos.toByteArray();
            }
        }
    }

    /** Deserialize an object using compresssion.
     *
     * @param data      The array to be deserialized.
     *
     * @return          The deserialized object.
     */
    public static Object deserialize_compressed(byte[] data)
        throws IOException, ClassNotFoundException
    {
        Object o;
        Kryo k = kryos.get();
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data))
        {
            try (InflaterInputStream iis = new InflaterInputStream(bis))
            {
                try (UnsafeInput i = new UnsafeInput(iis, 16384))
                {
                    o = k.readClassAndObject(i);
                    return o;
                }
            }
        }
    }

    /** Serialize an object using compression.
     * @param o     The object to be serialized.
     *
     * @return      The serialized byte array.
     */
    public static byte[] serialize_compressed (Object o)
        throws IOException
    {
        Kryo k = kryos.get();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream())
        {
            try (DeflaterOutputStream dos = new DeflaterOutputStream(baos))
            {
                try (UnsafeOutput output = new UnsafeOutput(dos, 16384))
                {
                    k.writeClassAndObject(output, o);
                    output.flush();
                }
                dos.finish();
            }
            return baos.toByteArray();
        }
    }


}
