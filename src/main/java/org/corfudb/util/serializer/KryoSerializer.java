package org.corfudb.util.serializer;

import com.esotericsoftware.kryo.ClassResolver;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.*;
import com.esotericsoftware.kryo.util.IdentityObjectIntMap;
import com.esotericsoftware.kryo.util.IntMap;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.esotericsoftware.kryo.util.ObjectMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.entries.SimpleStreamEntry;
import org.corfudb.runtime.objects.CorfuObjectByteBuddyProxy;
import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.smr.legacy.TxDec;
import org.corfudb.runtime.smr.legacy.TxInt;
import org.corfudb.runtime.smr.legacy.TxIntReadSetEntry;
import org.corfudb.runtime.smr.legacy.TxIntWriteSetEntry;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleTimestamp;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static com.esotericsoftware.kryo.util.Util.className;
import static com.esotericsoftware.kryo.util.Util.getWrapperClass;

/**
 * Created by mwei on 9/17/15.
 */
@Slf4j
public class KryoSerializer implements ISerializer{

    @Override
    public Object deserialize(ByteBuf b) {
        Object o;
        Kryo k = kryos.get();
        try (ByteBufInputStream bbis = new ByteBufInputStream(b))
        {
            try (UnsafeInput i = new UnsafeInput(bbis, 16384))
            {
                o = k.readClassAndObject(i);
                return o;
            }
        }
        catch (IOException ie)
        {
            log.error("Exception during deserialization!", ie);
            return null;
        }
    }

    @Override
    public void serialize(Object o, ByteBuf b) {
        Kryo k = kryos.get();
        try (ByteBufOutputStream bbos = new ByteBufOutputStream(b))
        {
            try (UnsafeOutput output = new UnsafeOutput(bbos, 16384))
            {
                k.writeClassAndObject(output, o);
                output.flush();
            }
        }
        catch (IOException ie)
        {
            log.error("Exception during serialization!", ie);
        }
    }

    static class UUIDSerializer extends com.esotericsoftware.kryo.Serializer<UUID> {

        public UUIDSerializer() {
            setImmutable(true);
            setAcceptsNull(true);
        }

        @Override
        public void write(final Kryo kryo, final Output output, final UUID uuid) {
            if (uuid == null) {
                output.writeLong(0L);
                output.writeLong(0L);
            }
            else {
                output.writeLong(uuid.getMostSignificantBits());
                output.writeLong(uuid.getLeastSignificantBits());}
        }

        @Override public UUID read(final Kryo kryo, final Input input, final Class<UUID> uuidClass) {
            return new UUID(input.readLong(), input.readLong());
        }
    }

    static class NewCommandSerializer extends com.esotericsoftware.kryo.Serializer<Serializable> {

        private static Method readResolve;
        private static Class serializedLambda;
        static {
            try {
                serializedLambda = Class.forName("java.lang.invoke.SerializedLambda");
                readResolve = serializedLambda.getDeclaredMethod("readResolve");
                readResolve.setAccessible(true);
            } catch (Exception e) {
                throw new RuntimeException("Could not obtain SerializedLambda or its methods via reflection", e);
            }
        }

        public NewCommandSerializer () {
            setImmutable(true);
            setAcceptsNull(true);
        }

        @Override
        public void write (Kryo kryo, Output output, Serializable object) {
            try {
                Class type = object.getClass();
                Method writeReplace = type.getDeclaredMethod("writeReplace");
                writeReplace.setAccessible(true);
                Object replacement = writeReplace.invoke(object);
                if (serializedLambda.isInstance(replacement)) {
                    // Serialize the representation of this lambda
                    kryo.writeObject(output, replacement);
                } else
                    throw new RuntimeException("Could not serialize lambda");
            } catch (Exception e) {
                throw new RuntimeException("Could not serialize lambda", e);
            }
        }

        @Override
        public Serializable read (Kryo kryo, Input input, final Class<Serializable> type) {
            try {
                Object object = kryo.readObject(input, serializedLambda);
                return (Serializable) readResolve.invoke(object);
            } catch (Exception e) {
                throw new RuntimeException("Could not deserialize lambda", e);
            }
        }

        public Serializable copy (Kryo kryo, Serializable original) {
            try {
                Class type = original.getClass();
                Method writeReplace = type.getDeclaredMethod("writeReplace");
                writeReplace.setAccessible(true);
                Object replacement = writeReplace.invoke(original);
                if (serializedLambda.isInstance(replacement)) {
                    return (Serializable) readResolve.invoke(replacement);
                } else
                    throw new RuntimeException("Could not serialize lambda");
            } catch (Exception e) {
                throw new RuntimeException("Could not serialize lambda", e);
            }
        }
    }


    static class CorfuDBObjectSerializer extends com.esotericsoftware.kryo.Serializer<ICorfuDBObject> {


        public CorfuDBObjectSerializer () {
            setAcceptsNull(true);
        }

        @Override
        public void write (Kryo kryo, Output output, ICorfuDBObject object) {
            try {
                Class<?> type = object.getClass();
                kryo.writeObject(output, object.getStreamID());
            } catch (Exception e) {
                throw new RuntimeException("Could not serialize CDB object", e);
            }
        }

        @Override
        public ICorfuDBObject read (Kryo kryo, Input input, final Class<ICorfuDBObject> type) {
            try {
                ICorfuDBObject o = kryo.newInstance(type);
                //o.setStreamID(kryo.readObject(input, UUID.class));
                if (TransactionalContext.getTX() != null)
                {
                  //  o.setInstance(TransactionalContext.getTX().getInstance());
                }
                return o;
            } catch (Exception e) {
                throw new RuntimeException("Could not deserialize CDBObject", e);
            }
        }

        public ICorfuDBObject copy (Kryo kryo, ICorfuDBObject original) {
            try {
                return  original.getClass().getConstructor(UUID.class, ISMREngine.class)
                        .newInstance(original.getStreamID(), null);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Could not serialize CDB object", e);
            }
        }
    }
    /** Register classes for serialization. Add the classes you use here to increase the speed
     * of serialization/deserialization.
     */
    static void registerSerializer(Kryo k)
    {
        k.setInstantiatorStrategy(new StdInstantiatorStrategy());
        k.addDefaultSerializer(ICorfuDBObject.class, new CorfuDBObjectSerializer());
        k.register(SimpleStreamEntry.class);
        k.register(SimpleTimestamp.class);
        k.register(HashMap.class);
        k.register(UUID.class, new UUIDSerializer());
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
        k.register(ISMREngineCommand.class, new NewCommandSerializer());
    }

    /** Get a thread local kryo instance for deserialization */
    public static ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo(new ClassResolver() {
                static public final byte NAME = -1;

                protected Kryo kryo;

                protected final IntMap<Registration> idToRegistration = new IntMap();
                protected final ObjectMap<Class, Registration> classToRegistration = new ObjectMap();

                protected IdentityObjectIntMap<Class> classToNameId;
                protected IntMap<Class> nameIdToClass;
                protected ObjectMap<String, Class> nameToClass;
                protected int nextNameId;

                private int memoizedClassId = -1;
                private Registration memoizedClassIdValue;
                private Class memoizedClass;
                private Registration memoizedClassValue;

                @Override
                public void setKryo(Kryo kryo) {
                    this.kryo = kryo;
                }

                @Override
                public Registration register(Registration registration) {
                    if (registration == null) throw new IllegalArgumentException("registration cannot be null.");
                    if (registration.getId() != NAME) {
                        log.trace("Register class ID " + registration.getId() + ": " + className(registration.getType()) + " ("
                                + registration.getSerializer().getClass().getName() + ")");
                        idToRegistration.put(registration.getId(), registration);
                        classToRegistration.put(registration.getType(), registration);
                    } else if (registration.getType().getName().contains("$$Lambda$")) {
                        log.trace("Dropping registration for lambda: " + className(registration.getType()) + " ("
                                + registration.getSerializer().getClass().getName() + ")");
                        return getRegistration(ISMREngineCommand.class);
                    }
                    else{
                        log.trace("Register class name: " + className(registration.getType()) + " ("
                                + registration.getSerializer().getClass().getName() + ")");
                        classToRegistration.put(registration.getType(), registration);
                    }
                    if (registration.getType().isPrimitive()) classToRegistration.put(getWrapperClass(registration.getType()), registration);
                    return registration;
                }

                @Override
                public Registration registerImplicit(Class type) {
                    return register(new Registration(type, kryo.getDefaultSerializer(type), NAME));
                }

                @Override
                public Registration getRegistration(Class type) {
                    if (type == memoizedClass) return memoizedClassValue;
                    Registration registration = classToRegistration.get(type);
                    if (registration != null) {
                        memoizedClass = type;
                        memoizedClassValue = registration;
                    }
                    return registration;
                }

                @Override
                public Registration getRegistration(int classID) {
                    return idToRegistration.get(classID);
                }

                @Override
                public Registration writeClass(Output output, Class type) {
                    if (type == null) {
                        log.trace("Write");
                        output.writeVarInt(Kryo.NULL, true);
                        return null;
                    }

                    Registration registration;
                    if (className(type).contains("$$Lambda$"))
                    {
                        log.trace("Overwriting registration for lambda " + className(type));
                        registration = kryo.getRegistration(ISMREngineCommand.class);
                    }
                    else {
                        registration = kryo.getRegistration(type);
                    }
                    if (registration.getId() == NAME)
                    {
                        writeName(output, type, registration);
                    }
                    else {
                        log.trace("Write class " + registration.getId() + ": " + className(type));
                        output.writeVarInt(registration.getId() + 2, true);
                    }
                    return registration;
                }

                protected void writeName (Output output, Class type, Registration registration) {
                    output.writeVarInt(NAME + 2, true);
                    if (classToNameId != null) {
                        int nameId = classToNameId.get(type, -1);
                        if (nameId != -1) {
                            log.warn("Write class name reference " + nameId + ": " + className(type));
                            output.writeVarInt(nameId, true);
                            return;
                        }
                    }
                    // Only write the class name the first time encountered in object graph.
                    int nameId = nextNameId++;
                    log.warn("Write class name: " + className(type) + "id=" + nameId);
                    if (classToNameId == null) classToNameId = new IdentityObjectIntMap();
                    classToNameId.put(type, nameId);
                    output.writeVarInt(nameId, true);
                    String className = type.getName();
                    output.writeString(className);
                }

                @Override
                public Registration readClass(Input input) {
                    int classID = input.readVarInt(true);
                    switch (classID) {
                        case Kryo.NULL:
                            log.trace("Read");
                            return null;
                        case NAME + 2: // Offset for NAME and NULL.
                            return readName(input);
                    }
                    if (classID == memoizedClassId) return memoizedClassIdValue;
                    Registration registration = idToRegistration.get(classID - 2);
                    if (registration == null) throw new KryoException("Encountered unregistered class ID: " + (classID - 2));
                    log.warn("Read class " + (classID - 2) + ": " + className(registration.getType()));
                    memoizedClassId = classID;
                    memoizedClassIdValue = registration;
                    return registration;
                }

                protected Registration readName (Input input) {
                    int nameId = input.readVarInt(true);
                    if (nameIdToClass == null) nameIdToClass = new IntMap();
                    Class type = nameIdToClass.get(nameId);
                    if (type == null) {
                        // Only read the class name the first time encountered in object graph.
                        String className = input.readString();
                        type = getTypeByName(className);
                        if (type == null) {
                            try {
                                if (className.contains("$ByteBuddy$"))
                                {
                                    String undecoratedName = className.substring(0, className.indexOf("$ByteBuddy$"));
                                    type = Class.forName(undecoratedName, false, kryo.getClassLoader());
                                    /*
                                    log.info("Bytebuddy undecorated name: {}", undecoratedName);
                                    type =  CorfuObjectByteBuddyProxy.getProxy().getType(
                                            (Class<?>) Class.forName(undecoratedName),
                                            null,
                                            UUID.randomUUID());
                                            */
                                }
                                else {
                                    type = Class.forName(className, false, kryo.getClassLoader());
                                }
                            } catch (ClassNotFoundException ex) {
                                throw new KryoException("Unable to find class: " + className, ex);
                            }
                            if (nameToClass == null) nameToClass = new ObjectMap();
                            nameToClass.put(className, type);
                        }
                        nameIdToClass.put(nameId, type);
                        log.warn("Read class name: " + className);
                    } else {
                        log.warn("Read class name reference " + nameId + ": " + className(type));
                    }
                    return kryo.getRegistration(type);
                }

                protected Class<?> getTypeByName(final String className) {
                    return nameToClass != null ? nameToClass.get(className) : null;
                }

                @Override
                public void reset() {
                    if (!kryo.isRegistrationRequired()) {
                        if (classToNameId != null) classToNameId.clear();
                        if (nameIdToClass != null) nameIdToClass.clear();
                        nextNameId = 0;
                    }
                }
            }, new MapReferenceResolver());
            KryoSerializer.registerSerializer(kryo);
            return kryo;
        };
    };




    /** Serialize a object.
     *
     * @param o      The object to be serialized.
     *
     * @return       The serialized object byte array.
     */
    public ByteBuffer serializeBuffer (Object o)
            throws IOException
    {
        Kryo k = kryos.get();
        try (ByteBufferOutputStream baos = new ByteBufferOutputStream())
        {
            try (UnsafeOutput output = new UnsafeOutput(baos, 16384))
            {
                k.writeClassAndObject(output, o);
                output.flush();
                return baos.getByteBuffer();
            }
        }
    }

    /** Deserialize an object using compresssion.
     *
     * @param data      The array to be deserialized.
     *
     * @return          The deserialized object.
     */
    public Object deserialize_compressed(byte[] data)
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
    public byte[] serialize_compressed (Object o)
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

    /** Deep copy an object.
     *
     * @param o     The object to copy.
     * @return      A deep copy of the object.
     */
    public Object copy(Object o)
    {
        Kryo k = kryos.get();
        return k.copy(o);
    }

    /** Shallow copy an object.
     *
     * @param o
     * @return
     */
    public Object copyShallow(Object o)
    {
        Kryo k = kryos.get();
        return k.copyShallow(o);
    }
}
