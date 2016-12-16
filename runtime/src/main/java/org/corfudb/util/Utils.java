package org.corfudb.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.instrument.Instrumentation;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import jdk.internal.org.objectweb.asm.ClassReader;
import jdk.internal.org.objectweb.asm.tree.AbstractInsnNode;
import jdk.internal.org.objectweb.asm.tree.ClassNode;
import jdk.internal.org.objectweb.asm.tree.InsnList;
import jdk.internal.org.objectweb.asm.tree.MethodNode;
import jdk.internal.org.objectweb.asm.util.Printer;
import jdk.internal.org.objectweb.asm.util.Textifier;
import jdk.internal.org.objectweb.asm.util.TraceMethodVisitor;

import lombok.extern.slf4j.Slf4j;

import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;


/**
 * Created by crossbach on 5/22/15.
 */
@Slf4j
public class Utils {
    private static final Instrumentation instrumentation = ByteBuddyAgent.install();
    private static Printer printer = new Textifier();
    private static TraceMethodVisitor mp = new TraceMethodVisitor(printer);

    public static byte[] getByteCodeOf(Class<?> c) {
        try {
            ClassFileLocator locator = ClassFileLocator.AgentBased.of(instrumentation, c);
            TypeDescription.ForLoadedType desc = new TypeDescription.ForLoadedType(c);
            ClassFileLocator.Resolution resolution = locator.locate(desc.getName());
            return resolution.resolve();
        } catch (IOException ie) {
            throw new RuntimeException("Couldn't get byte code " + ie.toString(), ie);
        }
    }

    public static String printByteCode(byte[] bytes) {
        ClassReader cr = new ClassReader(bytes);
        ClassNode cn = new ClassNode();
        cr.accept(cn, 0);
        final List<MethodNode> methods = cn.methods;
        StringBuilder sb = new StringBuilder();
        for (MethodNode m : methods) {
            InsnList inList = m.instructions;
            sb.append(m.name);
            for (int i = 0; i < inList.size(); i++) {
                sb.append(insnToString(inList.get(i)));
            }
        }
        return sb.toString();
    }

    public static String insnToString(AbstractInsnNode insn) {
        insn.accept(mp);
        StringWriter sw = new StringWriter();
        printer.print(new PrintWriter(sw));
        printer.getText().clear();
        return sw.toString();
    }

    /**
     * A fancy parser which parses suffixes.
     *
     * @param toParseObj
     * @return
     */
    public static long parseLong(final Object toParseObj) {
        if (toParseObj == null) {
            return 0;
        }
        if (toParseObj instanceof Long) {
            return (Long) toParseObj;
        }
        if (toParseObj instanceof Integer) {
            return (Integer) toParseObj;
        }
        String toParse = (String) toParseObj;
        if (toParse.matches("[0-9]*[A-Za-z]$")) {
            long multiplier;
            char suffix = toParse.toUpperCase().charAt(toParse.length() - 1);
            switch (suffix) {
                case 'E':
                    multiplier = 1_000_000_000_000_000_000L;
                    break;
                case 'P':
                    multiplier = 1_000_000_000_000_000L;
                    break;
                case 'T':
                    multiplier = 1_000_000_000_000L;
                    break;
                case 'G':
                    multiplier = 1_000_000_000L;
                    break;
                case 'M':
                    multiplier = 1_000_000L;
                    break;
                case 'K':
                    multiplier = 1_000L;
                    break;
                default:
                    throw new NumberFormatException("Unknown suffix: '" + suffix + "'!");
            }
            return Long.parseLong(toParse.substring(0, toParse.length() - 2)) * multiplier;
        } else {
            return Long.parseLong(toParse);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T getOption(Map<String, Object> optionsMap, String option, Class<T> type, T defaultValue) {
        T obj = (T) optionsMap.get(option);
        if (type == Long.class) {
            if (obj == null && defaultValue != null) {
                return defaultValue;
            }
            return (T) (Long) parseLong(obj);
        } else if (type == Integer.class) {
            if (obj == null && defaultValue != null) {
                return defaultValue;
            }
            return (T) (Integer) ((Long) parseLong(obj)).intValue();
        }
        if (obj == null) {
            return defaultValue;
        }
        return obj;
    }

    public static <T> T getOption(Map<String, Object> optionsMap, String option, Class<T> type) {
        return getOption(optionsMap, option, type, null);
    }

    /**
     * Turn a range into a set of discrete longs.
     *
     * @param range The range to discretize.
     * @return A set containing all the longs in that range.
     */
    public static Set<Long> discretizeRange(Range<Long> range) {
        Set<Long> s = new HashSet<>();
        for (long l = range.lowerEndpoint(); l <= range.upperEndpoint(); l++) {
            if (range.contains(l)) {
                s.add(l);
            }
        }
        return s;
    }

    /**
     * Turn a set of ranges into a discrete set.
     *
     * @param ranges A set of ranges to discretize.
     * @return A set containing all the longs in that rangeset.
     */
    public static Set<Long> discretizeRangeSet(RangeSet<Long> ranges) {
        Set<Long> total = Collections.newSetFromMap(new ConcurrentHashMap<>());
        for (Range<Long> r : ranges.asRanges()) {
            total.addAll(Utils.discretizeRange(r));
        }
        return total;
    }

    /**
     * Convert to byte string representation.
     * from http://stackoverflow.com/questions/3758606/how-to-convert-byte-size-into-human-readable-format-in-java
     *
     * @param value The value to convert.
     * @return A string for bytes (i.e, 10GB).
     */
    public static String convertToByteStringRepresentation(final long value) {
        final long[] dividers = new long[]{1_000_000_000_000L, 1_000_000_000, 1_000_000, 1_000, 1};
        final String[] units = new String[]{"TB", "GB", "MB", "KB", "B"};
        if (value < 1)
            throw new IllegalArgumentException("Invalid file size: " + value);
        String result = null;
        for (int i = 0; i < dividers.length; i++) {
            final long divider = dividers[i];
            if (value >= divider) {
                final double cresult =
                        divider > 1 ? (double) value / (double) divider : (double) value;
                result = new DecimalFormat("#,##0.#").format(cresult) + " " + units[i];
                break;
            }
        }
        return result;
    }

    public static ByteBuffer serialize(Object obj) {
        try {
            //todo: make serialization less clunky!
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            byte b[] = baos.toByteArray();
            oos.close();
            return ByteBuffer.wrap(b);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object deserialize(ByteBuffer b) {
        try {
            //todo: make serialization less clunky!
            ByteArrayInputStream bais = new ByteArrayInputStream(b.array());
            ObjectInputStream ois = new ObjectInputStream(bais);
            Object obj = ois.readObject();
            return obj;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException ce) {
            throw new RuntimeException(ce);
        }
    }

    static long rotl64(long x, int r) {
        return (x << r) | (x >> (64 - r));
    }

    static long fmix64(long k) {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdl;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53l;
        k ^= k >> 33;
        return k;
    }

    /**
     * murmer hash 3 implementation specialized for UUIDs,
     * based on googlecode C implementation from:
     * http://smhasher.googlecode.com/svn/trunk/MurmurHash3.cpp
     *
     * @param key
     * @param seed
     * @return
     */
    public static UUID
    murmerhash3(
            UUID key,
            long seed
    ) {
        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        byte[] data = new byte[8];
        data[7] = (byte) (lsb & 0xFF);
        data[6] = (byte) ((lsb >> 8) & 0xFF);
        data[5] = (byte) ((lsb >> 16) & 0xFF);
        data[4] = (byte) ((lsb >> 24) & 0xFF);
        data[3] = (byte) (msb & 0xFF);
        data[2] = (byte) ((msb >> 8) & 0xFF);
        data[1] = (byte) ((msb >> 16) & 0xFF);
        data[0] = (byte) ((msb >> 24) & 0xFF);

        int nblocks = 2;
        long h1 = seed;
        long h2 = seed;
        long c1 = 0x87c37b91114253d5l;
        long c2 = 0x4cf5ad432745937fl;
        long[] blocks = new long[nblocks];
        blocks[0] = msb;
        blocks[1] = lsb;
        for (int i = 0; i < nblocks; i++) {
            long k1 = blocks[i * 2 + 0];
            long k2 = blocks[i * 2 + 1];
            k1 *= c1;
            k1 = rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;
            h1 = rotl64(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;
            k2 *= c2;
            k2 = rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;
            h2 = rotl64(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }
        h1 ^= 2;
        h2 ^= 2;
        h1 += h2;
        h2 += h1;
        h1 = fmix64(h1);
        h2 = fmix64(h2);
        h1 += h2;
        h2 += h1;
        return new UUID(h2, h1);
    }

    /**
     * simple UUID hashing, which is *not* hashing, and is effectively
     * customized to the task of deterministically allocating new UUIDs
     * based on a given UUID (which is necessary in the assignment of stream
     * IDs in ICOrfuDBObjects that contain others, since syncing the log in
     * multiple clients needs allocators to produce the same streamID/object
     * every time).
     *
     * @param key
     * @param seed
     * @return
     */
    public static UUID
    simpleUUIDHash(UUID key, long seed) {
        return new UUID(key.getMostSignificantBits(), key.getLeastSignificantBits() + seed);
    }

    public static UUID nextDeterministicUUID(UUID uuid, long seed) {
        return simpleUUIDHash(uuid, seed);
    }


    /** restart the JVM - borrowed from https://dzone.com/articles/programmatically-restart-java */
    /**
     * Sun property pointing the main class and its arguments.
     * Might not be defined on non Hotspot VM implementations.
     */
    public static final String SUN_JAVA_COMMAND = "sun.java.command";

    public static void sleepUninterruptibly(long millis) {
        while (true) {
            try {
                Thread.sleep(millis);
                return;
            } catch (InterruptedException ie) {
            }
        }
    }
}
