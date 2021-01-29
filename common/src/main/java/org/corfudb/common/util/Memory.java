package org.corfudb.common.util;

import org.ehcache.sizeof.SizeOf;

final public class Memory {

    private Memory() {}

    public static final SizeOf sizeOf = SizeOf.newInstance();
}
