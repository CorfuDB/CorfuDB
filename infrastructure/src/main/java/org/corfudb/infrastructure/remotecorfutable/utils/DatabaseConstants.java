package org.corfudb.infrastructure.remotecorfutable.utils;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * This class holds constants for usage in the database backing RemoteCorfuTables.
 *
 * <p>Created by nvaishampayan517 on 7/27/21.
 */
public final class DatabaseConstants {
    public static final Charset DATABASE_CHARSET = StandardCharsets.UTF_8;
    public static final byte[] METADATA_COLUMN_SUFFIX = "_mtd".getBytes(DATABASE_CHARSET);
    public static final byte[] SIZE_KEY = "size".getBytes(DATABASE_CHARSET);
    public static final byte[] LATEST_VERSION_READ = "latest-version".getBytes(DATABASE_CHARSET);
    public static final byte[] EMPTY_VALUE = new byte[0];

    public static final long METADATA_COLUMN_CACHE_SIZE = 64;
    //prevent instantiation
    private DatabaseConstants() {}

    public static boolean isEmpty(byte[] arr) {
        return arr.length == 0;
    }
}
