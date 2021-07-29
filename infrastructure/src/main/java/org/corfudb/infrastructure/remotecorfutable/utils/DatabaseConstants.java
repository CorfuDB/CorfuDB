package org.corfudb.infrastructure.remotecorfutable.utils;

import com.google.protobuf.ByteString;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * This class holds constants for usage in the database backing RemoteCorfuTables.
 *
 * <p>Created by nvaishampayan517 on 7/27/21.
 */
public final class DatabaseConstants {
    public static final Charset DATABASE_CHARSET = StandardCharsets.UTF_8;
    public static final ByteString METADATA_COLUMN_SUFFIX = ByteString.copyFrom("_mtd", DATABASE_CHARSET);
    public static final byte[] SIZE_KEY = "size".getBytes(DATABASE_CHARSET);
    public static final byte[] LATEST_VERSION_READ = "latest-version".getBytes(DATABASE_CHARSET);

    public static final long METADATA_COLUMN_CACHE_SIZE = 64;
    //prevent instantiation
    private DatabaseConstants() {}
}
