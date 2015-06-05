package org.corfudb.runtime.stream;

import java.util.UUID;

/**
 * Created by mwei on 6/3/15.
 */
public final class ReservedStreamID {

    /* hidden private constructor */
    private ReservedStreamID() {}

    public static final UUID NULL = UUID.fromString("00000000-0000-0000-0000-000000000000");

    public static final UUID MetadataStream = UUID.fromString("AAAAAAAA-0000-0000-0000-000000000000");
}
