package org.corfudb.common.remotecorfutable;

import com.google.protobuf.ByteString;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Objects;

/**
 * The RemoteCorfuTableDatabaseEntry provides an interface for storing key-value pairs for the
 * Remote Corfu Table.
 * <p>Created by nvaishampayan517 on 8/5/21.
 */
@EqualsAndHashCode
@AllArgsConstructor
@Getter
public class RemoteCorfuTableDatabaseEntry {
    private final RemoteCorfuTableVersionedKey key;
    private final ByteString value;
}
