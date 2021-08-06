package org.corfudb.common.remotecorfutable;

import com.google.protobuf.ByteString;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Objects;

/**
 * The RemoteCorfuTableEntry provides an interface for storing key-value pairs for the
 * Remote Corfu Table.
 * <p>Created by nvaishampayan517 on 8/5/21.
 */
@AllArgsConstructor
@Getter
public class RemoteCorfuTableEntry {
    private final RemoteCorfuTableVersionedKey key;
    private final ByteString value;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteCorfuTableEntry that = (RemoteCorfuTableEntry) o;
        return key.equals(that.key) && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}
