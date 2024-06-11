package org.corfudb.runtime.collections;

import com.google.protobuf.DynamicMessage;
import lombok.Getter;

import java.util.Objects;

/**
 * Contains the key as {@link CorfuDynamicMessage} after extracting it from {@link com.google.protobuf.Any}.
 * This wrapper contains the payload and the respective typeUrl required to populate the Any on serialization.
 * This object is stored as the key in the CorfuTable while viewing in the schema-less mode.
 * <p>
 * Created by zlokhandwala on 10/7/19.
 */
public class CorfuDynamicKey {

    /**
     * TypeUrl generated on serialization and populated in the Any message.
     */
    @Getter
    private final String keyTypeUrl;

    /**
     * Payload of the key stored in the CorfuTable.
     */
    private final CorfuDynamicMessage key;

    public CorfuDynamicKey(String keyTypeUrl, DynamicMessage key) {
        this.keyTypeUrl = keyTypeUrl;
        this.key = new CorfuDynamicMessage(key);
    }

    public DynamicMessage getKey() {
        return key.getPayload();
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyTypeUrl, key);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CorfuDynamicKey)) {
            return false;
        }
        CorfuDynamicKey corfuDynamicKey = (CorfuDynamicKey) obj;
        return keyTypeUrl.equals(corfuDynamicKey.keyTypeUrl)
                && key.equals(corfuDynamicKey.key);

    }
}
