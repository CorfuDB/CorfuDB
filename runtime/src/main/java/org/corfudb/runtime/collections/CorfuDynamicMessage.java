package org.corfudb.runtime.collections;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import lombok.Getter;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Wrapper over the Protobuf Dynamic Message to override hashcode and equals.
 * <p>
 * Created by zlokhandwala on 10/7/19.
 */
public class CorfuDynamicMessage {

    /**
     * Dynamic Message payload.
     */
    @Getter
    private final DynamicMessage payload;

    CorfuDynamicMessage(@Nullable DynamicMessage payload) {
        this.payload = payload;
    }

    /**
     * Creates a hashcode of all the field descriptors and objects.
     *
     * @return Hashcode generated from all field descriptors and objects.
     */
    @Override
    public int hashCode() {

        if (payload == null) {
            return 0;
        }

        List<Object> objectsList = new ArrayList<>();
        payload.getAllFields().forEach((key, value) -> {
            // FieldDescriptors hashcodes are unique even for the same descriptors.
            // Hence, conversion toProto is required.
            objectsList.add(key.toProto());
            objectsList.add(value);
        });
        Object[] objects = objectsList.toArray();
        return Objects.hash(objects);
    }

    /**
     * Compares all the fields and objects of both dynamic messages.
     *
     * @param obj Other CorfuDynamicMessage to be compared.
     * @return True if equal, false otherwise.
     */
    @Override
    public boolean equals(Object obj) {

        if (!(obj instanceof CorfuDynamicMessage)) {
            return false;
        }

        CorfuDynamicMessage other = (CorfuDynamicMessage) obj;

        if (payload == null && other.payload == null) return true;
        if (payload == null || other.payload == null) return false;

        if (payload.getAllFields().size() != other.payload.getAllFields().size()) {
            return false;
        }

        // The getAllFields returns a {@link com.google.protobuf.SmallSortedMap}. This map ensures ordering.
        // This is created internally in protobuf from 2 parts an entry list  which is an ArrayList and an overflowMap
        // which is a TreeMap. Both of these guarantee ordering and hence ordering is guaranteed in the iteration.
        Iterator<Map.Entry<FieldDescriptor, Object>> thisMessage
                = new ArrayList<>(payload.getAllFields().entrySet()).iterator();
        Iterator<Map.Entry<FieldDescriptor, Object>> otherMessage
                = new ArrayList<>(other.payload.getAllFields().entrySet()).iterator();

        while (thisMessage.hasNext()) {
            if (!otherMessage.hasNext()) {
                return false;
            }
            Map.Entry<FieldDescriptor, Object> entry1 = thisMessage.next();
            Map.Entry<FieldDescriptor, Object> entry2 = otherMessage.next();

            if (!entry1.getKey().toProto().equals(entry2.getKey().toProto())
                    || !entry1.getValue().equals(entry2.getValue())) {
                return false;
            }
        }

        return true;
    }
}
