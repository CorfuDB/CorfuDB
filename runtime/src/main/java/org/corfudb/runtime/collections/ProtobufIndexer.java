package org.corfudb.runtime.collections;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import org.corfudb.common.util.ClassUtils;
import org.corfudb.runtime.CorfuOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This layer implements the
 * ProtobufIndexer uses the special FieldOptions that the application can place on its
 * protobuf definitions (like secondary_key) and create secondary indexes callbacks over CorfuTable
 * based on that.
 *
 * Created by hisundar on 2019-08-12.
 */
public class ProtobufIndexer implements Index.Registry<Message, CorfuRecord<Message,
        Message>> {

    private final HashMap<String,
            Index.Spec<Message, CorfuRecord<Message, Message>, ?>>
            indices = new HashMap<>();

    ProtobufIndexer(Message payloadSchema) {
        payloadSchema.getDescriptorForType().getFields().forEach(this::registerIndices);
    }

    private <T> Index.Spec<Message, CorfuRecord<Message, Message>, ?>
    getIndex(String indexName, FieldDescriptor fieldDescriptor) {
        return new Index.Spec<>(
                () -> indexName,
                (Index.Function<Message, CorfuRecord<Message, Message>, T>)
                        (key, val) -> ClassUtils.cast(val.getPayload().getField(fieldDescriptor)));
    }

    private <T> Index.Spec<Message, CorfuRecord<Message, Message>, ?>
    getNestedIndex(String indexName) {
        return new Index.Spec<>(
                () -> indexName,
                (Index.MultiValueFunction<Message, CorfuRecord<Message, Message>, T>)
                        (key, val) -> getIndexedValues(indexName, val.getPayload()));
    }

    private <T> Iterable<T> getIndexedValues(String indexName, Message messageToIndex) {
        // Separate nested fields, as full path is a 'dot' separated String, e.g., 'person.address.street'
        String[] nestedFields = indexName.split("\\.");

        // Auxiliary variables used for the case of repeated fields
        List<Message> repeatedMessages = new ArrayList<>(); // Non-Primitive Types
        List<T> repeatedValues = new ArrayList<>();         // Primitive Types
        boolean upperLevelRepeatedField = false;

        Message subMessage = messageToIndex;
        FieldDescriptor nestedDescriptor;

        for (int i = 0; i < nestedFields.length; i++) {
            nestedDescriptor = subMessage.getDescriptorForType().findFieldByName(nestedFields[i]);

            if (nestedDescriptor == null) {
                throw new IllegalArgumentException(String.format("Secondary key %s, invalid field %s", indexName, nestedFields[i]));
            }

            boolean lastNestedField = (i == (nestedFields.length - 1));

            if (nestedDescriptor.isRepeated()) {
                upperLevelRepeatedField = true;
                subMessage = processRepeatedField(subMessage, indexName,
                        nestedFields[i], lastNestedField, repeatedMessages, repeatedValues);
            } else if (upperLevelRepeatedField) {
                // Case upper field was marked as a 'repeated' field
                // requires further iteration over each repeated message
                subMessage = processFieldAfterARepeatedField(nestedFields[i], repeatedMessages, repeatedValues, lastNestedField);
            } else {
                if (nestedDescriptor.getType().equals(FieldDescriptor.Type.MESSAGE)) {
                    subMessage = (Message) subMessage.getField(nestedDescriptor);
                } else {
                    // If its the last level it can be a primitive type, but if there are remaining
                    // levels in the secondary key, it is malformed
                    if (i != (nestedFields.length - 1)) {
                        throw new IllegalArgumentException(String.format("Malformed secondary key=%s, " +
                                "primitive field:: %s", indexName, nestedFields[i]));
                    }

                    return Arrays.asList((T) ClassUtils.cast(subMessage.getField(nestedDescriptor)));
                }
            }
        }

        if (!repeatedValues.isEmpty()) {
            return repeatedValues;
        } else {
            return Arrays.asList((T)ClassUtils.cast(subMessage));
        }
    }

    private <T> Message processFieldAfterARepeatedField(String fieldName, List<Message> repeatedMessages,
                                                        List<T> repeatedValues, boolean lastNestedField) {
        List<Message> nextLevelRepeatedMessages = new ArrayList<>();
        Message lastProcessedMessage = null;

        for (Message repeatedMessage : repeatedMessages) {
            FieldDescriptor descriptor = repeatedMessage.getDescriptorForType().findFieldByName(fieldName);
            if (descriptor.getType().equals(FieldDescriptor.Type.MESSAGE)) {
                // Replace upper level repeated message by next field, as we need to get all instances for this field
                lastProcessedMessage = (Message) repeatedMessage.getField(descriptor);
                if (lastNestedField) {
                    repeatedValues.add(ClassUtils.cast(lastProcessedMessage));
                } else {
                    nextLevelRepeatedMessages.add(lastProcessedMessage);
                }
            } else {
                // Primitive Type, directly add the indexed value to be returned
                repeatedValues.add(ClassUtils.cast(repeatedMessage.getField(descriptor)));
            }
        }

        if (!nextLevelRepeatedMessages.isEmpty()) {
                repeatedMessages.clear();
                repeatedMessages.addAll(nextLevelRepeatedMessages);
        }

        return lastProcessedMessage;
    }

    private <T> Message processRepeatedField(Message subMessage,
                                      String secondaryKey, String nestedIndexName, boolean lastNestedField,
                                      List<Message> repeatedMessages, List<T> repeatedValues) {
        Message repeatedMessage = subMessage;
        List<Message> messages = new ArrayList<>();

        if(!repeatedMessages.isEmpty()) {
            // Case of chained repeated types (of type MESSAGE), iterate over each repeated message
            messages.addAll(repeatedMessages);
            // Clear repeated messages, as it should contain the latest level of repeated messages
            repeatedMessages.clear();
        } else {
            messages.add(subMessage);
        }

        for (Message msg : messages) {
            FieldDescriptor descriptor = msg.getDescriptorForType().findFieldByName(nestedIndexName);
            int repeatedFieldCount = msg.getRepeatedFieldCount(descriptor);
            for (int index = 0; index < repeatedFieldCount; index++) {
                if (descriptor.getType().equals(FieldDescriptor.Type.MESSAGE)) {
                    // Special case, repeated MESSAGE field, get all elements in the repeated field
                    // Over which indexed values will be extracted
                    repeatedMessage = (Message) msg.getRepeatedField(descriptor, index);

                    if (lastNestedField) {
                        repeatedValues.add(ClassUtils.cast(repeatedMessage));
                    } else {
                        repeatedMessages.add(repeatedMessage);
                    }
                } else {
                    // A Primitive type is valid, only if it is the last level in the chain,
                    // otherwise, this is a malformed nested secondary key
                    if (!lastNestedField) {
                        throw new IllegalArgumentException(String.format("Malformed secondary key='%s', primitive field:: %s", secondaryKey, nestedIndexName));
                    }

                    repeatedValues.add(ClassUtils.cast(msg.getRepeatedField(descriptor, index)));
                }
            }
        }

        return repeatedMessage;
    }

    /**
     * Register a Secondary Index
     *
     * @param fieldDescriptor describes the root field of a proto MESSAGE type
     */
    private void registerIndices(final Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getSecondaryKey()) {
            final String indexName = fieldDescriptor.getName();
            if (fieldDescriptor.getType() == FieldDescriptor.Type.GROUP) {
                throw new IllegalArgumentException("group is a deprecated, unsupported type");
            }
            indices.put(indexName, getIndex(indexName, fieldDescriptor));
        } else if (fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).hasNestedSecondaryKeys()) {
            if (fieldDescriptor.getType() == FieldDescriptor.Type.GROUP) {
                throw new IllegalArgumentException("group is a deprecated, unsupported type");
            }
            // Parse and validate nested secondary key(s)
            String nestedSecondaryKeysString = fieldDescriptor.getOptions().getExtension(CorfuOptions.schema)
                    .getNestedSecondaryKeys();
            List<String> secondaryKeys = parseAndValidateNestedSecondaryKeys(nestedSecondaryKeysString, fieldDescriptor);
            for (String indexName : secondaryKeys) {
                // Place index name and a function on how the indexed value is computed
                indices.put(indexName, getNestedIndex(indexName));
            }
        }
    }

    /**
     * Parse and validate nested secondary keys string.
     *
     * Note that it supports multiple nested secondary keys which are 'comma-separated', e.g.,
     * person.fullName.middleName, person.age, person.phone.mobile
     *
     */
    private List<String> parseAndValidateNestedSecondaryKeys(String nestedSecondaryKeysString, FieldDescriptor fieldDescriptor) {
        if (!nestedSecondaryKeysString.isEmpty()) {
            // Get all nested secondary keys (comma-separated values)
            // and remove any whitespaces and invisible characters
            List<String> secondaryKeys = Arrays.stream(nestedSecondaryKeysString.split(","))
                    .map(secondaryKey -> secondaryKey.replaceAll("\\s+", ""))
                    .collect(Collectors.toList());
            return validateSecondaryKeys(secondaryKeys, fieldDescriptor);
        }

        return Collections.emptyList();
    }

    /**
     * Validate the secondary key full specified path exists
     */
    private List<String> validateSecondaryKeys(List<String> secondaryKeys, FieldDescriptor fieldDescriptor) {

        for(String secondaryKey : secondaryKeys) {
            // Get all nested fields for a single secondary key (dot-separated), format example: person.fullName.lastName
            String[] nestedFields = secondaryKey.split("\\.");

            // Confirm start of secondary key corresponds to the annotated field descriptor
            if (!nestedFields[0].equals(fieldDescriptor.toProto().getName())) {
                throw new IllegalArgumentException("Invalid nested secondary key=" + secondaryKey + ", invalid field :: " + nestedFields[0]);
            }

            FieldDescriptor nestedDescriptor = fieldDescriptor;

            // Skip root (index 0) field which corresponds to the initial fieldDescriptor
            for (int i = 1; i < nestedFields.length; i++) {
                nestedDescriptor = nestedDescriptor.getMessageType().findFieldByName(nestedFields[i]);

                if (nestedDescriptor == null) {
                    throw new IllegalArgumentException("Invalid nested secondary key=" + secondaryKey + ", invalid field :: " + nestedFields[i]);
                }
            }
        }

        return secondaryKeys;
    }

    @Override
    public Optional<
            Index.Spec<Message, CorfuRecord<Message, Message>, ?>
            > get(Index.Name name) {
        return Optional.ofNullable(name).map(indexName -> indices.get(indexName));
    }

    @Override
    public Iterator<Index.Spec<Message, CorfuRecord<Message, Message>, ?>> iterator() {
        return indices.values().iterator();
    }
}
