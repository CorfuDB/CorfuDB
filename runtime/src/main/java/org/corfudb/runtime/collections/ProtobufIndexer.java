package org.corfudb.runtime.collections;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.OneofDescriptor;
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
import java.util.Map;
import java.util.Optional;

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

    // Map from secondary index name to index path (index fully qualified name)
    private final HashMap<String, String> secondaryIndexNameToPath = new HashMap<>();

    ProtobufIndexer(Message payloadSchema) {
        payloadSchema.getDescriptorForType().getFields().forEach(this::registerIndices);
    }

    private <T> Index.Spec<Message, CorfuRecord<Message, Message>, ?>
    getIndex(String indexPath, String indexName, FieldDescriptor fieldDescriptor) {
        return new Index.Spec<>(
                () -> indexPath,
                () -> indexName,
                (Index.Function<Message, CorfuRecord<Message, Message>, T>)
                        (key, val) -> ClassUtils.cast(val.getPayload().getField(fieldDescriptor)));
    }

    private <T> Index.Spec<Message, CorfuRecord<Message, Message>, ?>
    getNestedIndex(String indexPath, String indexName) {
        return new Index.Spec<>(
                () -> indexPath,
                () -> indexName,
                (Index.MultiValueFunction<Message, CorfuRecord<Message, Message>, T>)
                        (key, val) -> getIndexedValues(indexPath, val.getPayload()));
    }

    private <T> Iterable<T> getIndexedValues(String indexPath, Message messageToIndex) {
        // Separate nested fields, as full path is a 'dot' separated String, e.g., 'person.address.street'
        String[] nestedFields = indexPath.split("\\.");

        // Auxiliary variables used for the case of repeated fields
        List<Message> repeatedMessages = new ArrayList<>(); // Non-Primitive Types
        List<T> repeatedValues = new ArrayList<>();         // Primitive Types
        boolean upperLevelRepeatedField = false;

        Message subMessage = messageToIndex;
        FieldDescriptor nestedFieldDescriptor;
        boolean lastNestedField;
        Map<String, FieldDescriptor> oneOfFieldNameToDescriptor = new HashMap<>();

        // Navigate over each level of the secondary index (from root to the last indexed key), e.g., contact.person.phoneNumber
        for (int i = 0; i < nestedFields.length; i++) {
            nestedFieldDescriptor = subMessage.getDescriptorForType().findFieldByName(nestedFields[i]);
            lastNestedField = (i == (nestedFields.length - 1));

            if (nestedFieldDescriptor == null) {
                return Collections.EMPTY_LIST;
            }

            OneofDescriptor oneOfDescriptor = inspectDescriptorForOneOfFields(nestedFieldDescriptor, oneOfFieldNameToDescriptor);

            if (nestedFieldDescriptor.isRepeated()) {
                // Process repeated fields
                // In this case iterate over each repeated entry, accumulate actual 'values' if its a primitive,
                // accumulate 'messages' if its a non-primitive type (for further inspection)
                upperLevelRepeatedField = true;
                subMessage = processRepeatedField(subMessage, indexPath, nestedFields[i], lastNestedField,
                        repeatedMessages, repeatedValues);

                if (repeatedMessages.isEmpty() && repeatedValues.isEmpty()) {
                    // If no repeated messages or values where found while parsing the repeated field, no point in continuing
                    // parsing the message as the secondary index will not be found.
                    return processNonPrimitive(lastNestedField);
                }
            } else if (upperLevelRepeatedField) {
                // Case where an upper field was marked as a 'repeated', requires further iteration over
                // each repeated message
                subMessage = processFieldAfterARepeatedField(nestedFields[i], repeatedMessages, repeatedValues,
                        lastNestedField, oneOfFieldNameToDescriptor.containsKey(nestedFields[i]) ?
                                oneOfDescriptor : null);
            } else {
                // Case: Non-Primitive type (message)
                if (nestedFieldDescriptor.getType().equals(FieldDescriptor.Type.MESSAGE)) {
                    if (isValidField(oneOfDescriptor, subMessage, nestedFields[i])) {
                        // If next level descriptor is not set for the given message, no point in continuing parsing
                        // the message as the secondary index will not be found.
                        if (!subMessage.hasField(nestedFieldDescriptor)) {
                            return processNonPrimitive(lastNestedField);
                        }
                        subMessage = (Message) subMessage.getField(nestedFieldDescriptor);
                    }
                } else {
                    // Case: Primitive Type
                    if (!lastNestedField) {
                        // Only if its the last level, it can be a primitive. If there are remaining
                        // levels in the secondary key, it is malformed
                        throw new IllegalArgumentException(String.format("Malformed secondary key=%s, " +
                                "field <%s> of type PRIMITIVE.", indexPath, nestedFields[i]));
                    }

                    return processPrimitive(subMessage, oneOfDescriptor, nestedFieldDescriptor, nestedFields[i]);
                }
            }
        }

        if (!repeatedValues.isEmpty()) {
            return repeatedValues;
        } else {
            return Arrays.asList((T)ClassUtils.cast(subMessage));
        }
    }

    private <T> Iterable<T> processPrimitive(Message subMessage, OneofDescriptor oneOfDescriptor,
                                             FieldDescriptor fieldDescriptor, String nestedField) {
        if (isValidField(oneOfDescriptor, subMessage, nestedField)) {
            return Arrays.asList((T) ClassUtils.cast(subMessage.getField(fieldDescriptor)));
        }
        return Collections.emptyList();
    }

    private <T> Iterable<T> processNonPrimitive(boolean lastNestedField) {
        if (lastNestedField) {
            // If this is the last nested field, it means it is the secondary index which is unset
            // Index as null (unset)
            List<T> index = new ArrayList<>();
            index.add(null);
            return index;
        }
        return Collections.EMPTY_LIST;
    }

    /**
     * Inspect field descriptor to verify if it contains a 'oneOf' type field.
     */
    private OneofDescriptor inspectDescriptorForOneOfFields(FieldDescriptor nestedFieldDescriptor,
                                                            Map<String, FieldDescriptor> oneOfFieldNameToDescriptor) {
        OneofDescriptor oneOfDescriptor = nestedFieldDescriptor.getContainingOneof();
        if (oneOfDescriptor != null) {
            List<FieldDescriptor> descriptors = oneOfDescriptor.getFields();
            for (FieldDescriptor fieldDescriptor : descriptors) {
                oneOfFieldNameToDescriptor.put(fieldDescriptor.getName(), fieldDescriptor);
            }
        }
        return oneOfDescriptor;
    }

    /**
     * Verify if message is valid.
     *
     * In the case of a 'oneOf' type field, it is valid if the field set is exactly 'fieldName'
     */
    private boolean isValidField(OneofDescriptor oneOfDescriptor, Message message, String fieldName) {
        if (oneOfDescriptor != null) {
            return message.getOneofFieldDescriptor(oneOfDescriptor).getName().equals(fieldName);
        }
        return true;
    }

    private <T> Message processFieldAfterARepeatedField(String fieldName, List<Message> repeatedMessages,
                                                        List<T> repeatedValues, boolean lastNestedField,
                                                        OneofDescriptor oneOfDescriptor) {
        List<Message> nextLevelRepeatedMessages = new ArrayList<>();
        Message lastProcessedMessage = null;

        for (Message repeatedMessage : repeatedMessages) {
            FieldDescriptor descriptor = repeatedMessage.getDescriptorForType().findFieldByName(fieldName);

            if (descriptor.getType().equals(FieldDescriptor.Type.MESSAGE)) {
                // Non-Primitive type
                if (isValidField(oneOfDescriptor, repeatedMessage, fieldName)) {
                    if (!repeatedMessage.hasField(descriptor) && lastNestedField) {
                        // If the repeated message does not contain the next level descriptor of the secondary index
                        // and this is the last level (the secondary index field), index as 'null' as it is unset.
                        repeatedValues.add(null);
                        continue;
                    }

                    lastProcessedMessage = (Message) repeatedMessage.getField(descriptor);

                    if (lastNestedField) {
                        // Case: last nested field (values to return, either primitive or non-primitives)
                        repeatedValues.add(ClassUtils.cast(lastProcessedMessage));
                    } else {
                        // Case: intermediate nested field (accumulate messages in this level)
                        nextLevelRepeatedMessages.add(lastProcessedMessage);
                    }
                }
            } else if (isValidField(oneOfDescriptor, repeatedMessage, fieldName)) {
                // Primitive Type
                repeatedValues.add(ClassUtils.cast(repeatedMessage.getField(descriptor)));
            }
        }

        if (!nextLevelRepeatedMessages.isEmpty()) {
            repeatedMessages.clear();
            repeatedMessages.addAll(nextLevelRepeatedMessages);
        }

        return lastProcessedMessage;
    }

    /**
     * Process ProtoBuf repeated fields. A 'repeated' field is one that can be repeated any number of times
     * (including zero) in a well-formed message.
     *
     * @return
     */
    private <T> Message processRepeatedField(Message subMessage, String indexPath,
                                             String nestedIndexName, boolean lastNestedField,
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
                        throw new IllegalArgumentException(String.format("Malformed secondary key='%s', primitive field:: %s", indexPath, nestedIndexName));
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
            indices.put(indexName, getIndex(indexName, indexName, fieldDescriptor));
        } else if (fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getNestedSecondaryKeyCount() > 0) {

            for (int i=0; i<fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getNestedSecondaryKeyCount(); i++) {
                CorfuOptions.NestedSecondaryIndex secondaryIndex = fieldDescriptor.getOptions().getExtension(CorfuOptions.schema)
                        .getNestedSecondaryKey(i);

                // Remove whitespaces and/or invisible characters
                String indexPath = secondaryIndex.getIndexPath().replaceAll("\\s+", "");
                String indexName;

                // Index Name is optional, if not present, default to last attributes name
                if (secondaryIndex.hasIndexName()) {
                    indexName = secondaryIndex.getIndexName().replaceAll("\\s+", "");
                } else {
                    // Get all nested fields for secondary key (dot-separated), format example: person.fullName.lastName
                    String[] nestedFields = indexPath.split("\\.");
                    indexName = nestedFields[(nestedFields.length) - 1];
                }

                validateNestedSecondaryKey(indexPath, fieldDescriptor);

                // Place index name and a function on how the indexed value is computed
                indices.put(indexPath, getNestedIndex(indexPath, indexName));

                // For nested secondary indexes, an 'index_name' (alias) is supported (it can be user-defined or
                // defaults to the last attribute's name if not specified)
                // e.g., nested secondary key with path 'adult.children.child.age' defaults to name/alias 'age'
                // We keep a map of name/alias to  path, as secondary indexes can ba accessed on either one of them

                // Throw exception if any two secondary keys map to the same 'index_name'
                // This implies either the developer explicitly set the same 'index_name', or the end attributes of two
                // secondary keys have the same name. In this case, either the attribute's name needs to be changed or
                // a custom alias needs to be given to one of them.
                if (secondaryIndexNameToPath.containsKey(indexName)) {
                    throw new IllegalArgumentException("Invalid index name: not unique. Verify index name for key=<" + indexPath +
                            "> and key=<" + secondaryIndexNameToPath.get(indexName) + ">");
                }

                secondaryIndexNameToPath.put(indexName, indexPath);
            }
        }
    }

    /**
     * Validate nested secondary key string
     *
     */
    private void validateNestedSecondaryKey(String indexPath, FieldDescriptor fieldDescriptor) {
        if (!indexPath.isEmpty()) {
            // Get all nested fields for a single secondary key (dot-separated), format example: person.fullName.lastName
            String[] nestedFields = indexPath.split("\\.");

            // Confirm start of secondary key corresponds to the annotated field descriptor
            if (!nestedFields[0].equals(fieldDescriptor.toProto().getName())) {
                throw new IllegalArgumentException("Invalid nested secondary key=" + indexPath + ", invalid field :: " + nestedFields[0]);
            }

            FieldDescriptor nestedDescriptor = fieldDescriptor;

            // Skip root (index 0) field which corresponds to the initial fieldDescriptor
            for (int i = 1; i < nestedFields.length; i++) {
                nestedDescriptor = nestedDescriptor.getMessageType().findFieldByName(nestedFields[i]);

                if (nestedDescriptor == null) {
                    throw new IllegalArgumentException("Invalid nested secondary key=" + indexPath + ", invalid field :: " + nestedFields[i]);
                }
            }
        } else {
            throw new IllegalArgumentException("Empty nested secondary key path");
        }
    }

    @Override
    public Optional<Index.Spec<Message, CorfuRecord<Message, Message>, ?>> get(Index.Name name) {
        return Optional.ofNullable(name).map(indexName -> {
            String indexNamePath = indexName.get();
            if (!indices.containsKey(indexName)) {
                // Nested Secondary Key path is not specified, but instead its name/alias
                // Note that the index name can be user defined, otherwise, defaults to the name of the last attribute in
                // the secondary key path
                // e.g., 'level2' instead of path 'level0.level1.level2', or the user defined name
                indexNamePath = secondaryIndexNameToPath.get(indexName);
            }

            return indices.get(indexNamePath);
        });
    }

    @Override
    public Iterator<Index.Spec<Message, CorfuRecord<Message, Message>, ?>> iterator() {
        return indices.values().iterator();
    }
}
