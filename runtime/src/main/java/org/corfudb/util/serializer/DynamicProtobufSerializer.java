package org.corfudb.util.serializer;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileDescriptor;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileName;
import org.corfudb.runtime.CorfuStoreMetadata.Record;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.util.serializer.ProtobufSerializer.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.REGISTRY_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;
import static org.corfudb.runtime.view.TableRegistry.getTypeUrl;


/**
 * The Protobuf serializer is the main component that allows CorfuStore to use Protobufs to
 * convert a language specific (Java here) object into an identifiable byte buffer that
 * can be then converted back to a language specific object.
 * <p>
 * This serializer first opens the table registry. From this table registry, it reads all the file descriptors and
 * creates mappings for the following:
 * - FileDescriptorProto name to FileDescriptorProto
 * - Message name in the FileDescriptorProto to name of the FileDescriptorProto.
 * This is to fetch all the depending FileDescriptorProtos required to desrialize a particular Message.
 * <p>
 * On deserialization, we extract the typeUrl from the Any field of the message. From the typeUrl, we get the
 * message name. Using this message name we build the FileDescriptor recursively to deserialize the byteString.
 * On serialization, we create the Any message ot be persisted. The typeUrl is provided from {@link CorfuDynamicKey}
 * or {@link CorfuDynamicRecord}.
 */
@Slf4j
public class DynamicProtobufSerializer implements ISerializer {

    /**
     * Type code of the serializer. In this case the code needs to override the code of {@link ProtobufSerializer}.
     */
    @Getter
    private final byte type;

    /**
     * This map is generated on initialization.
     * Maps the fileDescriptorProto name to the FileDescriptorProto.
     */
    private final ConcurrentMap<String, FileDescriptorProto> fdProtoMap;

    /**
     * This map is generated on initialization.
     * Maps the Message name to the name of the FileDescriptorProto containing it.
     */
    protected final ConcurrentMap<String, String> messagesFdProtoNameMap;

    /**
     * This is used as a file descriptor cache. Used for optimization.
     */
    private final ConcurrentMap<String, FileDescriptor> fileDescriptorMap = new ConcurrentHashMap<>();

    @Getter
    private ConcurrentMap<TableName, CorfuRecord<TableDescriptors,
        TableMetadata>> cachedRegistryTable = new ConcurrentHashMap<>();

    @Getter
    private ConcurrentMap<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor,
        TableMetadata>> cachedProtobufDescriptorTable =
        new ConcurrentHashMap<>();

    public DynamicProtobufSerializer(CorfuRuntime corfuRuntime) {
        this.type = ProtobufSerializer.PROTOBUF_SERIALIZER_CODE;
        this.fdProtoMap = new ConcurrentHashMap<>();
        this.messagesFdProtoNameMap = new ConcurrentHashMap<>();

        // Create or get a protobuf serializer to read the table registry.
        ISerializer protobufSerializer;
        try {
            protobufSerializer = corfuRuntime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);
        } catch (SerializerException se) {
            // This means the protobuf serializer had not been registered yet.
            log.info("Protobuf Serializer not found. Create and register a new one.");
            protobufSerializer = createProtobufSerializer();
            corfuRuntime.getSerializers().registerSerializer(protobufSerializer);
        }

        // Open the Registry Table and cache its contents
        PersistentCorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>> registryTable =
            corfuRuntime.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<TableName,
                    CorfuRecord<TableDescriptors, TableMetadata>>>() {
                })
                .setStreamName(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, REGISTRY_TABLE_NAME))
                .setSerializer(protobufSerializer)
                .addOpenOption(ObjectOpenOption.NO_CACHE)
                .open();

        Iterator<Map.Entry<TableName, CorfuRecord<TableDescriptors, TableMetadata>>> registryIt = registryTable.entryStream().iterator();
        while (registryIt.hasNext()) {
            Map.Entry<TableName, CorfuRecord<TableDescriptors, TableMetadata>> entry = registryIt.next();
            cachedRegistryTable.put(entry.getKey(), entry.getValue());
        }

        // Open the Protobuf Descriptor Table.
        PersistentCorfuTable<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor, TableMetadata>> descriptorTable = corfuRuntime.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor, TableMetadata>>>() {
                })
                .setStreamName(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                        PROTOBUF_DESCRIPTOR_TABLE_NAME))
                .setSerializer(protobufSerializer)
                .addOpenOption(ObjectOpenOption.NO_CACHE)
                .open();

        // Cache the FileDescriptorProtos from the protobuf descriptor table.
        Iterator<Map.Entry<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor, TableMetadata>>> descriptorIt = descriptorTable.entryStream().iterator();
        while (descriptorIt.hasNext()) {
            Map.Entry<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor, TableMetadata>> entry = descriptorIt.next();

            String protoFileName = entry.getValue().getPayload().getFileDescriptor().getName();
            // Since corfu_options is something within repo, the path gets truncated on insert.
            // However dynamicProtobufSerializer fails since the full path is needed.
            if (protoFileName.equals("corfu_options.proto")) {
                fdProtoMap.putIfAbsent(protoFileName, entry.getValue().getPayload().getFileDescriptor());
                // Until the truncating issue can be addressed, manually add both paths.
                protoFileName = "corfudb/runtime/corfu_options.proto";
            }
            fdProtoMap.putIfAbsent(protoFileName, entry.getValue().getPayload().getFileDescriptor());
            identifyMessageTypesinFileDescriptorProto(entry.getValue().getPayload().getFileDescriptor());

            // cache the entry
            cachedProtobufDescriptorTable.put(entry.getKey(), entry.getValue());
        }

        // Remove the protobuf serializer
        corfuRuntime.getSerializers().clearCustomSerializers();
    }

    /**
     *
     * @param cachedRegistryTable - pre-constructed cache of the entire RegistryTable
     * @param cachedProtobufDescriptorTable pre-constructed cache of the ProtobufDescriptorTable
     */
    public DynamicProtobufSerializer(
            ConcurrentMap<TableName,
                    CorfuRecord<TableDescriptors, TableMetadata>> cachedRegistryTable,
            ConcurrentMap<ProtobufFileName,
                    CorfuRecord<ProtobufFileDescriptor, TableMetadata>> cachedProtobufDescriptorTable) {
        this.type = ProtobufSerializer.PROTOBUF_SERIALIZER_CODE;
        this.cachedRegistryTable = cachedRegistryTable;
        this.cachedProtobufDescriptorTable = cachedProtobufDescriptorTable;
        this.fdProtoMap = new ConcurrentHashMap<>();
        this.messagesFdProtoNameMap = new ConcurrentHashMap<>();
        cachedProtobufDescriptorTable.forEach((fdName, fileDescriptorProto) -> {
            populateFileDescriptorProtosInMessage(fileDescriptorProto, fdProtoMap);
            identifyMessageTypesinFileDescriptorProto(fileDescriptorProto.getPayload().getFileDescriptor());
        });
    }

    /**
     * Create a protobuf serializer.
     *
     * @return Protobuf Serializer.
     */
    public static ISerializer createProtobufSerializer() {
        ConcurrentMap<String, Class<? extends Message>> classMap = new ConcurrentHashMap<>();

        // Register the schemas of TableName, TableDescriptors, TableMetadata, ProtobufFilename/Descriptor
        // to be able to understand registry table.
        classMap.put(getTypeUrl(TableName.getDescriptor()), TableName.class);
        classMap.put(getTypeUrl(TableDescriptors.getDescriptor()),
                TableDescriptors.class);
        classMap.put(getTypeUrl(TableMetadata.getDescriptor()),
                TableMetadata.class);
        classMap.put(getTypeUrl(ProtobufFileName.getDescriptor()),
                ProtobufFileName.class);
        classMap.put(getTypeUrl(ProtobufFileDescriptor.getDescriptor()),
                ProtobufFileDescriptor.class);
        return new ProtobufSerializer(classMap);
    }

    /**
     * @param fileDescriptorProto the input protobuf file descriptor
     * @param fdProtoMap the destination map to which we extract the types into
     */
    public static void populateFileDescriptorProtosInMessage(
            CorfuRecord<ProtobufFileDescriptor, TableMetadata> fileDescriptorProto,
            ConcurrentMap<String, FileDescriptorProto> fdProtoMap) {
        String protoFileName = fileDescriptorProto.getPayload().getFileDescriptor().getName();
        // Since corfu_options is something within repo, the path gets truncated on insert.
        // However dynamicProtobufSerializer fails since the full path is needed.
        if (protoFileName.equals("corfu_options.proto")) {
            fdProtoMap.putIfAbsent(protoFileName, fileDescriptorProto.getPayload().getFileDescriptor());
            // Until the truncating issue can be addressed, manually add both paths.
            protoFileName = "corfudb/runtime/corfu_options.proto";
        }
        fdProtoMap.putIfAbsent(protoFileName, fileDescriptorProto.getPayload().getFileDescriptor());
    }

    /**
     * Identify message types in file descriptor proto and create a mapping for the message name to the
     * file descriptor proto name.
     *
     * @param fileDescriptorProto FileDescriptorProto to inspect.
     */
    private void identifyMessageTypesinFileDescriptorProto(FileDescriptorProto fileDescriptorProto) {
        for (DescriptorProtos.DescriptorProto descriptorProto : fileDescriptorProto.getMessageTypeList()) {
            String messageName;
            fileDescriptorProto.getPackage();
            if (fileDescriptorProto.getPackage().equals("")) {
                messageName = descriptorProto.getName();
            } else {
                messageName = fileDescriptorProto.getPackage() + "." + descriptorProto.getName();
            }
            messagesFdProtoNameMap.putIfAbsent(messageName, fileDescriptorProto.getName());
        }
    }

    /**
     * Builds the FileDescriptor for a given FileDescriptorProto name recursively by building all the
     * relevant dependencies.
     * This uses the fileDescriptorMap to cache the already built FileDescriptors.
     *
     * @param name FileDescriptorProto name.
     * @return FileDescriptor.
     * @throws DescriptorValidationException If FileDescriptor construction fails.
     */
    protected FileDescriptor getDescriptor(String name) throws DescriptorValidationException {

        if (fileDescriptorMap.containsKey(name)) {
            return fileDescriptorMap.get(name);
        }

        if (!fdProtoMap.containsKey(name)) {
            log.error("DynamicProtobufSerializer failed to deserialize unknown file {}",
                    name);
            log.error(this.toString());
            throw new SerializerException(
                    "DynamicProtobufSerializer file " + name + " was never seen in registry");
        }
        List<FileDescriptor> fileDescriptorList = new ArrayList<>();
        for (String s : fdProtoMap.get(name).getDependencyList()) {
            FileDescriptor descriptor = getDescriptor(s);
            fileDescriptorList.add(descriptor);
        }
        FileDescriptor[] fileDescriptors = fileDescriptorList.toArray(new FileDescriptor[0]);
        FileDescriptor fileDescriptor = FileDescriptor.buildFrom(fdProtoMap.get(name), fileDescriptors);
        fileDescriptorMap.putIfAbsent(name, fileDescriptor);
        return fileDescriptor;
    }

    /**
     * Extracts the message name from the type url.
     * Example. typeUrl: type.googleapis.com/org.corfudb.runtime.TableName
     * This returns TableName.
     *
     * @param message Any message.
     * @return Message name.
     */
    protected String getMessageName(Any message) {
        String typeUrl = message.getTypeUrl();
        String messageName = typeUrl.substring(typeUrl.lastIndexOf('.') + 1);
        if (messageName.contains("/")) {
            // In case the message lacks package name
            // E.g. typeUrl: type.googleapis.com/TableName
            messageName = messageName.substring(messageName.lastIndexOf("/") + 1);
        }
        return messageName;
    }

    /**
     * Gets the full name with the package name from the type Url.
     * Example. typeUrl: type.googleapis.com/org.corfudb.runtime.TableName
     * This returns org.corfudb.runtime.TableName.
     *
     * @param message Any message.
     * @return Full name of the message.
     */
    protected String getFullMessageName(Any message) {
        String typeUrl = message.getTypeUrl();
        return typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
    }

    public DynamicMessage createDynamicMessageFromJson(Any anyMsg,
        String jsonString) {
        FileDescriptor fileDescriptor;
        try {
            String fullMessageName = getFullMessageName(anyMsg);
            fileDescriptor = getDescriptor(
                messagesFdProtoNameMap.get(fullMessageName));
        } catch (Descriptors.DescriptorValidationException e) {
            log.warn("DescriptorValidationException thrown", e);
            return null;
        }
        Descriptors.Descriptor descriptor =
            fileDescriptor.findMessageTypeByName(getMessageName(anyMsg));
        DynamicMessage.Builder builder;
        try {
            builder = DynamicMessage.parseFrom(descriptor,
                anyMsg.getValue()).toBuilder();
        } catch (InvalidProtocolBufferException e) {
            log.warn("Unable to Parse Key {}", anyMsg.getValue());
            return null;
        }

        try {
            JsonFormat.parser().merge(jsonString, builder);
        } catch(InvalidProtocolBufferException e) {
            log.warn("Unable to Parse String {}", jsonString);
            return null;
        }
        return builder.build();
    }

    /**
     * Deserialize an object from a given byte buffer.
     *
     * @param b The bytebuf to deserialize.
     * @return The deserialized object.
     */
    @Override
    public Object deserialize(ByteBuf b, CorfuRuntime rt) {

        try (ByteBufInputStream bbis = new ByteBufInputStream(b)) {
            MessageType type = MessageType.valueOf(bbis.readInt());
            int size = bbis.readInt();
            byte[] data = new byte[size];
            bbis.readFully(data);
            Record record = Record.parseFrom(data);
            Any payload = record.getPayload();

            String fullMessageName = getFullMessageName(payload);
            if (!messagesFdProtoNameMap.containsKey(fullMessageName)) {
                log.error("messagesFdProtoNameMap doesn't contain the message type {} of payload {}." +
                                "Please check if the related table is properly opened with correct schema.",
                        fullMessageName, payload);
                log.error("messagesFdProtoNameMap keySet is {}", messagesFdProtoNameMap.keySet());
            }

            FileDescriptor valueFileDescriptor
                    = getDescriptor(messagesFdProtoNameMap.get(fullMessageName));
            Descriptor valueDescriptor = valueFileDescriptor.findMessageTypeByName(getMessageName(payload));
            DynamicMessage value = DynamicMessage.parseFrom(valueDescriptor, payload.getValue());

            if (type.equals(MessageType.KEY)) {
                return new CorfuDynamicKey(payload.getTypeUrl(), value);
            }

            String metadataTypeUrl = null;
            DynamicMessage metadata = null;
            if (record.hasMetadata()) {
                Any anyMetadata = record.getMetadata();
                metadataTypeUrl = anyMetadata.getTypeUrl();

                String fullMetadataMessageName = getFullMessageName(anyMetadata);
                if (!messagesFdProtoNameMap.containsKey(fullMetadataMessageName)) {
                    log.error("messagesFdProtoNameMap doesn't contain the message type {} of metadata {}." +
                                    "Please check if the related table is properly opened with correct schema.",
                            fullMetadataMessageName, anyMetadata);
                    log.error("messagesFdProtoNameMap keySet is {}", messagesFdProtoNameMap.keySet());
                }

                FileDescriptor metaFileDescriptor
                        = getDescriptor(messagesFdProtoNameMap.get(fullMetadataMessageName));
                Descriptor metaDescriptor = metaFileDescriptor.findMessageTypeByName(getMessageName(anyMetadata));
                metadata = DynamicMessage.parseFrom(metaDescriptor, anyMetadata.getValue());
            }
            return new CorfuDynamicRecord(payload.getTypeUrl(), value, metadataTypeUrl, metadata);

        } catch (IOException | DescriptorValidationException ie) {
            log.error("Exception during deserialization!", ie);
            throw new SerializerException(ie);
        }
    }

    @Override
    public <T> T deserializeTyped(ByteBuf b, CorfuRuntime rt) {
        return (T) deserialize(b, rt);
    }

    /**
     * Serialize an object into a given byte buffer.
     *
     * @param o The object to serialize.
     * @param b The bytebuf to serialize it into.
     */
    @Override
    public void serialize(Object o, ByteBuf b) {

        Record record;
        MessageType messageType;

        if (o instanceof CorfuDynamicRecord) {
            CorfuDynamicRecord corfuRecord = (CorfuDynamicRecord) o;
            Any message = Any.newBuilder()
                    .setTypeUrl(corfuRecord.getPayloadTypeUrl())
                    .setValue(corfuRecord.getPayload().toByteString())
                    .build();
            Record.Builder recordBuilder = Record.newBuilder()
                    .setPayload(message);
            if (corfuRecord.getMetadata() != null) {
                Any metadata = Any.newBuilder()
                        .setTypeUrl(corfuRecord.getMetadataTypeUrl())
                        .setValue(corfuRecord.getMetadata().toByteString())
                        .build();
                recordBuilder.setMetadata(metadata);
            }
            record = recordBuilder.build();
            messageType = MessageType.VALUE;
        } else {
            CorfuDynamicKey corfuKey = (CorfuDynamicKey) o;
            Any message = Any.newBuilder()
                    .setTypeUrl(corfuKey.getKeyTypeUrl())
                    .setValue(corfuKey.getKey().toByteString())
                    .build();
            record = Record.newBuilder()
                    .setPayload(message)
                    .build();
            messageType = MessageType.KEY;
        }
        byte[] data = record.toByteArray();

        try (ByteBufOutputStream bbos = new ByteBufOutputStream(b)) {
            bbos.writeInt(messageType.val);
            bbos.writeInt(data.length);
            bbos.write(data);
        } catch (IOException ie) {
            log.error("Exception during serialization!", ie);
            throw new SerializerException(ie);
        }
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("\n---------messageFdProtoNameMap Messages:---------\n");
        for (Map.Entry<String, String> mName: messagesFdProtoNameMap.entrySet()) {
            stringBuilder
                    .append("Message: ")
                    .append(mName.getKey())
                    .append(" in file ")
                    .append(mName.getValue())
                    .append("\n");
        }

        stringBuilder.append("\n-------fdProtoMap FileDescriptors:--------------\n");
        for (Map.Entry<String, FileDescriptorProto> fdProto: fdProtoMap.entrySet()) {
            stringBuilder.append("File: ").append(fdProto.getKey());
            stringBuilder.append(" ===> (FileDescriptorProto follows...)\n");
            stringBuilder.append(fdProto.getValue());
        }

        stringBuilder.append("\n---------fileDescriptorCache FileDescriptors----------\n");
        for (Map.Entry<String, FileDescriptor> fds: fileDescriptorMap.entrySet()) {
            stringBuilder.append("File: ").append(fds.getKey()).append(" ==> ").append(fds.getValue()).append("\n");
        }
        return stringBuilder.toString();
    }
}
