package org.corfudb.util.serializer;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.Record;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.util.serializer.ProtobufSerializer.MessageType;

import java.io.IOException;
import java.util.ArrayList;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
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
    private final ConcurrentMap<String, FileDescriptorProto> fdProtoMap = new ConcurrentHashMap<>();

    /**
     * This map is generated on initialization.
     * Maps the Message name to the name of the FileDescriptorProto containing it.
     */
    private final ConcurrentMap<String, String> messagesFdProtoNameMap = new ConcurrentHashMap<>();

    /**
     * This is used as a file descriptor cache. Used for optimization.
     */
    private final ConcurrentMap<String, FileDescriptor> fileDescriptorMap = new ConcurrentHashMap<>();

    public DynamicProtobufSerializer(CorfuRuntime corfuRuntime) {
        this.type = ProtobufSerializer.PROTOBUF_SERIALIZER_CODE;

        // Create and register a protobuf serializer to read the table registry.
        ISerializer protobufSerializer = createProtobufSerializer();
        Serializers.registerSerializer(protobufSerializer);

        // Open the Registry Table.
        CorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>> corfuTable = corfuRuntime.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>>>() {
                })
                .setStreamName(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, REGISTRY_TABLE_NAME))
                .setSerializer(protobufSerializer)
                .addOpenOption(ObjectOpenOption.NO_CACHE)
                .open();

        // Cache the FileDescriptorProtos from the registry table.
        corfuTable.forEach((tableName, value) -> {
            TableDescriptors tableDescriptors = value.getPayload();
            tableDescriptors.getFileDescriptorsMap().forEach((fdName, fileDescriptorProto) -> {
                String protoFileName = fileDescriptorProto.getName();
                // Looks like protobuf file descriptors are randomly truncating the path.
                // This causes dynamicProtobufSerializer to fail since the full path is needed.
                if (protoFileName.equals("corfu_options.proto")) {
                    fdProtoMap.putIfAbsent(protoFileName, fileDescriptorProto);
                    // Until the truncating issue can be addressed, manually add both paths.
                    protoFileName = "corfudb/runtime/corfu_options.proto";
                }
                fdProtoMap.putIfAbsent(protoFileName, fileDescriptorProto);
                identifyMessageTypesinFileDescriptorProto(fileDescriptorProto);
            });
        });
        Serializers.registerSerializer(this);
    }

    /**
     * Create a protobuf serializer.
     *
     * @return Protobuf Serializer.
     */
    private ISerializer createProtobufSerializer() {
        Map<String, Class<? extends Message>> classMap = new ConcurrentHashMap<>();

        // Register the schemas of TableName, TableDescriptors & TableMetadata
        // to be able to understand registry table.
        classMap.put(getTypeUrl(TableName.getDescriptor()), TableName.class);
        classMap.put(getTypeUrl(TableDescriptors.getDescriptor()), TableDescriptors.class);
        classMap.put(getTypeUrl(TableMetadata.getDescriptor()), TableMetadata.class);
        return new ProtobufSerializer(classMap);
    }

    /**
     * Identify message types in file descriptor proto and create a mapping for the message name to the
     * file descriptor proto name.
     *
     * @param fileDescriptorProto FileDescriptorProto to inspect.
     */
    private void identifyMessageTypesinFileDescriptorProto(FileDescriptorProto fileDescriptorProto) {
        for (DescriptorProtos.DescriptorProto descriptorProto : fileDescriptorProto.getMessageTypeList()) {
            String messageName = fileDescriptorProto.getPackage() + "." + descriptorProto.getName();
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
    private FileDescriptor getDescriptor(String name) throws DescriptorValidationException {

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
        FileDescriptor[] fileDescriptors = fileDescriptorList.toArray(new FileDescriptor[fileDescriptorList.size()]);
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
    private String getMessageName(Any message) {
        String typeUrl = message.getTypeUrl();
        return typeUrl.substring(typeUrl.lastIndexOf('.') + 1);
    }

    /**
     * Gets the full name with the package name from the type Url.
     * Example. typeUrl: type.googleapis.com/org.corfudb.runtime.TableName
     * This returns org.corfudb.runtime.TableName.
     *
     * @param message Any message.
     * @return Full name of the message.
     */
    private String getFullMessageName(Any message) {
        String typeUrl = message.getTypeUrl();
        return typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
    }

    /**
     * Deserialize an object from a given byte buffer.
     *
     * @param b The bytebuf to deserialize.
     * @return The deserialized object.
     */
    @Override
    public Object deserialize(ByteBuf b) {

        try (ByteBufInputStream bbis = new ByteBufInputStream(b)) {
            MessageType type = MessageType.valueOf(bbis.readInt());
            int size = bbis.readInt();
            byte[] data = new byte[size];
            bbis.readFully(data);
            Record record = Record.parseFrom(data);
            Any payload = record.getPayload();

            FileDescriptor valueFileDescriptor
                    = getDescriptor(messagesFdProtoNameMap.get(getFullMessageName(payload)));
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

                FileDescriptor metaFileDescriptor
                        = getDescriptor(messagesFdProtoNameMap.get(getFullMessageName(anyMetadata)));
                Descriptor metaDescriptor = metaFileDescriptor.findMessageTypeByName(getMessageName(anyMetadata));
                metadata = DynamicMessage.parseFrom(metaDescriptor, anyMetadata.getValue());
            }
            return new CorfuDynamicRecord(payload.getTypeUrl(), value, metadataTypeUrl, metadata);

        } catch (IOException | DescriptorValidationException ie) {
            log.error("Exception during deserialization!", ie);
            throw new SerializerException(ie);
        }
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
        for (Map.Entry<String, String> mname: messagesFdProtoNameMap.entrySet()) {
            stringBuilder.append("Message: "+ mname.getKey()+ " in file "+ mname.getValue()+"\n");
        }

        stringBuilder.append("\n-------fdProtoMap FileDescriptors:--------------\n");
        for (Map.Entry<String, FileDescriptorProto> fdProto: fdProtoMap.entrySet()) {
            stringBuilder.append("File: " + fdProto.getKey());
            stringBuilder.append(" ===> (FileDescriptorProto follows...)\n");
            stringBuilder.append(fdProto.getValue());
        }

        stringBuilder.append("\n---------fileDescriptorCache FileDescriptors----------\n");
        for (Map.Entry<String, FileDescriptor> fds: fileDescriptorMap.entrySet()) {
            stringBuilder.append("File: "+ fds.getKey() + " ==> " + fds.getValue() + "\n");
        }
        return stringBuilder.toString();
    }
}

