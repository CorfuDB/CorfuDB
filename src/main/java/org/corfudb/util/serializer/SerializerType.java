package org.corfudb.util.serializer;

/**
 * Created by box on 8/31/16.
 */
public class SerializerType {
    public final Class<? extends ISerializer> entryType;
    private final String typeName;

    public SerializerType(Class<? extends ISerializer> entryType, String typeName) {
        this.entryType = entryType;
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }
}