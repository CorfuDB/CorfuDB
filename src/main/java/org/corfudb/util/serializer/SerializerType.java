package org.corfudb.util.serializer;

/**
 * SerializerType defines the supported serializers. The SerializerType.CUSTOM is a place holder for
 * a custom serializer that the user can register.
 */

public enum SerializerType {
    CORFU(0, CorfuSerializer.class),
    JAVA(1, JavaSerializer.class),
    JSON(2, JSONSerializer.class),
    PRIMITIVE(3, PrimitiveSerializer.class),
    CUSTOM(4, null);

    SerializerType(int type, Class<? extends ISerializer> serializer){
        this.type = type;
        this.serializer = serializer;
    }

    private int type;
    private Class<? extends ISerializer> serializer;

    void setSerializer(Class<? extends ISerializer> serializer){
        synchronized(this) {
            if (this.serializer != null) {
                throw new RuntimeException("Can't set serializer!");
            }
            this.serializer = serializer;
        }
    }

    public Class<? extends ISerializer> getSerializer(){
        return serializer;
    }

    public byte asByte() {
        return (byte) type;
    }
}