package org.corfudb.util.serializer;

import org.corfudb.CustomSerializer;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test class verifies that multiple custom serializers can be registered.
 */
public class SerializersTest {

    @Test (expected = RuntimeException.class)
    public void registerSerializerWithInvalidTypeTest() {
        final byte SERIALIZER_TYPE0 = 10;
        ISerializer customSerializer = new CustomSerializer(SERIALIZER_TYPE0);
        Serializers.registerSerializer(customSerializer);
    }

    @Test
    public void registerMultipleSerializersTest() {
        final byte type1 = (byte) 11;
        final byte type2 = (byte) 12;

        ISerializer customSerializer1 = new CustomSerializer(type1);
        ISerializer customSerializer2 = new CustomSerializer(type2);

        Serializers.registerSerializer(customSerializer1);
        Serializers.registerSerializer(customSerializer2);

        assertThat(Serializers.getSerializer(type1)).isEqualTo(customSerializer1);
        assertThat(Serializers.getSerializer(type2)).isEqualTo(customSerializer2);
    }
}
