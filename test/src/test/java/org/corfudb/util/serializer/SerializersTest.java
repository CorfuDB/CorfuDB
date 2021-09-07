package org.corfudb.util.serializer;

import org.corfudb.CustomSerializer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test class verifies that multiple custom serializers can be registered.
 */
public class SerializersTest extends AbstractViewTest {

    public CorfuRuntime corfuRuntime;

    @Before
    public void setRuntime() throws Exception {
        corfuRuntime = getDefaultRuntime().connect();
    }

    @Test (expected = RuntimeException.class)
    public void registerSerializerWithInvalidTypeTest() {
        final byte SERIALIZER_TYPE0 = 10;
        ISerializer customSerializer = new CustomSerializer(SERIALIZER_TYPE0);
        corfuRuntime.getSerializers().registerSerializer(customSerializer);
    }

    @Test
    public void registerMultipleSerializersTest() {
        final byte type1 = (byte) 11;
        final byte type2 = (byte) 12;

        ISerializer customSerializer1 = new CustomSerializer(type1);
        ISerializer customSerializer2 = new CustomSerializer(type2);

        corfuRuntime.getSerializers().registerSerializer(customSerializer1);
        corfuRuntime.getSerializers().registerSerializer(customSerializer2);

        assertThat(corfuRuntime.getSerializers().getSerializer(type1)).isEqualTo(customSerializer1);
        assertThat(corfuRuntime.getSerializers().getSerializer(type2)).isEqualTo(customSerializer2);
    }
}
