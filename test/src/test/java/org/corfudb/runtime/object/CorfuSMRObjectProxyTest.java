package org.corfudb.runtime.object;

import com.google.common.reflect.TypeToken;
import org.corfudb.CustomSerializer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/21/16.
 */
public class CorfuSMRObjectProxyTest extends AbstractObjectTest {
    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteToSingle()
            throws Exception {
        getDefaultRuntime();

        ICorfuTable<String, String> testMap = (ICorfuTable<String, String>)
                instantiateCorfuObject(new TypeToken<PersistentCorfuTable<String,String>>() {}, "test");

        testMap.clear();
        assertThat(testMap.get("a")).isNull();
        testMap.insert("a", "a");
        assertThat(testMap.get("a")).isEqualTo("a");
        testMap.insert("a", "b");
        assertThat(testMap.get("a")).isEqualTo("b");

        ICorfuTable<String, String> testMap2 = (ICorfuTable<String, String>)
                instantiateCorfuObject(new TypeToken<PersistentCorfuTable<String,String>>() {}, "test");

        assertThat(testMap2.get("a"))
                .isEqualTo("b");
    }

    @Test
    public void canUseCustomSerializer() throws Exception {
        //Register a custom serializer and use it with an SMR object
        ISerializer customSerializer = new CustomSerializer((byte) (Serializers.SYSTEM_SERIALIZERS_COUNT + 1));
        getDefaultRuntime().getSerializers().registerSerializer(customSerializer);
        CorfuRuntime r = getDefaultRuntime();

        ICorfuTable<String, String> test = r.getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("test")
                .setSerializer(customSerializer)
                .open();

        test.insert("a", "b");
        test.get("a");
        assertThat(test.get("a")).isEqualTo("b");
    }

    /**
     * Once a SMR map is created with a serializer, we should
     * not be able to change the serializer by re-opening the map with
     * a different serializer.
     *
     * @throws Exception
     */
    @Test
    public void doesNotResetSerializerIfMapAlreadyExists() throws Exception {
        ISerializer customSerializer = new CustomSerializer((byte) (Serializers.SYSTEM_SERIALIZERS_COUNT + 1));
        getDefaultRuntime().getSerializers().registerSerializer(customSerializer);
        CorfuRuntime r = getDefaultRuntime();

        ICorfuTable<String, String> test = r.getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("test")
                .setSerializer(customSerializer)
                .open();

        ICorfuTable<String, String> test2 = r.getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("test")
                .setSerializer(Serializers.getDefaultSerializer())
                .open();

        ObjectsView.ObjectID mapId = new ObjectsView.
                ObjectID(CorfuRuntime.getStreamID("test"), PersistentCorfuTable.class);

        ICorfuSMRProxyMetadata cp = ((ICorfuSMR) r.getObjectsView().
                getObjectCache().
                get(mapId)).
                getCorfuSMRProxy();

        assertThat(cp.getSerializer()).isEqualTo(customSerializer);
    }

}
