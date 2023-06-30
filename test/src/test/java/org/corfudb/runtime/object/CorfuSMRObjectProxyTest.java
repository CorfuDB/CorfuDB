package org.corfudb.runtime.object;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.ObjectsView.ObjectID;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import org.corfudb.CustomSerializer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/21/16.
 */
public class CorfuSMRObjectProxyTest extends AbstractObjectTest {
    @Test
    public void canReadWriteToSingle() {
        getDefaultRuntime();

        ICorfuTable<String, String> testMap = instantiateCorfuObject(PersistentCorfuTable.getTableType(), "test");

        testMap.clear();
        assertThat(testMap.get("a")).isNull();
        testMap.insert("a", "a");
        assertThat(testMap.get("a")).isEqualTo("a");
        testMap.insert("a", "b");
        assertThat(testMap.get("a")).isEqualTo("b");

        ICorfuTable<String, String> testMap2 = instantiateCorfuObject(PersistentCorfuTable.getTableType(), "test");

        assertThat(testMap2.get("a"))
                .isEqualTo("b");
    }

    @Test
    public void canOpenObjectWithTwoRuntimes() {
        getDefaultRuntime();

        final int TEST_VALUE = 42;
        TestClass testClass = instantiateCorfuObject(new TypeToken<TestClass>() {}, "test");

        testClass.set(TEST_VALUE);
        assertThat(testClass.get())
                .isEqualTo(TEST_VALUE);

        CorfuRuntime runtime2 = getNewRuntime(getDefaultNode());
        runtime2.connect();

        TestClass testClass2 = instantiateCorfuObject(runtime2, new TypeToken<TestClass>() {}, "test");

        assertThat(testClass2.get())
                .isEqualTo(TEST_VALUE);
    }

    @Test
    public void canUseAnnotations()
            throws Exception {
        getDefaultRuntime();

        TestClassUsingAnnotation test = instantiateCorfuObject(new TypeToken<TestClassUsingAnnotation>() {}, "test");

        assertThat(test.testFn1())
                .isTrue();

        assertThat(test.testIncrement())
                .isTrue();

        assertThat(test.getValue())
                .isNotZero();

        // clear the cache, forcing a new object to be built.
        getRuntime().getObjectsView().getObjectCache().clear();

        TestClassUsingAnnotation test2 = instantiateCorfuObject(TestClassUsingAnnotation.class, "test");

        assertThat(test)
                .isNotSameAs(test2);

        assertThat(test2.getValue())
                .isNotZero();
    }

    @Test
    public void canUseCustomSerializer() {
        //Register a custom serializer and use it with an SMR object
        ISerializer customSerializer = new CustomSerializer((byte) (Serializers.SYSTEM_SERIALIZERS_COUNT + 1));
        getDefaultRuntime().getSerializers().registerSerializer(customSerializer);
        CorfuRuntime r = getDefaultRuntime();

        ICorfuTable<String, String> test = r.getObjectsView().build()
                .setTypeToken(PersistentCorfuTable.<String,String>getTableType())
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
     */
    @Test
    public void doesNotResetSerializerIfMapAlreadyExists() {
        ISerializer customSerializer = new CustomSerializer((byte) (Serializers.SYSTEM_SERIALIZERS_COUNT + 1));
        getDefaultRuntime().getSerializers().registerSerializer(customSerializer);
        CorfuRuntime r = getDefaultRuntime();

        ICorfuTable<String, String> test = r.getObjectsView().build()
                .setTypeToken(PersistentCorfuTable.<String,String>getTableType())
                .setStreamName("test")
                .setSerializer(customSerializer)
                .open();

        ICorfuTable<String, String> test2 = r.getObjectsView().build()
                .setTypeToken(PersistentCorfuTable.<String,String>getTableType())
                .setStreamName("test")
                .setSerializer(Serializers.getDefaultSerializer())
                .open();

        ObjectID<?> mapId = new ObjectID<>(CorfuRuntime.getStreamID("test"), PersistentCorfuTable.class);

        MVOCorfuCompileProxy<?> cp = ((MVOCorfuCompileProxy<?>) ((ICorfuSMR<?>) r.getObjectsView().
                getObjectCache().
                get(mapId)).
                getCorfuSMRProxy());

        assertThat(cp.getSerializer()).isEqualTo(customSerializer);
    }

}
