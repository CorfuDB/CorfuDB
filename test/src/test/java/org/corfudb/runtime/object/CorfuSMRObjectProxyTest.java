package org.corfudb.runtime.object;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.Map;

import org.corfudb.CustomSerializer;

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

        Map<String, String> testMap = (Map<String, String>)
                instantiateCorfuObject(new TypeToken<CorfuTable<String,String>>() {}, "test");

        testMap.clear();
        assertThat(testMap.put("a", "a"))
                .isNull();
        assertThat(testMap.put("a", "b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");

        Map<String, String> testMap2 = (Map<String, String>)
                instantiateCorfuObject(new TypeToken<CorfuTable<String,String>>() {}, "test");

        assertThat(testMap2.get("a"))
                .isEqualTo("b");
    }

    @Test
    public void canOpenObjectWithTwoRuntimes()
            throws Exception {
        getDefaultRuntime();

        final int TEST_VALUE = 42;
        TestClass testClass = (TestClass)
                instantiateCorfuObject(new TypeToken<TestClass>() {}, "test");

        testClass.set(TEST_VALUE);
        assertThat(testClass.get())
                .isEqualTo(TEST_VALUE);

        CorfuRuntime runtime2 = getNewRuntime(getDefaultNode());
        runtime2.connect();

        TestClass testClass2 = (TestClass)
                instantiateCorfuObject(runtime2,
                        new TypeToken<TestClass>() {}, "test");

        assertThat(testClass2.get())
                .isEqualTo(TEST_VALUE);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canUseAnnotations()
            throws Exception {
        getDefaultRuntime();

        TestClassUsingAnnotation test = (TestClassUsingAnnotation)
                instantiateCorfuObject(new TypeToken<TestClassUsingAnnotation>() {}, "test");

        assertThat(test.testFn1())
                .isTrue();

        assertThat(test.testIncrement())
                .isTrue();

        assertThat(test.getValue())
                .isNotZero();

        // clear the cache, forcing a new object to be built.
        getRuntime().getObjectsView().getObjectCache().clear();

        TestClassUsingAnnotation test2 = (TestClassUsingAnnotation)
                instantiateCorfuObject(TestClassUsingAnnotation.class, "test");

        assertThat(test)
                .isNotSameAs(test2);

        assertThat(test2.getValue())
                .isNotZero();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canUseCustomSerializer() throws Exception {
        //Register a custom serializer and use it with an SMR object
        ISerializer customSerializer = new CustomSerializer((byte) (Serializers.SYSTEM_SERIALIZERS_COUNT + 1));
        Serializers.registerSerializer(customSerializer);
        CorfuRuntime r = getDefaultRuntime();

        Map<String, String> test = r.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("test")
                .setSerializer(customSerializer)
                .open();

        test.put("a", "b");
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
        Serializers.registerSerializer(customSerializer);
        CorfuRuntime r = getDefaultRuntime();

        Map<String, String> test = r.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("test")
                .setSerializer(customSerializer)
                .open();

        Map<String, String> test2 = r.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("test")
                .setSerializer(Serializers.getDefaultSerializer())
                .open();

        ObjectsView.ObjectID mapId = new ObjectsView.
                ObjectID(CorfuRuntime.getStreamID("test"), CorfuTable.class);

        CorfuCompileProxy cp = ((CorfuCompileProxy) ((ICorfuSMR) r.getObjectsView().
                getObjectCache().
                get(mapId)).
                getCorfuSMRProxy());

        assertThat(cp.getSerializer()).isEqualTo(customSerializer);
    }

}
