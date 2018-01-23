package org.corfudb.runtime.object;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
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
                instantiateCorfuObject(new TypeToken<SMRMap<String,String>>() {}, "test");

        testMap.clear();
        assertThat(testMap.put("a", "a"))
                .isNull();
        assertThat(testMap.put("a", "b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");

        Map<String, String> testMap2 = (Map<String, String>)
                instantiateCorfuObject(new TypeToken<SMRMap<String,String>>() {}, "test");

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

    /* Test disabled until SMRObjectProxy is merged in
    @Test
    @SuppressWarnings("unchecked")
    public void multipleWritesConsistencyTest()
            throws Exception {
        getDefaultRuntime().connect();

        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<TreeMap<String,String>>() {})
                .open();

        testMap.clear();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                    .isNull();
        }

        Map<String, String> testMap2 = getRuntime().getObjectsView().open(
                CorfuRuntime.getStreamID("test"), TreeMap.class);
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {

            assertThat(testMap2.get(Integer.toString(i)))
                    .isEqualTo(Integer.toString(i));
        }
    }
    */

    /* Test disabled until SMRObjectProxy is merged in.
    @Test
    @SuppressWarnings("unchecked")
    public void multipleWritesConsistencyTestConcurrent()
            throws Exception {
        getDefaultRuntime().connect();


        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<TreeMap<String,String>>() {})
                .open();

        testMap.clear();
        int num_threads = PARAMETERS.CONCURRENCY_SOME;
        int num_records = PARAMETERS.NUM_ITERATIONS_LOW;

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                        .isEqualTo(null);
            }
        });
        executeScheduled(num_threads, PARAMETERS.TIMEOUT_LONG);

        Map<String, String> testMap2 = getRuntime().getObjectsView()
                .build()
                .setStreamName("A")
                .setTypeToken(new TypeToken<TreeMap<String, String>>() {})
                .open();

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testMap2.get(Integer.toString(i)))
                        .isEqualTo(Integer.toString(i));
            }
        });
        executeScheduled(num_threads, PARAMETERS.TIMEOUT_LONG);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canWrapObjectWithPrimitiveTypes()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime().connect();
        TestClassWithPrimitives test = r.getObjectsView().build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<TestClassWithPrimitives>() {})
                .open();

        test.setPrimitive("hello world".getBytes());
        assertThat(test.getPrimitive())
                .isEqualTo("hello world".getBytes());
    }
    */

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

    /** Disabled pending resolution of issue #285
    @Test
    public void deadLockTest() throws Exception {
        CorfuRuntime runtime = getDefaultRuntime().connect();
        Map<String, Integer> map =
                runtime.getObjectsView()
                        .build()
                        .setStreamName("M")
                        .setType(SMRMap.class)
                        .open();

        for(int x = 0; x < PARAMETERS.NUM_ITERATIONS_LOW; x++) {
            // thread 1: update "a" and "b" atomically
            Thread t1 = new Thread(() -> {
                runtime.getObjectsView().TXBegin();
                map.put("a", 1);
                map.put("b", 1);
                runtime.getObjectsView().TXEnd();
            }
            );
            t1.start();

            // thread 2: read "a", then "b"
            Thread t2 = new Thread(() -> {
                map.get("a");
                map.get("b");
            });
            t2.start();

            t1.join(PARAMETERS.TIMEOUT_NORMAL.toMillis());
            t2.join(PARAMETERS.TIMEOUT_NORMAL.toMillis());

            assertThat(t1.isAlive()).isFalse();
            assertThat(t2.isAlive()).isFalse();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canUsePrimitiveSerializer()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime().connect();
        TestClassWithPrimitives test = r.getObjectsView().build()
                .setType(TestClassWithPrimitives.class)
                .setStreamName("test")
                .setSerializer(Serializers.PRIMITIVE)
                .open();

        test.setPrimitive("hello world".getBytes());
        assertThat(test.getPrimitive())
                .isEqualTo("hello world".getBytes());
    }
     **/

    @Test
    @SuppressWarnings("unchecked")
    public void canUseCustomSerializer() throws Exception {
        //Register a custom serializer and use it with an SMR object
        ISerializer customSerializer = new CustomSerializer((byte) (Serializers.SYSTEM_SERIALIZERS_COUNT + 1));
        Serializers.registerSerializer(customSerializer);
        CorfuRuntime r = getDefaultRuntime();

        Map<String, String> test = r.getObjectsView().build()
                .setType(SMRMap.class)
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
                .setType(SMRMap.class)
                .setStreamName("test")
                .setSerializer(customSerializer)
                .open();

        Map<String, String> test2 = r.getObjectsView().build()
                .setType(SMRMap.class)
                .setStreamName("test")
                .setSerializer(Serializers.JSON)
                .open();

        ObjectsView.ObjectID mapId = new ObjectsView.
                ObjectID(CorfuRuntime.getStreamID("test"), SMRMap.class);

        CorfuCompileProxy cp = ((CorfuCompileProxy) ((ICorfuSMR) r.getObjectsView().
                getObjectCache().
                get(mapId)).
                getCorfuSMRProxy());

        assertThat(cp.getSerializer()).isEqualTo(customSerializer);
    }

}
