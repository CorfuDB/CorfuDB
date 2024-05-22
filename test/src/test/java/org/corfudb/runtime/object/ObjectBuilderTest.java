package org.corfudb.runtime.object;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.util.ReflectionUtils;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests related to {@link ICorfuSMR} creation.
 */
public class ObjectBuilderTest {

    public interface BaseInterface { }
    public interface ChildInterface extends BaseInterface { }

    static class ChildImpl implements ChildInterface { }

    static class Base { }
    static class Child extends Base { }

    public static class ExampleInterface {
        public ExampleInterface(ChildInterface base) {
            // NOOP.
        }

        public ExampleInterface(BaseInterface base) {
            throw new IllegalStateException("Not suppose to be called");
        }
    }

    public static class Example {
        public Example() {
            // NOOP.
        }

        public Example(Base base) {
            throw new IllegalStateException("Not suppose to be called");
        }

        public Example(Child base) {
            // NOOP.
        }
    }

    @Test
    public void testObjectWithInvalidStreamId() {
        CorfuRuntime rt = new CorfuRuntime();
        assertThatThrownBy(() -> {
            SMRObject.builder()
                    .setCorfuRuntime(rt)
                    .setTypeToken(PersistentCorfuTable.<String, String>getTypeToken())
                    .open();
        }).isInstanceOf(NullPointerException.class)
          .hasMessageStartingWith("streamName is marked non-null but is null");
    }

    /**
     * Make sure that the correct constructor is being used
     * in case of constructor overloading.
     *
     * @throws IllegalAccessException should not be thrown
     * @throws InstantiationException should not be thrown
     * @throws InvocationTargetException should not be thrown
     */
    @Test
    public void constructorMatching()
            throws IllegalAccessException, InstantiationException, InvocationTargetException {
        Object[] args = { new Child() };
        ReflectionUtils.findMatchingConstructor(
                Example.class.getDeclaredConstructors(), args);
    }

    /**
     * Make sure that the correct constructor is being used
     * in case of constructor overloading (interface version).
     *
     * @throws IllegalAccessException should not be thrown
     * @throws InstantiationException should not be thrown
     * @throws InvocationTargetException should not be thrown
     */
    @Test
    public void constructorInterfaceMatching()
            throws IllegalAccessException, InstantiationException, InvocationTargetException {
        Object[] args = { new ChildImpl() };
        ReflectionUtils.findMatchingConstructor(
                ExampleInterface.class.getDeclaredConstructors(), args);
    }

    /**
     * Make sure that the correct constructor is being used
     * in case of a zero-arg constructor.
     *
     * @throws IllegalAccessException should not be thrown
     * @throws InstantiationException should not be thrown
     * @throws InvocationTargetException should not be thrown
     */
    @Test
    public void noArgeConstructorMatch()
            throws IllegalAccessException, InstantiationException, InvocationTargetException {
        Object[] args = { };
        ReflectionUtils.findMatchingConstructor(
                Example.class.getDeclaredConstructors(), args);
    }
}
