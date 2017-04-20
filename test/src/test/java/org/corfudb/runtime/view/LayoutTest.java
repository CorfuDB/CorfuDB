package org.corfudb.runtime.view;

import groovy.util.logging.Slf4j;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.reflect.Field;
import static java.lang.reflect.Modifier.TRANSIENT;
import java.nio.file.Files;
import java.nio.file.Paths;


/**
 * Created by rmichoud on 4/17/17.
 */
@Slf4j
public class LayoutTest {

    /* Helper */
    private String getResourceJSONFileAsString(String fileName)
            throws IOException{

        return new String(Files.readAllBytes(Paths.get("src/test/resources/JSONLayouts", fileName)));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotDeserializeEmptyLayout()
            throws Exception {

        String JSONEmptyLayout = getResourceJSONFileAsString("EmptyLayout.json");
        Layout shouldYieldException = Layout.fromJSONString(JSONEmptyLayout);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotDeserializeMissingRequiredFieldLayout()
            throws Exception{

        String JSONMissingRequiredFieldLayout =  getResourceJSONFileAsString("MissingRequiredFieldLayout.json");
        Layout shouldYieldException = Layout.fromJSONString(JSONMissingRequiredFieldLayout);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotDeserializeMissingRequiredFieldInnerLayout()
            throws Exception {
        String JSONMissingRequiredFieldInnerLayout = getResourceJSONFileAsString("MissingRequiredFieldInStripes.json");
        Layout shouldYieldException = Layout.fromJSONString(JSONMissingRequiredFieldInnerLayout);
    }

    @Test
    public void canDeserializeMissingNotRequiredFieldLayout() throws Exception {
        String JSONMissingNotRequiredFieldLayout = getResourceJSONFileAsString("MissingNotRequiredFieldLayout.json");
        Layout safeLayout = Layout.fromJSONString(JSONMissingNotRequiredFieldLayout);

        /* Assert that no field is null */
        Field[] fields = Layout.class.getDeclaredFields();
        for (Field f : fields) {
            if (!f.isAccessible()) {
                f.setAccessible(true);
            }

            /* Transient field by definition are not deserialized */
            if (f.getModifiers() != TRANSIENT) {
                assertThat(f.get(safeLayout)).isNotNull();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldInvalidateNotValidLayout() throws Exception {
        String JSONEmptySequencerListLayout = getResourceJSONFileAsString("EmptyListOfSequencers.json");
        Layout shouldYieldException = Layout.fromJSONString(JSONEmptySequencerListLayout);
    }

}
