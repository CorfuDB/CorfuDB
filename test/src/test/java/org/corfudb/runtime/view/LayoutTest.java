package org.corfudb.runtime.view;

import com.google.common.collect.Sets;
import groovy.util.logging.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import static java.lang.reflect.Modifier.TRANSIENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


/**
 * Created by rmichoud on 4/17/17.
 */
@Slf4j
public class LayoutTest {

    /* Helper */
    private String getResourceJSONFileAsString(String fileName) throws IOException {
        return new String(Files.readAllBytes(Paths.get("src/test/resources/JSONLayouts", fileName)));
    }

    @Test
    public void shouldNotDeserializeEmptyLayout() throws Exception {
        String JSONEmptyLayout = getResourceJSONFileAsString("EmptyLayout.json");
        assertThatThrownBy(() -> Layout.fromJSONString(JSONEmptyLayout))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void checkAllActiveLayoutServers() throws IOException {
        String JSONEmptyLayout = getResourceJSONFileAsString("DefaultLayout.json");
        Layout layout = Layout.fromJSONString(JSONEmptyLayout);
        layout.unresponsiveServers.add("localhost:9001");

        assertThat(layout.getActiveLayoutServers()).containsExactly("localhost:9000", "localhost:9002");
    }

    @Test
    public void shouldNotDeserializeMissingRequiredFieldLayout() throws Exception {
        String JSONMissingRequiredFieldLayout = getResourceJSONFileAsString("MissingRequiredFieldLayout.json");
        assertThatThrownBy(() -> Layout.fromJSONString(JSONMissingRequiredFieldLayout))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void shouldNotDeserializeMissingRequiredFieldInnerLayout() throws Exception {
        String JSONMissingRequiredFieldInnerLayout = getResourceJSONFileAsString("MissingRequiredFieldInStripes.json");
        assertThatThrownBy(() -> Layout.fromJSONString(JSONMissingRequiredFieldInnerLayout))
                .isInstanceOf(NullPointerException.class);
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

    @Test
    public void shouldInvalidateNotValidLayout() throws Exception {
        String JSONEmptySequencerListLayout = getResourceJSONFileAsString("EmptyListOfSequencers.json");
        assertThatThrownBy(() -> Layout.fromJSONString(JSONEmptySequencerListLayout))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testLayoutGetAllLogServers() throws Exception {
        String JSONEmptySequencerListLayout = getResourceJSONFileAsString("MultiSegmentLayout1.json");
        assertThat(Layout.fromJSONString(JSONEmptySequencerListLayout).getAllLogServers())
                .isEqualTo(Sets.newHashSet(
                        "localhost:9000", "localhost:9001",
                        "localhost:9002", "localhost:9003"
        ));
    }

    @Test
    public void testLayoutGetFullyRedundantLogServers() throws Exception {
        String JSONEmptySequencerListLayout = getResourceJSONFileAsString("MultiSegmentLayout2.json");
        assertThat(Layout.fromJSONString(JSONEmptySequencerListLayout).getFullyRedundantLogServers())
                .isEqualTo(Collections.singleton("localhost:9000"));
    }
}
