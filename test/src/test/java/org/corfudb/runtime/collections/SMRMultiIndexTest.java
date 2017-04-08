package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import lombok.Data;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 4/7/17.
 */
public class SMRMultiIndexTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    public CorfuRuntime r;

    public class IndexRow {

        @Getter
        final int id;

        @Getter
        final String i0;

        @Getter
        final String i1;

        @Getter
        final String i2;

        public IndexRow(int id, String i0, String i1, String i2)
        {
            this.id = id;
            this.i0 = i0;
            this.i1 = i1;
            this.i2 = i2;
        }

    }

    @Before
    public void setRuntime() throws Exception {
        r = getDefaultRuntime().connect();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadFromEachIndex()
            throws Exception {

        List<SMRMultiIndex.IndexFunction<IndexRow>> indexFunctions =
                ImmutableList.<SMRMultiIndex.IndexFunction<IndexRow>>builder()
                .add(r -> r.getI0())
                .add(r -> r.getI1())
                .add(r -> r.getI2())
                .build();

        SMRMultiIndex<String, IndexRow> testMap = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMultiIndex<String, IndexRow>>() {})
                .setArguments(indexFunctions)
                .open();

        IndexRow row0 =  new IndexRow(0, "a0", "b0", "c0");
        IndexRow row1 =  new IndexRow(1, "a1", "b1", "c1");

        testMap.put("test0", row0);
        testMap.put("test1", row1);

        assertThat(testMap.getByRowIndex("test0").getId())
                .isEqualTo(0);
        assertThat(testMap.getByRowIndex("test1").getId())
                .isEqualTo(1);

        assertThat(testMap.getByColumnIndex(0, "a0"))
                .contains(row0);
        assertThat(testMap.getByColumnIndex(1, "b0"))
                .contains(row0);
        assertThat(testMap.getByColumnIndex(2, "c0"))
                .contains(row0);

        assertThat(testMap.getByColumnIndex(0, "a1"))
                .contains(row1);
        assertThat(testMap.getByColumnIndex(1, "b1"))
                .contains(row1);
        assertThat(testMap.getByColumnIndex(2, "c1"))
                .contains(row1);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void indexContainsMultipleCols()
            throws Exception {

        List<SMRMultiIndex.IndexFunction<IndexRow>> indexFunctions =
                ImmutableList.<SMRMultiIndex.IndexFunction<IndexRow>>builder()
                        .add(r -> r.getI0())
                        .add(r -> r.getI1())
                        .add(r -> r.getI2())
                        .build();

        SMRMultiIndex<String, IndexRow> testMap = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMultiIndex<String, IndexRow>>() {})
                .setArguments(indexFunctions)
                .open();

        IndexRow row0 =  new IndexRow(0, "a0", "b0", "c0");
        IndexRow row1 =  new IndexRow(1, "a0", "b1", "c0");

        testMap.put("test0", row0);
        testMap.put("test1", row1);

        assertThat(testMap.getByRowIndex("test0").getId())
                .isEqualTo(0);
        assertThat(testMap.getByRowIndex("test1").getId())
                .isEqualTo(1);

        assertThat(testMap.getByColumnIndex(0, "a0"))
                .contains(row0)
                .contains(row0);

        assertThat(testMap.getByColumnIndex(1, "b0"))
                .contains(row0);

        assertThat(testMap.getByColumnIndex(1, "b1"))
                .contains(row1);

        assertThat(testMap.getByColumnIndex(2, "c0"))
                .contains(row0)
                .contains(row1);
    }
}
