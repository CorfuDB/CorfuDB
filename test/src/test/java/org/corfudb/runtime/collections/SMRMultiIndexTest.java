package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 4/7/17.
 */
public class SMRMultiIndexTest extends AbstractViewTest {

    @lombok.Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    public CorfuRuntime r;


    @Before
    public void setRuntime() throws Exception {
        r = getDefaultRuntime().connect();
    }

    @Test
    public <K, V> void createIndexTest() throws Exception {
        List<SMRMultiIndex.IndexSpecification<String, String, String, Map.Entry>> indexSpecifications = new ArrayList<>();
        indexSpecifications.add(new SMRMultiIndex.IndexSpecification<String, String, String, Map.Entry>(
                "INDEX",
                Arrays.asList((String key , String val) -> key.contains("5") ? "five": key ),
               (String key , String val) -> new AbstractMap.SimpleEntry<String, String>(key, val)
        ));

         SMRMultiIndex<String, String, String, Map.Entry> multiIndexMap = r.getObjectsView()
                .build()
                .setType(SMRMultiIndex.class)
                .setStreamName("MultiIndexMap")
                .setTypeToken(new TypeToken<SMRMultiIndex<String, String, String, Map.Entry>>() {})
                .setArguments(indexSpecifications)
                .open();
        SMRMap<String, String> smrMap = r.getObjectsView()
                .build()
                .setType(SMRMap.class)
                .setStreamName("SMRMap")
                .setTypeToken(new TypeToken<SMRMap>() {})
                .open();

         final int samples = 500000;
         final int queries = 100;

        long t = System.currentTimeMillis();
         for(int i = 0; i < samples ; i++) {
             multiIndexMap.put("key" + i, "value" + i);
         }
        System.out.println("");
        System.out.println("Time:Puts:" + (System.currentTimeMillis() - t));
        t = System.currentTimeMillis();
        for(int i=0; i < queries; i++) {
            Collection<Map.Entry<String, Map.Entry>> left = multiIndexMap.getByNamedIndex("INDEX", "key1");
            Collection<Map.Entry<String, Map.Entry>> right = multiIndexMap.getByNamedIndex("INDEX", "five");
            //System.out.println(Stream.concat(left.stream().map(e -> e.getKey()), right.stream().map(e -> e.getKey())).collect(Collectors.toList()));
            Stream.concat(left.stream().map(e -> e.getKey()), right.stream().map(e -> e.getKey())).collect(Collectors.toList());
        }
        System.out.println("Time:Query IDX:"+(System.currentTimeMillis() - t));

        t = System.currentTimeMillis();
        for(int i = 0; i < samples ; i++) {
            smrMap.put("key" + i, "value" + i);
        }
        System.out.println("Time:Puts:" + (System.currentTimeMillis() - t));
        t = System.currentTimeMillis();
        for(int i=0; i < queries; i++) {
            //System.out.println(smrMap.scanAndFilterByEntry(e -> e.getKey().equals("key1") || e.getKey().contains("5")).stream().map(e -> e.getKey()).collect(Collectors.toList()));
            smrMap.scanAndFilterByEntry(e -> e.getKey().equals("key1") || e.getKey().contains("5")).stream().map(e -> e.getKey()).collect(Collectors.toList());

        }
        System.out.println("Time:Query SF:"+(System.currentTimeMillis() - t));


    }
    /*

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
    */


}
