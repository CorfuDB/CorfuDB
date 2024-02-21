package org.corfudb.runtime.collections.vavr;

import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("checkstyle:magicnumber")
public class HashArrayMappedTrieTest {
    private static final int INDEX_NODE_ELEMENTS = 15;
    private static final int ARRAY_NODE_ELEMENTS = 1000;
    private static final String PAYLOAD_PREFIX = "payload_";
    private static final String KEY_PREFIX = "key_";

    private static final IntWithHashCode key1 = new IntWithHashCode(1);
    private static final IntWithHashCode key2 = new IntWithHashCode(2);
    private static final IntWithHashCode key101 = new IntWithHashCode(101);
    private static final ExampleValue value1 = ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 1).build();
    private static final ExampleValue value2 = ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 2).build();
    private static final ExampleValue value101 = ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 101).build();

    @Test
    public void testLeafSingleton() {
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt1 = HashArrayMappedTrie.empty();
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt2 = HashArrayMappedTrie.empty();
        assertTrue(!hamt1.getNode(key1).isPresent());

        //Get and Put
        hamt1 = hamt1.put(key1, value1);
        hamt2 = hamt2.putNode(hamt1.getNode(key1).get());
        assertSame(hamt1.getNode(key1).get(), hamt2.getNode(key1).get());
        assertSame(hamt1.get(key1).get(), hamt2.get(key1).get());

        //Remove
        hamt1 = hamt1.remove(key1);
        assertThat(!hamt1.get(key1).isPresent());
        assertSame(hamt2.get(key1).get(), value1);

        //Remove non-existent key
        hamt1 = hamt1.remove(key1);
        assertThat(!hamt1.get(key1).isPresent());
    }

    @Test
    public void testLeafList() {
        IntWithHashCode key2 = new IntWithHashCode(2);
        IntWithHashCode key201 = new IntWithHashCode(201);
        ExampleValue value2 = ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 2).build();
        ExampleValue value201 = ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 201).build();

        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt1 = HashArrayMappedTrie.empty();
        hamt1 = hamt1.put(key1, value1);
        hamt1 = hamt1.put(key101, value101);
        //hamt1 is a LeafListNode
        assertEquals(hamt1.get(key1).get(), value1);
        assertEquals(hamt1.get(key101).get(), value101);

        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt2 = HashArrayMappedTrie.empty();
        hamt2 = hamt2.put(key2, value2);
        hamt2 = hamt2.put(key201, value201);

        hamt1 = hamt1.putNode(hamt2.getNode(key2).get()); //key2 will not be added to the existing LeafList
        assertSame(hamt1.getNode(key2).get(), hamt2.getNode(key2).get());

        hamt1 = hamt1.putNode(hamt2.getNode(key201).get()); //key201 will be added to the existing LeafList
        assertEquals(hamt1.get(key201).get(), value201); //key201 is found
        assertTrue(!hamt1.getNode(key201).isPresent()); //LeafList doesn't support getNode

        //Remove existing key
        hamt1 = hamt1.remove(key1);
        assertTrue(!hamt1.get(key1).isPresent());

        //Remove non-existent key
        hamt1 = hamt1.remove(key1);
        assertTrue(!hamt1.get(key1).isPresent());
    }

    @Test
    public void testIndexNode() {
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt1 = buildHamt(INDEX_NODE_ELEMENTS);
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt2 = buildHamt(INDEX_NODE_ELEMENTS);
        IntWithHashCode hamtKey = new IntWithHashCode(1);

        //Get and Put
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt3 = hamt2.putNode(hamt1.getNode(hamtKey).get());
        HashArrayMappedTrieModule.LeafSingleton<IntWithHashCode, ExampleValue> l1 = hamt1.getNode(hamtKey).get();
        HashArrayMappedTrieModule.LeafSingleton<IntWithHashCode, ExampleValue> l2 = hamt2.getNode(hamtKey).get();
        HashArrayMappedTrieModule.LeafSingleton<IntWithHashCode, ExampleValue> l3 = hamt3.getNode(hamtKey).get();
        assertNotSame(l1, l2);
        assertSame(l1, l3);

        //Remove key
        assertEquals(hamt1.get(hamtKey).get(), value1);
        hamt1 = hamt1.remove(hamtKey);
        assertTrue(!hamt1.get(hamtKey).isPresent());
        assertEquals(hamt3.get(hamtKey).get(), value1);

        //Remove non-existent key
        hamt1 = hamt1.remove(hamtKey);
        assertTrue(!hamt1.get(hamtKey).isPresent());
    }

    @Test
    public void testArrayNode() {
        HashArrayMappedTrie<String, ExampleValue> hamt1 = buildHamtWithStringKey(ARRAY_NODE_ELEMENTS);
        HashArrayMappedTrie<String, ExampleValue> hamt2 = buildHamtWithStringKey(ARRAY_NODE_ELEMENTS);
        String hamtKey = KEY_PREFIX + 100;

        //Get and Put
        HashArrayMappedTrie<String, ExampleValue> hamt3 = hamt2.putNode(hamt1.getNode(hamtKey).get());
        HashArrayMappedTrieModule.LeafSingleton<String, ExampleValue> l1 = hamt1.getNode(hamtKey).get();
        HashArrayMappedTrieModule.LeafSingleton<String, ExampleValue> l2 = hamt2.getNode(hamtKey).get();
        HashArrayMappedTrieModule.LeafSingleton<String, ExampleValue> l3 = hamt3.getNode(hamtKey).get();
        assertNotSame(l1, l2);
        assertSame(l1, l3);

        //Remove existing key
        ExampleValue hamtValue = ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 100).build();
        assertEquals(hamt1.get(hamtKey).get(), hamtValue);
        hamt1 = hamt1.remove(hamtKey);
        assertTrue(!hamt1.get(hamtKey).isPresent());
        assertEquals(hamt3.get(hamtKey).get(), hamtValue);

        //Remove non-existent key
        hamt1 = hamt1.remove(hamtKey);
        assertTrue(!hamt1.get(hamtKey).isPresent());
    }

    @Test
    public void testIterator() {
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt = buildHamt(ARRAY_NODE_ELEMENTS);
        Set<IntWithHashCode> expectedKeySet = new HashSet<>();
        for (int i = 0; i < ARRAY_NODE_ELEMENTS; i++) {
            expectedKeySet.add(new IntWithHashCode(i));
        }
        Set<IntWithHashCode> actualKeySet = hamt.getKeySet();
        assertTrue(actualKeySet.equals(expectedKeySet));

        System.out.println(hamt.toString());
    }

    HashArrayMappedTrie<IntWithHashCode, ExampleValue> buildHamt(int numElements) {
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt = HashArrayMappedTrie.empty();
        for (int i = 0; i < numElements; i++) {
            hamt = hamt.put(new IntWithHashCode(i), ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + i).build());
        }
        return hamt;
    }

    HashArrayMappedTrie<String, ExampleValue> buildHamtWithStringKey(int numElements) {
        HashArrayMappedTrie<String, ExampleValue> hamt = HashArrayMappedTrie.empty();
        for (int i = 0; i < numElements; i++) {
            hamt = hamt.put(KEY_PREFIX + i, ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + i).build());
        }
        return hamt;
    }

    private static class IntWithHashCode {
        final int key;

        public IntWithHashCode(int key) {
            this.key = key;
        }

        @Override
        public int hashCode() {
            return key % 100;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            IntWithHashCode intWithHashCode = (IntWithHashCode) o;
            return this.key == intWithHashCode.key;
        }
    }
}
