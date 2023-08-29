package org.corfudb.runtime.collections.vavr;

import io.vavr.Tuple2;
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
    private static final int SMALL_TREE_ELEMENTS = 100;
    private static final int LARGE_TREE_ELEMENTS = 1000;
    private static final String PAYLOAD_PREFIX = "payload_";
    private static final String KEY_PREFIX = "key_";

    private static final IntWithHashCode key1 = new IntWithHashCode(1);
    private static final IntWithHashCode key2 = new IntWithHashCode(2);
    private static final IntWithHashCode key101 = new IntWithHashCode(101);
    private static final ExampleValue value1 = ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 1).build();
    private static final ExampleValue value2 = ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 2).build();
    private static final ExampleValue value101 = ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 101).build();

    @Test
    public void testCollision() {
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt = HashArrayMappedTrie.empty();
        hamt = hamt.put(key1, value1);
        hamt = hamt.put(key101, value101);
        assertEquals(hamt.get(key1).get(), value1);
        assertEquals(hamt.get(key101).get(), value101);
        assertThat(hamt instanceof HashArrayMappedTrieModule.LeafList);
    }

    @Test
    public void testGetAndPutNodeBasic() {
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt1 = HashArrayMappedTrie.empty();
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt2 = HashArrayMappedTrie.empty();
        assertTrue(hamt1.getNode(key1).isEmpty());

        hamt1 = hamt1.put(key1, value1);
        hamt2 = hamt2.putNode(hamt1.getNode(key1).get());
        assertSame(hamt1.getNode(key1).get(), hamt2.getNode(key1).get());

        hamt1 = hamt1.put(key2, value2);
        hamt2 = hamt2.put(key2, value2);
        assertNotSame(hamt1.getNode(key2).get(), hamt2.getNode(key2).get());
    }

    @Test
    public void testGetAndPutNodeFromLeafList() {
        IntWithHashCode key2 = new IntWithHashCode(2);
        IntWithHashCode key201 = new IntWithHashCode(201);
        ExampleValue value2 = ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 2).build();
        ExampleValue value201 = ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 201).build();

        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt1 = HashArrayMappedTrie.empty();
        hamt1 = hamt1.put(key1, value1);
        hamt1 = hamt1.put(key101, value101); //hamt1 is a LeafListNode now

        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt2 = HashArrayMappedTrie.empty();
        hamt2 = hamt2.put(key2, value2);
        hamt2 = hamt2.put(key201, value201);

        hamt1 = hamt1.putNode(hamt2.getNode(key2).get()); //key2 will not be added to the existing LeafList
        assertSame(hamt1.getNode(key2).get(), hamt2.getNode(key2).get());

        hamt1 = hamt1.putNode(hamt2.getNode(key201).get()); //key201 will be added to the existing LeafList
        assertEquals(hamt1.get(key201).get(), value201); //key201 is found
        assertTrue(hamt1.getNode(key201).isEmpty()); //LeafList doesn't support getNode
    }

    @Test
    public void testGetAndPutNodeFromIndexNode() {
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt1 = buildHamt(SMALL_TREE_ELEMENTS);
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt2 = buildHamt(SMALL_TREE_ELEMENTS);
        IntWithHashCode hamtKey = new IntWithHashCode(1);

        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt3 = hamt2.putNode(hamt1.getNode(hamtKey).get());

        HashArrayMappedTrieModule.LeafSingleton<IntWithHashCode, ExampleValue> l1 = hamt1.getNode(hamtKey).get();
        HashArrayMappedTrieModule.LeafSingleton<IntWithHashCode, ExampleValue> l2 = hamt2.getNode(hamtKey).get();
        HashArrayMappedTrieModule.LeafSingleton<IntWithHashCode, ExampleValue> l3 = hamt3.getNode(hamtKey).get();
        assertNotSame(l1, l2);
        assertSame(l1, l3);
    }

    @Test
    public void testGetAndPutNodeFromArrayNode() {
        HashArrayMappedTrie<String, ExampleValue> hamt1 = buildHamtWithStringKey(LARGE_TREE_ELEMENTS);
        HashArrayMappedTrie<String, ExampleValue> hamt2 = buildHamtWithStringKey(LARGE_TREE_ELEMENTS);
        String hamtKey = KEY_PREFIX + 100;

        HashArrayMappedTrie<String, ExampleValue> hamt3 = hamt2.putNode(hamt1.getNode(hamtKey).get());

        HashArrayMappedTrieModule.LeafSingleton<String, ExampleValue> l1 = hamt1.getNode(hamtKey).get();
        HashArrayMappedTrieModule.LeafSingleton<String, ExampleValue> l2 = hamt2.getNode(hamtKey).get();
        HashArrayMappedTrieModule.LeafSingleton<String, ExampleValue> l3 = hamt3.getNode(hamtKey).get();
        assertNotSame(l1, l2);
        assertSame(l1, l3);
    }

    @Test
    public void testRemoveNonExistentKey() {
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt = buildHamt(SMALL_TREE_ELEMENTS);
        IntWithHashCode hamtKey = new IntWithHashCode(101);
        assertTrue(hamt.get(hamtKey).isEmpty());
        hamt = hamt.remove(hamtKey);
        assertTrue(hamt.get(hamtKey).isEmpty());
    }

    @Test
    public void testRemoveKey() {
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt = buildHamt(SMALL_TREE_ELEMENTS);
        IntWithHashCode hamtKey = new IntWithHashCode(1);
        assertEquals(hamt.get(hamtKey).get(), ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 1).build());
        hamt = hamt.remove(hamtKey);
        assertTrue(hamt.get(hamtKey).isEmpty());
    }

    @Test
    public void testRemoveKeyFromArrayNode() {
        HashArrayMappedTrie<String, ExampleValue> hamt = buildHamtWithStringKey(LARGE_TREE_ELEMENTS);
        String hamtKey = KEY_PREFIX + 100;
        assertEquals(hamt.get(hamtKey).get(), ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 100).build());
        hamt = hamt.remove(hamtKey);
        assertTrue(hamt.get(hamtKey).isEmpty());
    }

    @Test
    public void testIterator() {
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt = buildHamt(SMALL_TREE_ELEMENTS);
        Set<IntWithHashCode> expectedKeySet = new HashSet<>();
        for (int i = 0; i < SMALL_TREE_ELEMENTS; i++) {
            expectedKeySet.add(new IntWithHashCode(i));
        }
        Set<IntWithHashCode> actualKeySet = hamt.iterator().map(Tuple2::_1).toJavaSet();
        assertTrue(actualKeySet.equals(expectedKeySet));
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
