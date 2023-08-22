package org.corfudb.runtime.collections;

import io.vavr.Tuple2;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

@SuppressWarnings("checkstyle:magicnumber")
public class HashArrayMappedTrieTest {
    private final int SMALL_TREE_ELEMENTS = 100;
    private final int LARGE_TREE_ELEMENTS = 1000;
    private final String PAYLOAD_PREFIX = "payload_";
    private final String KEY_PREFIX = "key_";

    @Test
    public void testCollision() {
        IntWithHashCode key1 = new IntWithHashCode(1);
        IntWithHashCode key2 = new IntWithHashCode(101);
        ExampleValue value1 = ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 1).build();
        ExampleValue value2 = ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 2).build();

        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt = HashArrayMappedTrie.empty();
        hamt = hamt.put(key1, value1);
        hamt = hamt.put(key2, value2);
        assertEquals(hamt.get(key1).get(), value1);
        assertEquals(hamt.get(key2).get(), value2);
        //LeafList doesn't support getNode
        assert(hamt.getNode(key1).isEmpty());
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
        assert(hamt.get(hamtKey).isEmpty());
        hamt = hamt.remove(hamtKey);
        assert(hamt.get(hamtKey).isEmpty());
    }

    @Test
    public void testRemoveKey() {
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt = buildHamt(SMALL_TREE_ELEMENTS);
        IntWithHashCode hamtKey = new IntWithHashCode(1);
        assertEquals(hamt.get(hamtKey).get(), ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 1).build());
        hamt = hamt.remove(hamtKey);
        assert(hamt.get(hamtKey).isEmpty());
    }

    @Test
    public void testRemoveKeyFromArrayNode() {
        HashArrayMappedTrie<String, ExampleValue> hamt = buildHamtWithStringKey(SMALL_TREE_ELEMENTS);
        String hamtKey = KEY_PREFIX + 1;
        assertEquals(hamt.get(hamtKey).get(), ExampleValue.newBuilder().setPayload(PAYLOAD_PREFIX + 1).build());
        hamt = hamt.remove(hamtKey);
        assert(hamt.get(hamtKey).isEmpty());
    }

    @Test
    public void testIterator() {
        HashArrayMappedTrie<IntWithHashCode, ExampleValue> hamt = buildHamt(SMALL_TREE_ELEMENTS);
        Set<IntWithHashCode> expectedKeySet = new HashSet<>();
        for (int i = 0; i < SMALL_TREE_ELEMENTS; i++) {
            expectedKeySet.add(new IntWithHashCode(i));
        }
        Set<IntWithHashCode> actualKeySet = hamt.iterator().map(Tuple2::_1).toJavaSet();
        assert(actualKeySet.equals(expectedKeySet));
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
