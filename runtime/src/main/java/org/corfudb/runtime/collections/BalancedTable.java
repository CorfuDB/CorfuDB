package org.corfudb.runtime.collections;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.view.ObjectBuilder;
import org.corfudb.runtime.view.ObjectOpenOptions;

import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;

/**
 * An implementation of CorfuTable that is backed by multiple instances of the stream. Access to the object
 * is load balanced over the streams to reduce the cost of syncing forward/backward.
 *
 * @author Maithem
 */
public class BalancedTable<K ,V, F extends Enum<F> & CorfuTable.IndexSpecification, I> implements ICorfuMap<K, V> {

    final CorfuTable<K ,V, F, I>[] tables;

    final Random rand = new Random();

    final CorfuRuntime rt;

    public BalancedTable(CorfuRuntime rt, int numVLOs, CorfuTable table) {
        if (numVLOs < 1) throw new IllegalArgumentException("numVLOs has to be at least 1");

        this.rt = rt;
        tables = new CorfuTable[numVLOs];

        ObjectBuilder<CorfuTable> builder = (ObjectBuilder) ((ICorfuSMR) table).getCorfuSMRProxy().getObjectBuilder();

        tables[0] = table;

        for (int x = 1; x < numVLOs - 1; x++) {
            tables[x] = builder.addOption(ObjectOpenOptions.NO_CACHE).open();
        }
    }


    private ICorfuMap<K, V> getTable() {
        int n = rand.nextInt(tables.length);
        return tables[n];
    };

    @Override
    public void insert(K key, V value) {
        getTable().insert(key, value);
    }

    @Override
    public void delete(K key) {
        getTable().delete(key);
    }


    @Override
    public Collection<Map.Entry<K, V>> scanAndFilterByEntry(Predicate<? super Entry<K, V>> entryPredicate) {
        return getTable().scanAndFilterByEntry(entryPredicate);
    }

    @Override
    public void clear() {
        getTable().clear();
    }

    @Override
    public boolean isEmpty() {
        return getTable().isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return getTable().keySet();
    }

    @Override
    public boolean containsKey(Object key) {
        return getTable().containsKey(key);
    }

    @Override
    public Collection<V> values() {
        return getTable().values();
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return getTable().entrySet();
    }

    @Override
    public V put(K key, V value) {
        return getTable().put(key, value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        getTable().putAll(m);
    }

    @Override
    public int size() {
        return getTable().size();
    }

    @Override
    public V remove(Object key) {
        return getTable().remove(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return getTable().containsKey(value);
    }

    @Override
    public V get(Object key) {
        return getTable().get(key);
    }
}
