package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public interface TableStringApi {

    <K extends Message, V extends Message, M extends Message>
    void clear(@Nonnull String tableName);

    <K extends Message, V extends Message, M extends Message>
    void delete(@Nonnull String tableName,
                @Nonnull K key);

    <K extends Message, V extends Message, M extends Message>
    void touch(@Nonnull String tableName,
               @Nonnull K key);

    @Nonnull
    <K extends Message, V extends Message, M extends Message>
    CorfuStoreEntry<K, V, M> getRecord(@Nonnull String tableName,
                                       @Nonnull K key);

    @Nonnull
    <K extends Message, V extends Message, M extends Message, I>
    List<CorfuStoreEntry<K, V, M>> getByIndex(@Nonnull String tableName,
                                              @Nonnull String indexName,
                                              @Nonnull I indexKey);

    int count(@Nonnull String tableName);

    <K extends Message, V extends Message, M extends Message>
    Set<K> keySet(@Nonnull String tableName);

    /**
     * Variant of isExists that works on tableName instead of the table object.
     *
     * @param tableName - namespace + tablename of table being tested-     * @param key       - key to check for existence
     * @param <K>       - type of the key
     * @param <V>       - type of payload or value
     * @param <M>       - type of metadata
     * @return - true if record exists and false if record does not exist.
     */
    <K extends Message, V extends Message, M extends Message>
    boolean isExists(@Nonnull String tableName, @Nonnull final K key);

    <K extends Message, V extends Message, M extends Message>
    List<CorfuStoreEntry<K, V, M>> executeQuery(@Nonnull String tableName,
                                                @Nonnull Predicate<CorfuStoreEntry<K, V, M>> entryPredicate);


}
