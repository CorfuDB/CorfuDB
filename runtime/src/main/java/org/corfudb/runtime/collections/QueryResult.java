package org.corfudb.runtime.collections;

import java.util.Collection;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Created by zlokhandwala on 2019-08-10.
 */
@EqualsAndHashCode
public class QueryResult<E> {

    @Getter
    private final Collection<E> result;

    public QueryResult(Collection<E> result) {
        this.result = result;
    }
}
