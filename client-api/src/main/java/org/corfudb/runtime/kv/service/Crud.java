package org.corfudb.runtime.kv.service;

public interface Crud {

    enum CrudOpType {
        CREATE, GET, UPDATE, DELETE
    }

    interface CrudOp<Result> {
        CrudOpType type();

        Result apply();
    }
}
