package org.corfudb.infrastructure.logreplication.PgUtils;

import lombok.Getter;

public class PostgresException extends RuntimeException {

    @Getter
    private final String sqlState;

    public PostgresException(String message, String sqlState, Throwable cause) {
        super(message, cause);
        this.sqlState = sqlState;
    }
}