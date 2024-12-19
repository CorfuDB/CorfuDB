package org.corfudb.infrastructure.logreplication.PgUtils;

import lombok.extern.slf4j.Slf4j;
import lombok.Getter;

import java.sql.SQLException;

@Slf4j
public class SQLExceptionHandler {

    @Getter
    public enum SQLState {
        OBJECT_ALREADY_EXISTS("42710"),
        OBJECT_UNDEFINED("42704"),
        TABLE_ALREADY_EXISTS("42P07");

        private final String code;

        SQLState(String code) {
            this.code = code;
        }

        public static SQLState fromCode(String code) {
            for (SQLState state : values()) {
                if (state.code.equals(code)) {
                    return state;
                }
            }
            return null;
        }
    }

    public static void handleSQLException(SQLException e) throws PostgresException {
        String sqlState = e.getSQLState();
        SQLState state = SQLState.fromCode(sqlState);

        if (state != null) {
            switch (state) {
                case OBJECT_ALREADY_EXISTS:
                case TABLE_ALREADY_EXISTS:
                    log.info("Object/Table already exists!");
                    throw new ObjectAlreadyExistsException(
                            "Database object already exists", sqlState, e);

                case OBJECT_UNDEFINED:
                    log.info("Object is undefined!");
                    throw new ObjectUndefinedException(
                            "Database object is undefined", sqlState, e);
            }
        }

        if (e.getMessage().contains("replication slot") &&
                e.getMessage().contains("does not exist")) {
            throw new ReplicationSlotDoesNotExistException(
                    "Replication slot error occurred", sqlState, e);
        }

        log.error("Unhandled SQL exception", e);
        throw new PostgresException("Database operation failed", sqlState, e);
    }

    public static class ObjectAlreadyExistsException extends PostgresException {
        public ObjectAlreadyExistsException(String message, String sqlState, Throwable cause) {
            super(message, sqlState, cause);
        }
    }

    public static class ObjectUndefinedException extends PostgresException {
        public ObjectUndefinedException(String message, String sqlState, Throwable cause) {
            super(message, sqlState, cause);
        }
    }

    public static class ReplicationSlotDoesNotExistException extends PostgresException {
        public ReplicationSlotDoesNotExistException(String message, String sqlState, Throwable cause) {
            super(message, sqlState, cause);
        }
    }
}