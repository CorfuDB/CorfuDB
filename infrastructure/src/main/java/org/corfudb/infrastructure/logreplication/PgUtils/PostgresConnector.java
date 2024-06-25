package org.corfudb.infrastructure.logreplication.PgUtils;

public class PostgresConnector {
    public final String ADDRESS;
    public final String PORT;

    public final String URL;

    public final String USER;

    public final String PASSWORD;

    public final String DATABASE_NAME;

    public PostgresConnector(String address, String port, String user, String password, String databaseName) {
        this.ADDRESS = address;
        this.PORT = port;
        this.URL = getConnectionString(address, port);
        this.USER = user;
        this.PASSWORD = password;
        this.DATABASE_NAME = databaseName;
    }

    private static String getConnectionString(String address, String port) {
        return "jdbc:postgresql://" + address + (port != null && !port.isEmpty() ? ":" + port : "5432") + "/postgres";
    }
}
