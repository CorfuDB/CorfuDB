package org.corfudb.infrastructure.logreplication.PgUtils;

import lombok.ToString;

@ToString
public class PostgresConnector {
    public final String address;
    public final String port;

    public final String url;

    public final String user;

    public final String password;

    public final String databaseName;

    public PostgresConnector(String address, String port, String user, String password, String databaseName) {
        this.address = address;
        this.port = port;
        this.url = getConnectionString(address, port, databaseName);
        this.user = user;
        this.password = password;
        this.databaseName = databaseName;
    }

    private static String getConnectionString(String address, String port, String databaseName) {
        return "jdbc:postgresql://" + address + (port != null && !port.isEmpty() ? ":" + port : ":" + "5432") + "/" + databaseName;
    }
}
