package com.rustql.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public final class RustqlDriver implements Driver {
    static {
        try {
            DriverManager.registerDriver(new RustqlDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register RustqlDriver", e);
        }
    }

    static final String PREFIX = "jdbc:rustql://";

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        Endpoint endpoint = parseEndpoint(url);
        int timeoutMs = parseTimeout(info);
        return new RustqlConnection(endpoint.host, endpoint.port, timeoutMs);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        DriverPropertyInfo timeout = new DriverPropertyInfo("timeoutMs", info.getProperty("timeoutMs", "5000"));
        timeout.description = "Socket connect/read timeout in milliseconds";
        return new DriverPropertyInfo[]{timeout};
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("No parent logger");
    }

    private static int parseTimeout(Properties info) {
        String raw = info.getProperty("timeoutMs", "5000");
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException ignored) {
            return 5000;
        }
    }

    private static Endpoint parseEndpoint(String url) throws SQLException {
        String endpoint = url.substring(PREFIX.length());

        int slash = endpoint.indexOf('/');
        if (slash >= 0) {
            endpoint = endpoint.substring(0, slash);
        }

        String host;
        int port;

        int colon = endpoint.lastIndexOf(':');
        if (colon < 0) {
            host = endpoint;
            port = 5544;
        } else {
            host = endpoint.substring(0, colon);
            String p = endpoint.substring(colon + 1);
            if (p.isBlank()) {
                port = 5544;
            } else {
                try {
                    port = Integer.parseInt(p);
                } catch (NumberFormatException e) {
                    throw new SQLException("Invalid port in JDBC URL: " + url, e);
                }
            }
        }

        if (host.isBlank()) {
            host = "127.0.0.1";
        }

        return new Endpoint(host, port);
    }

    private record Endpoint(String host, int port) {
    }
}
