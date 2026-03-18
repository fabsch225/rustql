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
        int lockRetryMaxRetries = parseIntProperty(
            info,
            "lockRetryMaxRetries",
            RustqlConnection.DEFAULT_LOCK_RETRY_MAX_RETRIES,
            0
        );
        int lockRetryInitialBackoffMs = parseIntProperty(
            info,
            "lockRetryInitialBackoffMs",
            RustqlConnection.DEFAULT_LOCK_RETRY_INITIAL_BACKOFF_MS,
            1
        );
        int lockRetryMaxBackoffMs = parseIntProperty(
            info,
            "lockRetryMaxBackoffMs",
            RustqlConnection.DEFAULT_LOCK_RETRY_MAX_BACKOFF_MS,
            1
        );

        return new RustqlConnection(
            endpoint.host,
            endpoint.port,
            timeoutMs,
            lockRetryMaxRetries,
            lockRetryInitialBackoffMs,
            lockRetryMaxBackoffMs
        );
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        DriverPropertyInfo timeout = new DriverPropertyInfo("timeoutMs", info.getProperty("timeoutMs", "5000"));
        timeout.description = "Socket connect/read timeout in milliseconds";
        DriverPropertyInfo retryCount = new DriverPropertyInfo(
            "lockRetryMaxRetries",
            info.getProperty("lockRetryMaxRetries", String.valueOf(RustqlConnection.DEFAULT_LOCK_RETRY_MAX_RETRIES))
        );
        retryCount.description = "Number of retries after ExceptionTableLocked";

        DriverPropertyInfo retryInitialBackoff = new DriverPropertyInfo(
            "lockRetryInitialBackoffMs",
            info.getProperty(
                "lockRetryInitialBackoffMs",
                String.valueOf(RustqlConnection.DEFAULT_LOCK_RETRY_INITIAL_BACKOFF_MS)
            )
        );
        retryInitialBackoff.description = "Initial lock-conflict retry backoff in milliseconds";

        DriverPropertyInfo retryMaxBackoff = new DriverPropertyInfo(
            "lockRetryMaxBackoffMs",
            info.getProperty(
                "lockRetryMaxBackoffMs",
                String.valueOf(RustqlConnection.DEFAULT_LOCK_RETRY_MAX_BACKOFF_MS)
            )
        );
        retryMaxBackoff.description = "Maximum lock-conflict retry backoff in milliseconds";

        return new DriverPropertyInfo[]{timeout, retryCount, retryInitialBackoff, retryMaxBackoff};
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
        return parseIntProperty(info, "timeoutMs", 5000, 1);
    }

    private static int parseIntProperty(Properties info, String key, int defaultValue, int minValue) {
        String raw = info.getProperty(key, String.valueOf(defaultValue));
        try {
            int parsed = Integer.parseInt(raw);
            return Math.max(minValue, parsed);
        } catch (NumberFormatException ignored) {
            return defaultValue;
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
