package com.rustql.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class RustqlDriverTest {

    @Test
    void acceptsValidJdbcUrlPrefix() {
        RustqlDriver driver = new RustqlDriver();
        assertTrue(driver.acceptsURL("jdbc:rustql://127.0.0.1:5544"));
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432/db"));
    }

    @Test
    void connectReturnsNullForUnsupportedUrl() throws SQLException {
        RustqlDriver driver = new RustqlDriver();
        Connection connection = driver.connect("jdbc:other://localhost", new Properties());
        assertNull(connection);
    }

    @Test
    void connectParsesEndpointAndTimeout() throws SQLException {
        RustqlDriver driver = new RustqlDriver();
        Properties properties = new Properties();
        properties.setProperty("timeoutMs", "1234");

        Connection connection = driver.connect("jdbc:rustql://db.local:7777", properties);
        assertNotNull(connection);

        RustqlConnection rustqlConnection = (RustqlConnection) connection;
        assertEquals("db.local", rustqlConnection.host());
        assertEquals(7777, rustqlConnection.port());
        assertEquals(1234, rustqlConnection.timeoutMs());
    }

    @Test
    void connectParsesRetryBackoffProperties() throws SQLException {
        RustqlDriver driver = new RustqlDriver();
        Properties properties = new Properties();
        properties.setProperty("lockRetryMaxRetries", "7");
        properties.setProperty("lockRetryInitialBackoffMs", "15");
        properties.setProperty("lockRetryMaxBackoffMs", "120");

        Connection connection = driver.connect("jdbc:rustql://db.local:7777", properties);
        assertNotNull(connection);

        RustqlConnection rustqlConnection = (RustqlConnection) connection;
        assertEquals(7, rustqlConnection.lockRetryMaxRetries());
        assertEquals(15, rustqlConnection.lockRetryInitialBackoffMs());
        assertEquals(120, rustqlConnection.lockRetryMaxBackoffMs());
    }

    @Test
    void connectUsesDefaultsForBlankHostAndPort() throws SQLException {
        RustqlDriver driver = new RustqlDriver();

        Connection connection = driver.connect("jdbc:rustql://:", new Properties());
        RustqlConnection rustqlConnection = (RustqlConnection) connection;

        assertEquals("127.0.0.1", rustqlConnection.host());
        assertEquals(5544, rustqlConnection.port());
    }

    @Test
    void connectFailsOnInvalidPort() {
        RustqlDriver driver = new RustqlDriver();

        assertThrows(SQLException.class,
            () -> driver.connect("jdbc:rustql://localhost:not-a-port", new Properties()));
    }
}
