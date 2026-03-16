package com.rustql.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RustqlStatementTest {

    @Test
    void executeQueryFailsWhenStatementClosed() throws SQLException {
        RustqlConnection connection = new RustqlConnection("127.0.0.1", 5544, 100);
        try (RustqlStatement statement = new RustqlStatement(connection)) {
            statement.close();
            assertThrows(SQLException.class, () -> statement.executeQuery("SELECT 1"));
        }
    }

    @Test
    void executeQueryFailsWhenConnectionClosed() throws SQLException {
        RustqlConnection connection = new RustqlConnection("127.0.0.1", 5544, 100);
        try (RustqlStatement statement = new RustqlStatement(connection)) {
            connection.close();
            assertThrows(SQLException.class, () -> statement.executeQuery("SELECT 1"));
        }
    }

    @Test
    void fetchSizeCanBeConfigured() throws SQLException {
        RustqlConnection connection = new RustqlConnection("127.0.0.1", 5544, 100);
        try (RustqlStatement statement = new RustqlStatement(connection)) {
            statement.setFetchSize(3);
            assertEquals(3, statement.getFetchSize());

            statement.setFetchSize(0);
            assertEquals(256, statement.getFetchSize());
        }
    }

    @Test
    void negativeFetchSizeIsRejected() throws SQLException {
        RustqlConnection connection = new RustqlConnection("127.0.0.1", 5544, 100);
        try (RustqlStatement statement = new RustqlStatement(connection)) {
            assertThrows(SQLException.class, () -> statement.setFetchSize(-1));
        }
    }
}
