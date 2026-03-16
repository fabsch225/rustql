package com.rustql.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import static org.junit.jupiter.api.Assertions.*;

class RustqlConnectionTest {

    @Test
    void createStatementReturnsRustqlStatement() throws SQLException {
        RustqlConnection connection = new RustqlConnection("127.0.0.1", 5544, 5000);
        assertNotNull(connection.createStatement());
    }

    @Test
    void createStatementFailsWhenClosed() {
        RustqlConnection connection = new RustqlConnection("127.0.0.1", 5544, 5000);
        connection.close();

        assertThrows(SQLException.class, connection::createStatement);
    }

    @Test
    void disablingAutoCommitIsUnsupported() {
        RustqlConnection connection = new RustqlConnection("127.0.0.1", 5544, 5000);
        assertThrows(SQLFeatureNotSupportedException.class, () -> connection.setAutoCommit(false));
    }

    @Test
    void nonNoneTransactionIsolationIsUnsupported() {
        RustqlConnection connection = new RustqlConnection("127.0.0.1", 5544, 5000);
        assertThrows(SQLFeatureNotSupportedException.class,
            () -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
    }

    @Test
    void unwrapAndWrapperChecksWork() throws SQLException {
        RustqlConnection connection = new RustqlConnection("127.0.0.1", 5544, 5000);

        assertTrue(connection.isWrapperFor(RustqlConnection.class));
        assertSame(connection, connection.unwrap(RustqlConnection.class));
    }
}
