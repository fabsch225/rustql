package com.rustql.jdbc;

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.ShardingKey;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

final class RustqlConnection implements Connection {
    static final int DEFAULT_LOCK_RETRY_MAX_RETRIES = 5;
    static final int DEFAULT_LOCK_RETRY_INITIAL_BACKOFF_MS = 50;
    static final int DEFAULT_LOCK_RETRY_MAX_BACKOFF_MS = 500;

    private final String host;
    private final int port;
    private final int timeoutMs;
    private final int lockRetryMaxRetries;
    private final int lockRetryInitialBackoffMs;
    private final int lockRetryMaxBackoffMs;
    private boolean closed;
    private RustqlProtocol.Session session;

    RustqlConnection(String host, int port, int timeoutMs) {
        this(
            host,
            port,
            timeoutMs,
            DEFAULT_LOCK_RETRY_MAX_RETRIES,
            DEFAULT_LOCK_RETRY_INITIAL_BACKOFF_MS,
            DEFAULT_LOCK_RETRY_MAX_BACKOFF_MS
        );
    }

    RustqlConnection(
        String host,
        int port,
        int timeoutMs,
        int lockRetryMaxRetries,
        int lockRetryInitialBackoffMs,
        int lockRetryMaxBackoffMs
    ) {
        this.host = host;
        this.port = port;
        this.timeoutMs = timeoutMs;
        this.lockRetryMaxRetries = Math.max(0, lockRetryMaxRetries);
        this.lockRetryInitialBackoffMs = Math.max(1, lockRetryInitialBackoffMs);
        this.lockRetryMaxBackoffMs = Math.max(this.lockRetryInitialBackoffMs, lockRetryMaxBackoffMs);
    }

    @Override
    public Statement createStatement() throws SQLException {
        ensureOpen();
        return new RustqlStatement(this);
    }

    String host() {
        return host;
    }

    int port() {
        return port;
    }

    int timeoutMs() {
        return timeoutMs;
    }

    int lockRetryMaxRetries() {
        return lockRetryMaxRetries;
    }

    int lockRetryInitialBackoffMs() {
        return lockRetryInitialBackoffMs;
    }

    int lockRetryMaxBackoffMs() {
        return lockRetryMaxBackoffMs;
    }

    @Override
    public void close() {
        if (session != null) {
            try {
                session.close();
            } catch (IOException ignored) {
            }
            session = null;
        }
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    void ensureOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }

    synchronized RustqlProtocol.QueryResponse execute(String sql, int fetchSize) throws SQLException {
        ensureOpen();
        if (session == null) {
            session = RustqlProtocol.openSession(host, port, timeoutMs);
        }
        return session.execute(sql, fetchSize);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (!autoCommit) {
            throw new SQLFeatureNotSupportedException("Transactions are not supported");
        }
    }

    @Override
    public boolean getAutoCommit() {
        return true;
    }

    @Override
    public void commit() {
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions are not supported");
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("Metadata not implemented");
    }

    @Override
    public boolean isValid(int timeout) {
        return !closed;
    }

    @Override
    public void clearWarnings() {
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public String nativeSQL(String sql) {
        return sql;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Prepared statements are not implemented");
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements are not implemented");
    }

    @Override
    public String getCatalog() {
        return null;
    }

    @Override
    public void setCatalog(String catalog) {
    }

    @Override
    public int getTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (level != Connection.TRANSACTION_NONE) {
            throw new SQLFeatureNotSupportedException("Transactions are not supported");
        }
    }

    @Override
    public Map<String, Class<?>> getTypeMap() {
        return Map.of();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type maps are not supported");
    }

    @Override
    public void setHoldability(int holdability) {
    }

    @Override
    public int getHoldability() {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createStatement();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
    }

    @Override
    public String getClientInfo(String name) {
        return null;
    }

    @Override
    public Properties getClientInfo() {
        return new Properties();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) {
    }

    @Override
    public String getSchema() {
        return null;
    }

    @Override
    public void abort(Executor executor) {
        closed = true;
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
    }

    @Override
    public int getNetworkTimeout() {
        return timeoutMs;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (readOnly) {
            throw new SQLFeatureNotSupportedException("Read-only mode is not supported");
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("No unwrap available for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void beginRequest() {
    }

    @Override
    public void endRequest() {
    }

    @Override
    public boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey, int timeout) {
        return false;
    }

    @Override
    public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) {
        return false;
    }

    @Override
    public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey) {
    }

    @Override
    public void setShardingKey(ShardingKey shardingKey) {
    }
}
