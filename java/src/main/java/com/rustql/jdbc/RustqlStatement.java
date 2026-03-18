package com.rustql.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

final class RustqlStatement implements Statement {
    private static final int DEFAULT_FETCH_SIZE = 256;

    private final RustqlConnection connection;
    private boolean closed;
    private ResultSet lastResultSet;
    private int updateCount = -1;
    private int fetchSize = DEFAULT_FETCH_SIZE;

    RustqlStatement(RustqlConnection connection) {
        this.connection = connection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        ensureOpen();
        RustqlProtocol.QueryResponse response = executeWithRetry(sql);

        this.lastResultSet = toCachedRowSet(response.columns, response.rows);
        this.updateCount = -1;
        return this.lastResultSet;
    }

    private RustqlProtocol.QueryResponse executeWithRetry(String sql) throws SQLException {
        int retries = 0;
        long backoffMs = connection.lockRetryInitialBackoffMs();

        while (true) {
            RustqlProtocol.QueryResponse response = connection.execute(sql, fetchSize);

            if (response.status == 0) {
                return response;
            }

            boolean retryable = response.message != null && response.message.contains("ExceptionTableLocked");
            if (!retryable || retries >= connection.lockRetryMaxRetries()) {
                throw new SQLException(response.message);
            }

            waitForRetry(backoffMs);
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted while waiting to retry lock-conflicted statement");
            }

            retries++;
            backoffMs = Math.min(backoffMs * 2L, connection.lockRetryMaxBackoffMs());
        }
    }

    private void waitForRetry(long backoffMs) {
        long nanos = Math.max(1L, backoffMs) * 1_000_000L;
        LockSupport.parkNanos(nanos);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        ResultSet rs = executeQuery(sql);
        return rs != null;
    }

    private CachedRowSet toCachedRowSet(List<RustqlProtocol.ColumnMeta> columns, List<Object[]> rows) throws SQLException {
        CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();

        RowSetMetaData metaData = new RowSetMetaDataImpl();
        metaData.setColumnCount(columns.size());

        for (int i = 0; i < columns.size(); i++) {
            RustqlProtocol.ColumnMeta c = columns.get(i);
            int jdbcType = RustqlProtocol.toJdbcType(c.typeTag);
            int idx = i + 1;
            metaData.setColumnName(idx, c.name);
            metaData.setColumnLabel(idx, c.name);
            metaData.setColumnType(idx, jdbcType);
            metaData.setNullable(idx, ResultSetMetaData.columnNullableUnknown);
            metaData.setPrecision(idx, 0);
            metaData.setScale(idx, 0);
        }

        rowSet.setMetaData((RowSetMetaDataImpl) metaData);

        for (Object[] row : rows) {
            rowSet.moveToCurrentRow();
            rowSet.last();
            rowSet.moveToInsertRow();
            for (int i = 0; i < row.length; i++) {
                rowSet.updateObject(i + 1, row[i]);
            }
            rowSet.insertRow();
        }

        rowSet.moveToCurrentRow();
        rowSet.beforeFirst();
        return rowSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        executeQuery(sql);
        this.updateCount = 0;
        return updateCount;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public int getMaxFieldSize() {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) {
    }

    @Override
    public int getMaxRows() {
        return 0;
    }

    @Override
    public void setMaxRows(int max) {
    }

    @Override
    public void setEscapeProcessing(boolean enable) {
    }

    @Override
    public int getQueryTimeout() {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) {
    }

    @Override
    public void cancel() {
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
    }

    @Override
    public void setCursorName(String name) {
    }

    @Override
    public ResultSet getResultSet() {
        return lastResultSet;
    }

    @Override
    public int getUpdateCount() {
        return updateCount;
    }

    @Override
    public boolean getMoreResults() {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) {
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows < 0) {
            throw new SQLException("Fetch size must be >= 0");
        }
        fetchSize = rows == 0 ? DEFAULT_FETCH_SIZE : rows;
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch not supported");
    }

    @Override
    public void clearBatch() {
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch not supported");
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) {
    }

    @Override
    public boolean isPoolable() {
        return false;
    }

    @Override
    public void closeOnCompletion() {
    }

    @Override
    public boolean isCloseOnCompletion() {
        return true;
    }

    @Override
    public long getLargeUpdateCount() {
        return updateCount;
    }

    @Override
    public void setLargeMaxRows(long max) {
    }

    @Override
    public long getLargeMaxRows() {
        return 0;
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql, columnIndexes);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql, columnNames);
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

    private void ensureOpen() throws SQLException {
        connection.ensureOpen();
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }
}
