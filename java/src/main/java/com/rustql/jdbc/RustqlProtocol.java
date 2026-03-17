package com.rustql.jdbc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

final class RustqlProtocol {
    private static final byte[] MAGIC = new byte[]{'R', 'S', 'Q', 'L'};
    private static final byte VERSION = 2;

    static final int TYPE_NULL = 0;
    static final int TYPE_INTEGER = 1;
    static final int TYPE_STRING = 2;
    static final int TYPE_VARCHAR = 3;
    static final int TYPE_DATE = 4;
    static final int TYPE_BOOLEAN = 5;

    private RustqlProtocol() {
    }

    static Session openSession(String host, int port, int timeoutMs) throws SQLException {
        try {
            return new Session(host, port, timeoutMs);
        } catch (IOException e) {
            throw new SQLException("RustQL network error", e);
        }
    }

    static QueryResponse execute(String host, int port, int timeoutMs, String sql, int fetchSize) throws SQLException {
        try (Session session = openSession(host, port, timeoutMs)) {
            return session.execute(sql, fetchSize);
        } catch (IOException e) {
            throw new SQLException("RustQL network error", e);
        }
    }

    static final class Session implements Closeable {
        private final Socket socket;
        private final DataOutputStream out;
        private final DataInputStream in;

        Session(String host, int port, int timeoutMs) throws IOException {
            this.socket = new Socket();
            this.socket.connect(new InetSocketAddress(host, port), timeoutMs);
            this.socket.setSoTimeout(timeoutMs);
            this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        }

        QueryResponse execute(String sql, int fetchSize) throws SQLException {
            try {
                writeRequest(out, sql, fetchSize);
                out.flush();
                return readResponse(in);
            } catch (IOException e) {
                throw new SQLException("RustQL network error", e);
            }
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private static void writeRequest(DataOutputStream out, String sql, int fetchSize) throws IOException {
        byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
        out.write(MAGIC);
        out.writeByte(VERSION);
        out.writeInt(sqlBytes.length);
        out.write(sqlBytes);
        out.writeInt(Math.max(fetchSize, 0));
    }

    private static QueryResponse readResponse(DataInputStream in) throws IOException, SQLException {
        byte[] magic = in.readNBytes(4);
        if (magic.length != 4 || magic[0] != MAGIC[0] || magic[1] != MAGIC[1] || magic[2] != MAGIC[2] || magic[3] != MAGIC[3]) {
            throw new SQLException("Invalid RustQL response magic");
        }

        int status = in.readUnsignedByte();
        String message = readString(in);

        int columnCount = in.readUnsignedShort();
        List<ColumnMeta> columns = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            String name = readStringU16(in);
            int typeTag = in.readUnsignedByte();
            int typeArg = in.readInt();
            columns.add(new ColumnMeta(name, typeTag, typeArg));
        }

        List<Object[]> rows = new ArrayList<>();
        while (true) {
            int chunkRowCount = in.readInt();
            if (chunkRowCount < 0) {
                throw new SQLException("Invalid chunk row count");
            }

            for (int r = 0; r < chunkRowCount; r++) {
                int rowLen = in.readInt();
                if (rowLen < 0) {
                    throw new SQLException("Invalid row length");
                }
                byte[] row = in.readNBytes(rowLen);
                if (row.length != rowLen) {
                    throw new EOFException("Unexpected EOF while reading row");
                }
                rows.add(decodeRow(row, columns));
            }

            int done = in.readUnsignedByte();
            if (done == 1) {
                break;
            }
            if (done != 0) {
                throw new SQLException("Invalid chunk done marker");
            }
        }

        return new QueryResponse(status, message, columns, rows);
    }

    private static String readString(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0) {
            throw new IOException("Negative string length");
        }
        byte[] bytes = in.readNBytes(len);
        if (bytes.length != len) {
            throw new EOFException("Unexpected EOF while reading string");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String readStringU16(DataInputStream in) throws IOException {
        int len = in.readUnsignedShort();
        byte[] bytes = in.readNBytes(len);
        if (bytes.length != len) {
            throw new EOFException("Unexpected EOF while reading string");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static Object[] decodeRow(byte[] row, List<ColumnMeta> columns) throws SQLException {
        Object[] result = new Object[columns.size()];
        int offset = 0;

        for (int i = 0; i < columns.size(); i++) {
            ColumnMeta c = columns.get(i);
            int size = rustFieldSize(c.typeTag, c.typeArg);
            if (offset + size > row.length) {
                throw new SQLException("Row shorter than expected for column " + c.name);
            }
            result[i] = decodeCell(row, offset, c.typeTag, c.typeArg);
            offset += size;
        }

        return result;
    }

    private static int rustFieldSize(int typeTag, int typeArg) throws SQLException {
        return switch (typeTag) {
            case TYPE_NULL -> 1;
            case TYPE_INTEGER -> 5;
            case TYPE_STRING -> 256;
            case TYPE_VARCHAR -> typeArg + 1;
            case TYPE_DATE -> 5;
            case TYPE_BOOLEAN -> 1;
            default -> throw new SQLException("Unknown RustQL type tag: " + typeTag);
        };
    }

    private static Object decodeCell(byte[] row, int offset, int typeTag, int typeArg) throws SQLException {
        return switch (typeTag) {
            case TYPE_NULL -> null;
            case TYPE_INTEGER -> decodeInteger(row, offset);
            case TYPE_STRING -> decodeString(row, offset, 256);
            case TYPE_VARCHAR -> decodeString(row, offset, typeArg + 1);
            case TYPE_DATE -> decodeDate(row, offset);
            case TYPE_BOOLEAN -> (row[offset] & 1) != 0;
            default -> throw new SQLException("Unknown RustQL type tag: " + typeTag);
        };
    }

    private static Integer decodeInteger(byte[] row, int offset) {
        int b1 = row[offset + 1] & 0xFF;
        int b2 = row[offset + 2] & 0xFF;
        int b3 = row[offset + 3] & 0xFF;
        int b4 = row[offset + 4] & 0xFF;
        return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
    }

    private static String decodeString(byte[] row, int offset, int len) {
        int payload = Math.max(0, len - 1);
        int end = offset;
        while (end < offset + payload && row[end] != 0) {
            end++;
        }
        return new String(row, offset, end - offset, StandardCharsets.UTF_8);
    }

    private static Date decodeDate(byte[] row, int offset) {
        int year = ((row[offset] & 0xFF) << 8) | (row[offset + 1] & 0xFF);
        int month = row[offset + 2] & 0xFF;
        int day = row[offset + 3] & 0xFF;
        return Date.valueOf(LocalDate.of(year, month, day));
    }

    static int toJdbcType(int rustType) throws SQLException {
        return switch (rustType) {
            case TYPE_NULL -> Types.NULL;
            case TYPE_INTEGER -> Types.INTEGER;
            case TYPE_STRING, TYPE_VARCHAR -> Types.VARCHAR;
            case TYPE_DATE -> Types.DATE;
            case TYPE_BOOLEAN -> Types.BOOLEAN;
            default -> throw new SQLException("Unknown RustQL type tag: " + rustType);
        };
    }

    static final class ColumnMeta {
        final String name;
        final int typeTag;
        final int typeArg;

        ColumnMeta(String name, int typeTag, int typeArg) {
            this.name = name;
            this.typeTag = typeTag;
            this.typeArg = typeArg;
        }
    }

    static final class QueryResponse {
        final int status;
        final String message;
        final List<ColumnMeta> columns;
        final List<Object[]> rows;

        QueryResponse(int status, String message, List<ColumnMeta> columns, List<Object[]> rows) {
            this.status = status;
            this.message = message;
            this.columns = columns;
            this.rows = rows;
        }
    }
}
