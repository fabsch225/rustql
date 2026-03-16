package com.rustql.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RustqlProtocolTest {

    @Test
    void mapsRustTypesToJdbcTypes() throws SQLException {
        assertEquals(Types.NULL, RustqlProtocol.toJdbcType(RustqlProtocol.TYPE_NULL));
        assertEquals(Types.INTEGER, RustqlProtocol.toJdbcType(RustqlProtocol.TYPE_INTEGER));
        assertEquals(Types.VARCHAR, RustqlProtocol.toJdbcType(RustqlProtocol.TYPE_STRING));
        assertEquals(Types.VARCHAR, RustqlProtocol.toJdbcType(RustqlProtocol.TYPE_VARCHAR));
        assertEquals(Types.DATE, RustqlProtocol.toJdbcType(RustqlProtocol.TYPE_DATE));
        assertEquals(Types.BOOLEAN, RustqlProtocol.toJdbcType(RustqlProtocol.TYPE_BOOLEAN));
    }

    @Test
    void unknownRustTypeFails() {
        assertThrows(SQLException.class, () -> RustqlProtocol.toJdbcType(999));
    }
}
