package com.rustql.jdbc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RustqlIntegrationTest {
    private static final String HOST = "127.0.0.1";
    private static int PORT;
    private static Process serverProcess;
    private static Thread logDrainer;
    private static String dbPath;

    @BeforeAll
    static void startServer() throws Exception {
        Class.forName(RustqlDriver.class.getName());

        Path rustProjectRoot = Path.of("..").toAbsolutePath().normalize();
        File rootDir = rustProjectRoot.toFile();

        PORT = findFreePort();
        dbPath = "./server-it-" + System.nanoTime() + ".db.bin";
        String bindAddr = HOST + ":" + PORT;

        serverProcess = new ProcessBuilder(
            "cargo",
            "run",
            "--example",
            "tcp_server",
            "--",
            bindAddr,
            dbPath
        )
            .directory(rootDir)
            .redirectErrorStream(true)
            .start();

        logDrainer = new Thread(() -> drain(serverProcess.getInputStream()), "rustql-server-log-drainer");
        logDrainer.setDaemon(true);
        logDrainer.start();

        waitForPort(HOST, PORT, Duration.ofSeconds(20));
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (serverProcess != null) {
            serverProcess.destroy();
            if (!serverProcess.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                serverProcess.destroyForcibly();
            }
        }

        if (dbPath != null) {
            try {
                java.nio.file.Files.deleteIfExists(Path.of(dbPath));
            } catch (IOException ignored) {
            }
        }
    }

    @Test
    void roundTripCreateInsertSelectWorks() throws SQLException {
        String table = "it_users_" + System.nanoTime();

        try (Connection connection = DriverManager.getConnection(jdbcUrl());
             Statement statement = connection.createStatement()) {
            statement.setFetchSize(1);

            statement.execute("CREATE TABLE " + table + " (id Integer, name Varchar(25), place Varchar(25))");
            statement.execute("INSERT INTO " + table + " (id, name, place) VALUES (1, 'Ada', 'Berlin')");
            statement.execute("INSERT INTO " + table + " (id, name, place) VALUES (2, 'Linus', 'Helsinki')");

            try (ResultSet rs = statement.executeQuery("SELECT id, name, place FROM " + table)) {
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertEquals(2, count);
            }
        }
    }

    @Test
    void invalidQueryReturnsSQLException() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl());
             Statement statement = connection.createStatement()) {
            assertThrows(SQLException.class, () -> statement.executeQuery("SELECT FROM"));
        }
    }

    @Test
    void roundTripAllDatatypesWorks() throws SQLException {
        String table = "it_types_" + System.nanoTime();

        try (Connection connection = DriverManager.getConnection(jdbcUrl());
             Statement statement = connection.createStatement()) {
            statement.setFetchSize(1);

            statement.execute(
                "CREATE TABLE " + table +
                    " (id Integer, name String, nick Varchar(10), born Date, active Boolean)"
            );

            statement.execute(
                "INSERT INTO " + table +
                    " (id, name, nick, born, active) VALUES (42, 'Ada Lovelace', 'Ada', '1815-12-10', true)"
            );
            statement.execute(
                "INSERT INTO " + table +
                    " (id, name, nick, born, active) VALUES (7, 'Alan Turing', 'Al', '1912-06-23', false)"
            );

            try (ResultSet rs = statement.executeQuery(
                "SELECT id, name, nick, born, active FROM " + table + " WHERE id = 42"
            )) {
                assertTrue(rs.next());

                assertEquals(42, rs.getInt("id"));
                assertEquals("Ada Lovelace", rs.getString("name"));
                assertEquals("Ada", rs.getString("nick"));
                assertEquals(Date.valueOf("1815-12-10"), rs.getDate("born"));
                assertTrue(rs.getBoolean("active"));

                ResultSetMetaData meta = rs.getMetaData();
                assertEquals(java.sql.Types.INTEGER, meta.getColumnType(1));
                assertEquals(java.sql.Types.VARCHAR, meta.getColumnType(2));
                assertEquals(java.sql.Types.VARCHAR, meta.getColumnType(3));
                assertEquals(java.sql.Types.DATE, meta.getColumnType(4));
                assertEquals(java.sql.Types.BOOLEAN, meta.getColumnType(5));

                assertFalse(rs.next());
            }
        }
    }

    @Test
    void concurrentReadsAcrossConnectionsWork() throws Exception {
        String table = "it_concurrent_reads_" + System.nanoTime();
        final int rowCount = 24000;
        final int workers = 8;
        final int roundsPerWorker = 12;
        final int fetchSize = 32;

        try (Connection setupConnection = DriverManager.getConnection(jdbcUrl());
             Statement setupStatement = setupConnection.createStatement()) {
            setupStatement.execute("CREATE TABLE " + table + " (id Integer, name Varchar(25), place Varchar(25))");

            for (int i = 1; i <= rowCount; i++) {
                setupStatement.execute(
                    "INSERT INTO " + table + " (id, name, place) VALUES (" +
                        i + ", 'name_" + i + "', 'city_" + (i % 7) + "')"
                );
            }
        }

        ExecutorService executor = Executors.newFixedThreadPool(workers);
        CyclicBarrier startBarrier = new CyclicBarrier(workers);
        List<Callable<Integer>> tasks = new ArrayList<>();

        long baselineStart = System.nanoTime();
        int baselineObservedRows = runReadRounds(table, roundsPerWorker, rowCount, fetchSize);
        long baselineNanosPerWorker = System.nanoTime() - baselineStart;
        assertEquals(rowCount * roundsPerWorker, baselineObservedRows);

        for (int worker = 0; worker < workers; worker++) {
            tasks.add(() -> {
                startBarrier.await(5, TimeUnit.SECONDS);

                int observedRows = 0;
                observedRows += runReadRounds(table, roundsPerWorker, rowCount, fetchSize);
                return observedRows;
            });
        }

        try {
            long concurrentStart = System.nanoTime();
            List<Future<Integer>> futures = executor.invokeAll(tasks, 30, TimeUnit.SECONDS);
            long concurrentNanos = System.nanoTime() - concurrentStart;
            int expectedPerWorker = rowCount * roundsPerWorker;

            for (Future<Integer> future : futures) {
                assertTrue(future.isDone(), "concurrent read worker did not finish in time");
                assertFalse(future.isCancelled(), "concurrent read worker was cancelled");
                assertEquals(expectedPerWorker, future.get(1, TimeUnit.SECONDS));
            }

            long serializedEstimate = baselineNanosPerWorker * workers;
            assertTrue(
                concurrentNanos < (long) (serializedEstimate * 0.75),
                "reads look serialized: concurrent=" + concurrentNanos + "ns, serialized_estimate=" + serializedEstimate + "ns"
            );
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "worker threads did not terminate");
        }
    }

    private static int runReadRounds(String table, int rounds, int expectedRowsPerRound, int fetchSize) throws SQLException {
        int observedRows = 0;
        try (Connection connection = DriverManager.getConnection(jdbcUrl());
             Statement statement = connection.createStatement()) {
            statement.setFetchSize(fetchSize);
            for (int round = 0; round < rounds; round++) {
                int localCount = 0;
                try (ResultSet rs = statement.executeQuery("SELECT id, name, place FROM " + table)) {
                    while (rs.next()) {
                        localCount++;
                    }
                }
                if (localCount != expectedRowsPerRound) {
                    throw new AssertionError("expected " + expectedRowsPerRound + " rows, got " + localCount);
                }
                observedRows += localCount;
            }
        }
        return observedRows;
    }

    private static String jdbcUrl() {
        return "jdbc:rustql://" + HOST + ":" + PORT;
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void waitForPort(String host, int port, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (serverProcess != null && !serverProcess.isAlive()) {
                throw new IllegalStateException("RustQL test server exited before opening port");
            }
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 250);
                return;
            } catch (IOException ignored) {
                Thread.sleep(150);
            }
        }
        assertTrue(false, "RustQL test server did not open port " + port + " within " + timeout);
    }

    private static void drain(InputStream inputStream) {
        try (InputStream in = inputStream) {
            byte[] buffer = new byte[4096];
            while (in.read(buffer) != -1) {
                // discard output
            }
        } catch (IOException ignored) {
        }
    }
}
