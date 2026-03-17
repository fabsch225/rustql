package com.rustql.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalTime;
import java.util.concurrent.CountDownLatch;

import com.rustql.jdbc.RustqlDriver;

public class Demo {
    private static final String URL = "jdbc:rustql://127.0.0.1:5544";

    public static void main(String[] args) throws Exception {
        Class.forName(RustqlDriver.class.getName());

        long runId = System.currentTimeMillis();
        String users = "users_" + runId;
        String orders = "orders_" + runId;

        log("Preparing tables: " + users + ", " + orders);
        try (Connection connection = DriverManager.getConnection(URL);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + users + " (id Integer, name Varchar(25))");
            statement.execute("CREATE TABLE " + orders + " (id Integer, note Varchar(25))");
        }

        demoDisjointTableConcurrency(users, orders);
        demoConflictingTableLock(users);
        verifyFinalData(users, orders);
    }

    private static void demoDisjointTableConcurrency(String users, String orders) throws Exception {
        log("=== Scenario 1: Concurrent access on disjoint tables (should succeed) ===");

        CountDownLatch tx1Locked = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {
            try (Connection c1 = DriverManager.getConnection(URL);
                 Statement s1 = c1.createStatement()) {
                log("T1 BEGIN on " + users);
                s1.execute("BEGIN TRANSACTION");
                s1.execute("INSERT INTO " + users + " (id, name) VALUES (1, 'alice')");
                log("T1 inserted into " + users + " and keeps tx open for 2s");
                tx1Locked.countDown();
                Thread.sleep(2000);
                s1.execute("COMMIT");
                log("T1 COMMIT complete");
            } catch (Exception e) {
                log("T1 ERROR: " + e.getMessage());
            } finally {
                done.countDown();
            }
        }, "tx-users");

        Thread t2 = new Thread(() -> {
            try (Connection c2 = DriverManager.getConnection(URL);
                 Statement s2 = c2.createStatement()) {
                tx1Locked.await();
                log("T2 tries write on disjoint table " + orders + " while T1 tx is open");
                s2.execute("INSERT INTO " + orders + " (id, note) VALUES (100, 'ok-disjoint')");
                log("T2 SUCCESS on disjoint table (concurrency works)");
            } catch (Exception e) {
                log("T2 ERROR (unexpected for disjoint tables): " + e.getMessage());
            } finally {
                done.countDown();
            }
        }, "tx-orders");

        t1.start();
        t2.start();
        done.await();
    }

    private static void demoConflictingTableLock(String users) throws Exception {
        log("=== Scenario 2: Conflicting access on same table (should block/fail until commit) ===");

        CountDownLatch tx1Locked = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {
            try (Connection c1 = DriverManager.getConnection(URL);
                 Statement s1 = c1.createStatement()) {
                log("T1 BEGIN on " + users);
                s1.execute("BEGIN TRANSACTION");
                s1.execute("INSERT INTO " + users + " (id, name) VALUES (2, 'bob')");
                log("T1 inserted id=2 and keeps tx open for 2s");
                tx1Locked.countDown();
                Thread.sleep(2000);
                s1.execute("COMMIT");
                log("T1 COMMIT complete");
            } catch (Exception e) {
                log("T1 ERROR: " + e.getMessage());
            } finally {
                done.countDown();
            }
        }, "tx-conflict-owner");

        Thread t2 = new Thread(() -> {
            try (Connection c2 = DriverManager.getConnection(URL);
                 Statement s2 = c2.createStatement()) {
                tx1Locked.await();
                log("T2 tries write on SAME table while T1 holds tx lock");
                try {
                    s2.execute("INSERT INTO " + users + " (id, name) VALUES (3, 'charlie')");
                    log("T2 unexpected SUCCESS while lock held");
                } catch (SQLException expected) {
                    log("T2 expected lock conflict: " + expected.getMessage());
                }

                Thread.sleep(2300);
                s2.execute("INSERT INTO " + users + " (id, name) VALUES (3, 'charlie')");
                log("T2 retry after T1 COMMIT succeeded");
            } catch (Exception e) {
                log("T2 ERROR: " + e.getMessage());
            } finally {
                done.countDown();
            }
        }, "tx-conflict-waiter");

        t1.start();
        t2.start();
        done.await();
    }

    private static void verifyFinalData(String users, String orders) throws Exception {
        log("=== Final verification ===");
        try (Connection connection = DriverManager.getConnection(URL);
             Statement statement = connection.createStatement()) {

            try (ResultSet rs = statement.executeQuery("SELECT id, name FROM " + users)) {
                while (rs.next()) {
                    log("users row => id=" + rs.getInt(1) + ", name=" + rs.getString(2));
                }
            }

            try (ResultSet rs = statement.executeQuery("SELECT id, note FROM " + orders)) {
                while (rs.next()) {
                    log("orders row => id=" + rs.getInt(1) + ", note=" + rs.getString(2));
                }
            }
        }
    }

    private static void log(String msg) {
        System.out.printf("[%s] [%s] %s%n", LocalTime.now(), Thread.currentThread().getName(), msg);
    }
}
