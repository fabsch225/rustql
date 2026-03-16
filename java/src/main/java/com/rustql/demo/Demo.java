package com.rustql.demo;

import com.rustql.jdbc.RustqlDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Demo {
    public static void main(String[] args) throws Exception {
        Class.forName(RustqlDriver.class.getName());

        String url = "jdbc:rustql://127.0.0.1:5544";

        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement()) {

            statement.execute("CREATE TABLE Users (id Integer, name Varchar(25), place Varchar(25))");
            for (int i = 1; i <= 10000; i++) {
                statement.execute(
                    String.format("INSERT INTO Users (id, name, place) VALUES (%d, 'User%d', 'City%d')", i, i, i)
                );
            }

            try (ResultSet rs = statement.executeQuery("SELECT id, name, place FROM Users")) {
                while (rs.next()) {
                    int id = rs.getInt(1);
                    String name = rs.getString(2);
                    String place = rs.getString(3);
                    System.out.printf("id=%d, name=%s, place=%s%n", id, name, place);
                }
            }
        }
    }
}
