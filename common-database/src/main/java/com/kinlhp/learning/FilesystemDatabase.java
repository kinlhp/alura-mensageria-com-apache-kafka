package com.kinlhp.learning;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class FilesystemDatabase {
    private final Connection connection;

    public FilesystemDatabase(final String databaseName) {
        final var jdbcUrl = "jdbc:sqlite:target/" + databaseName + ".db";
        try {
            this.connection = DriverManager.getConnection(jdbcUrl);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    // yes, this is way too generic.
    // according to your database tool, avoid sql injection.
    public boolean createTable(final String sql) {
        System.out.printf("Executing SQL: %s%n", sql);
        try {
            return connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public boolean execute(final String sql, final String... params) throws SQLException {
        return prepare(sql, params).execute();
    }

    public ResultSet executeQuery(final String sql, final String... params) throws SQLException {
        return prepare(sql, params).executeQuery();
    }

    private PreparedStatement prepare(final String sql, final String... params) throws SQLException {
        final var preparedStatement = connection.prepareStatement(sql);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setString(i + 1, params[i]);
        }
        return preparedStatement;
    }
}
