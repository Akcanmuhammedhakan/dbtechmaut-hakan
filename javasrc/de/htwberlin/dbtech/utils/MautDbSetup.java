package de.htwberlin.dbtech.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class MautDbSetup {
    private static final Logger L = LoggerFactory.getLogger(MautDbSetup.class);

    public static void main(String[] args) throws Exception {
        L.info("Starte Maut DB Setup (create.sql)");
        JdbcUtils.loadDriver(DbCred.driverClass);
        try (Connection c = DriverManager.getConnection(DbCred.url, DbCred.user, DbCred.password)) {
            c.setAutoCommit(false);
            // Optional: drop.sql ausführen, wenn vorhanden
            execSqlIfExists(c, "db/maut/drop.sql");
            // create.sql ausführen
            execSql(c, "db/maut/create.sql");
            c.commit();
        }
        L.info("Maut DB Setup abgeschlossen");
    }

    private static void execSqlIfExists(Connection c, String path) {
        try {
            if (Files.exists(Paths.get(path))) {
                execSql(c, path);
            } else {
                L.info("Datei nicht gefunden (uebersprungen): {}", path);
            }
        } catch (RuntimeException e) {
            // Drop-Fehler ignorieren (z. B. falls Tabellen noch nicht existieren)
            L.warn("Fehler beim Ausfuehren von {} (ignoriert): {}", path, e.getMessage());
        }
    }

    private static void execSql(Connection c, String path) {
        String content;
        try {
            content = Files.readString(Paths.get(path), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Kann SQL-Datei nicht lesen: " + path, e);
        }
        // Kommentare entfernen (Zeilen, die mit -- beginnen) und Blockkommentare rudimentär
        content = content.replaceAll("/\\*.*?\\*/", " ");
        String[] statements = content.split(";\\s*\n|;\\r?\n|;\\s*$");
        for (String raw : statements) {
            String sql = raw.trim();
            if (sql.isEmpty()) continue;
            // Zeilenkommentare entfernen
            StringBuilder sb = new StringBuilder();
            for (String line : sql.split("\n")) {
                String t = line.trim();
                if (t.startsWith("--") || t.startsWith("//")) continue;
                sb.append(line).append('\n');
            }
            sql = sb.toString().trim();
            if (sql.isEmpty()) continue;
            L.debug("SQL: {}", shorten(sql));
            try (Statement st = c.createStatement()) {
                st.execute(sql);
            } catch (SQLException e) {
                L.error("Fehler bei SQL: {}", sql, e);
                throw new RuntimeException(e);
            }
        }
    }

    private static String shorten(String s) {
        return s.length() > 120 ? s.substring(0, 117) + "..." : s;
    }
}

