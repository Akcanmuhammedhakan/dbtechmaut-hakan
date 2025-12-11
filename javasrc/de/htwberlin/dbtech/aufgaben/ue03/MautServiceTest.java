package de.htwberlin.dbtech.aufgaben.ue02;

import de.htwberlin.dbtech.exceptions.DataException;
import de.htwberlin.dbtech.utils.DbCred;
import de.htwberlin.dbtech.utils.DbUnitUtils;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.dataset.csv.CsvDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

/**
 * Die Klasse enthaelt die Testfaelle fuer die Methoden des Mautservice /
 * Mauterhebung.
 *
 * @author Patrick Dohmeier
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MautServiceTest {
    private static final Logger L = LoggerFactory.getLogger(MautServiceTest.class);
    private static IDatabaseConnection dbTesterCon = null;

    private static final IMautVerwaltung maut = new MautVerwaltungImpl();

    @BeforeClass
    public static void setUp() {
        L.debug("setup: start");
        try {
            IDatabaseTester dbTester = new JdbcDatabaseTester(DbCred.driverClass, DbCred.url, DbCred.user, DbCred.password,
                    DbCred.schema);
            dbTesterCon = dbTester.getConnection();
            IDataSet pre = new CsvDataSet(new File("test-data/ue02"));
            dbTester.setDataSet(pre);
            DatabaseOperation.CLEAN_INSERT.execute(dbTesterCon, pre);
            maut.setConnection(dbTesterCon.getConnection());
        } catch (Exception e) {
            DbUnitUtils.closeDbUnitConnectionQuietly(dbTesterCon);
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void tearDown() {
        L.debug("tearDown: start");
        DbUnitUtils.closeDbUnitConnectionQuietly(dbTesterCon);
    }

    /**
     * Der Testfall testet im Erfolgsfall, ob der Status des Fahrzeuggerätes
     * (On-Board-Unit) korrekt zurückgegeben wird.
     */
    @org.junit.Test
    public void testMautverwaltung_1() {
        // Der Status des Fahrzeuggerätes mit der ID 5456921154 soll ermittelt
        // und zurückgegeben werden.
        String status = maut.getStatusForOnBoardUnit(5456921154L);
        // Vergleiche zu erwartendes Ergebnis mit dem tatsächlichen Ergebnis
        Assert.assertEquals("Der Status des Fahrzeuggeraetes stimmt nicht mit dem erwarteten Status ueberein!", "inactive",
                status);
    }

    /**
     * Der Testfall testet im Erfolgsfall, ob die Rückgabe der richtigen
     * Nutzernummer zu einer Mauterhebung im Automatischen Verfahren korrekt ist
     */
    @org.junit.Test
    public void testMautverwaltung_2() {
        // Im Automatischen Verfahren wurde bereits eine Mauterhebung durch ein
        // Fahrzeuggerät durchgeführt. Für die Mauterhebung mit der Maut_ID 1015
        // soll nun die Nutzernummer ermittelt werden.
        int nutzer_id = maut.getUsernumber(1015);
        // Vergleiche zu erwartendes Ergebnis mit dem tatsächlichen Ergebnis
        Assert.assertEquals("Die Nutzernummer stimmt nicht mit der erwarteten ueberein!", 1000002, nutzer_id);
    }

    /**
     * Der Testfall testet im Erfolgsfall das Laden von Mautabschnitten aus der
     * Datenbank
     */
    @org.junit.Test
    public void testMautverwaltung_3() {
        // Abschnittyp B13, also die Bundesstrasse 13 besitzt eine bestimmte
        // Anzahl von Mautabschnittsobjekten, die geladen werden sollen.
        List<Mautabschnitt> m = maut.getTrackInformations("B13");
        Assert.assertEquals("Die Anzahl der geladenen Mautabschnittsobjekt stimmt nicht ueberein!", 3, m.size());
    }

    /**
     * Der Testfall testet im Erfolgfall, ob ein Fahrzeug korrekt in die Tabelle
     * Fahrzeug eingefügt und gespeichert worden ist.
     */
    @org.junit.Test
    public void testMautverwaltung_4() throws Exception {
        // Das Fahrzeug mit dem Kennzeichen 935 DGG aus Spanien soll in die
        // Tabelle Fahrzeug hinzugefügt werden. Gleichzeitig muss auch ein
        // Anmeldedatum (Systemdatum) gesetzt werden.
        maut.registerVehicle(100441556794622L, 3, 1000009, "935 GDD", "WD60554718", 5, 12500, "Spanien");
        int actual = countRecords(dbTesterCon.getConnection(), "FAHRZEUG", "fz_id = 100441556794622");

        Assert.assertEquals("Fahrzeug 100441556794622 ist nicht in der Datenbank!", 1, actual);
    }

    /**
     * Der Testfall testet im Erfolgsfall das Löschen eines Fahrzeugs aus der Datenbank
     */
    @org.junit.Test
    public void testMautverwaltung_5() throws Exception {
        // Das Fahrzeug mir der ID 100441556794623 soll aus der Datenbank gelöscht werden.
        maut.deleteVehicle(100441556794623L);
        Connection c = dbTesterCon.getConnection();
        int actual = countRecords(dbTesterCon.getConnection(), "FAHRZEUG", "fz_id = 100441556794623");

        Assert.assertEquals("Fahrzeug 100441556794623 ist noch in der Datenbank!", 0, actual);
    }

    /**
     * Der Testfall testet im Erfolgsfall, ob der Status eines Fahrzeuggerätes
     * (On-Board-Unit) korrekt geändert worden ist.
     */
    @org.junit.Test
    public void testMautverwaltung_6() {
        // Das Fahrzeuggerät mit der ID soll einen anderen Status bekommen. In
        // diesem Fall soll der Status auf inactive gesetzt werden.
        maut.updateStatusForOnBoardUnit(1696502191, "inactive");
        // Vergleiche zu erwartendes Ergebnis mit dem tatsächlichen Ergebnis aus
        // der DB
        Assert.assertEquals("Der Status des Fahrzeuggerätes stimmt nicht mit dem erwarteten Status ueberein!", "inactive",
                maut.getStatusForOnBoardUnit(1696502191));
    }

    /**
     * Hilfsfunktion zum Zählen von Datensätzen
     */
    int countRecords(Connection c, String tableName, String condition) {
        String sql = "select count(*) as anzahl from " + tableName + " where " + condition;
        L.debug("countRecords:" + sql);
        try (Statement stmt = c.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {
                rs.next();
                return rs.getInt("anzahl");
            }
        } catch (SQLException e) {
            L.error("", e);
            throw new DataException(e);
        }
    }

}
