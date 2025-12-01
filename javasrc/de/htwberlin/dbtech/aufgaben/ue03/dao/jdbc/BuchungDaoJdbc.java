package de.htwberlin.dbtech.aufgaben.ue03.dao.jdbc;

import de.htwberlin.dbtech.aufgaben.ue03.dao.IBuchungDao;
import de.htwberlin.dbtech.aufgaben.ue03.dao.rows.BuchungRow;
import de.htwberlin.dbtech.exceptions.DataException;

import java.sql.*;

public class BuchungDaoJdbc implements IBuchungDao {
    private Connection connection;
    @Override public void setConnection(Connection connection) { this.connection = connection; }

    private int statusId(String name) {
        if (connection == null) throw new DataException("Connection not set");
        final String sql = "SELECT B_ID FROM BUCHUNGSTATUS WHERE LOWER(STATUS)=LOWER(?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        } catch (SQLException e) { throw new DataException(e); }
        throw new DataException("Buchungsstatus nicht gefunden: " + name);
    }

    @Override
    public boolean existsOffeneBuchungByKennzeichen(String kennzeichen) {
        int offen = statusId("offen");
        String sql = "SELECT 1 FROM BUCHUNG WHERE TRIM(KENNZEICHEN) = ? AND B_ID = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, kennzeichen == null ? null : kennzeichen.trim());
            ps.setInt(2, offen);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        } catch (SQLException e) { throw new DataException(e); }
    }

    @Override
    public BuchungRow findOffeneBuchungForAbschnitt(String kennzeichen, int abschnittId) {
        int offen = statusId("offen");
        String sql = "SELECT BUCHUNG_ID, KATEGORIE_ID FROM BUCHUNG WHERE TRIM(KENNZEICHEN) = ? AND ABSCHNITTS_ID = ? AND B_ID = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, kennzeichen == null ? null : kennzeichen.trim());
            ps.setInt(2, abschnittId);
            ps.setInt(3, offen);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    BuchungRow r = new BuchungRow();
                    r.buchungId = rs.getLong(1);
                    r.kategorieId = rs.getInt(2);
                    return r;
                }
            }
        } catch (SQLException e) { throw new DataException(e); }
        return null;
    }

    @Override
    public void finalizeBuchung(long buchungId) {
        int abgeschlossen = statusId("abgeschlossen");
        String sql = "UPDATE BUCHUNG SET B_ID = ?, BEFAHRUNGSDATUM = SYSDATE WHERE BUCHUNG_ID = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, abgeschlossen);
            ps.setLong(2, buchungId);
            ps.executeUpdate();
        } catch (SQLException e) { throw new DataException(e); }
    }
}
