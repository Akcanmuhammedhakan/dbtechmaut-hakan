package de.htwberlin.dbtech.aufgaben.ue02;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.dbtech.exceptions.DataException;

public class MautVerwaltungImpl implements IMautVerwaltung {

    private static final Logger L = LoggerFactory.getLogger(MautVerwaltungImpl.class);

    // SQL-Konstanten
    private static final String SQL_GET_STATUS_OBU = "SELECT STATUS FROM FAHRZEUGGERAT WHERE FZG_ID = ?";
    private static final String SQL_GET_USERNUMBER =
            "SELECT f.NUTZER_ID " +
            "FROM MAUTERHEBUNG m " +
            "JOIN FAHRZEUGGERAT g ON m.FZG_ID = g.FZG_ID " +
            "JOIN FAHRZEUG f ON g.FZ_ID = f.FZ_ID " +
            "WHERE m.MAUT_ID = ?";
    private static final String SQL_REGISTER_VEHICLE =
            "INSERT INTO FAHRZEUG (FZ_ID, SSKL_ID, NUTZER_ID, KENNZEICHEN, FIN, ACHSEN, GEWICHT, ANMELDEDATUM, ZULASSUNGSLAND) " +
            "VALUES (?,?,?,?,?,?,?,SYSDATE,?)";
    private static final String SQL_UPDATE_OBU_STATUS = "UPDATE FAHRZEUGGERAT SET STATUS = ? WHERE FZG_ID = ?";
    private static final String SQL_DELETE_OBU = "DELETE FROM FAHRZEUGGERAT WHERE FZ_ID = ?";
    private static final String SQL_DELETE_VEHICLE = "DELETE FROM FAHRZEUG WHERE FZ_ID = ?";
    private static final String SQL_GET_TRACK_INFOS =
            "SELECT ABSCHNITTS_ID, LAENGE, START_KOORDINATE, ZIEL_KOORDINATE, NAME, ABSCHNITTSTYP " +
            "FROM MAUTABSCHNITT WHERE ABSCHNITTSTYP = ? ORDER BY ABSCHNITTS_ID";

    private Connection connection;

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    private Connection getConnection() {
        if (connection == null) {
            throw new DataException("Connection not set");
        }
        return connection;
    }

    @Override
    public String getStatusForOnBoardUnit(long fzg_id) {
        try (PreparedStatement ps = getConnection().prepareStatement(SQL_GET_STATUS_OBU)) {
            ps.setLong(1, fzg_id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) {
            L.error("Fehler bei getStatusForOnBoardUnit(fzg_id={})", fzg_id, e);
            throw new DataException(e);
        }
    }

    @Override
    public int getUsernumber(int maut_id) {
        try (PreparedStatement ps = getConnection().prepareStatement(SQL_GET_USERNUMBER)) {
            ps.setInt(1, maut_id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
                throw new DataException("Kein Nutzer zu MAUT_ID=" + maut_id + " gefunden");
            }
        } catch (SQLException e) {
            L.error("Fehler bei getUsernumber(maut_id={})", maut_id, e);
            throw new DataException(e);
        }
    }

    @Override
    public void registerVehicle(long fz_id, int sskl_id, int nutzer_id,
                                String kennzeichen, String fin, int achsen,
                                int gewicht, String zulassungsland) {
        try (PreparedStatement ps = getConnection().prepareStatement(SQL_REGISTER_VEHICLE)) {
            ps.setLong(1, fz_id);
            ps.setInt(2, sskl_id);
            ps.setInt(3, nutzer_id);
            ps.setString(4, kennzeichen);
            ps.setString(5, fin);
            ps.setInt(6, achsen);
            ps.setInt(7, gewicht);
            ps.setString(8, zulassungsland);
            ps.executeUpdate();
        } catch (SQLException e) {
            L.error("Fehler bei registerVehicle(fz_id={})", fz_id, e);
            throw new DataException(e);
        }
    }

    @Override
    public void updateStatusForOnBoardUnit(long fzg_id, String status) {
        try (PreparedStatement ps = getConnection().prepareStatement(SQL_UPDATE_OBU_STATUS)) {
            ps.setString(1, status);
            ps.setLong(2, fzg_id);
            ps.executeUpdate();
        } catch (SQLException e) {
            L.error("Fehler bei updateStatusForOnBoardUnit(fzg_id={}, status={})", fzg_id, status, e);
            throw new DataException(e);
        }
    }

    @Override
    public void deleteVehicle(long fz_id) {

        try (PreparedStatement p1 = getConnection().prepareStatement(SQL_DELETE_OBU);
             PreparedStatement p2 = getConnection().prepareStatement(SQL_DELETE_VEHICLE)) {
            p1.setLong(1, fz_id);
            p1.executeUpdate();
            p2.setLong(1, fz_id);
            p2.executeUpdate();
        } catch (SQLException e) {
            L.error("Fehler bei deleteVehicle(fz_id={})", fz_id, e);
            throw new DataException(e);
        }
    }

    @Override
    public List<Mautabschnitt> getTrackInformations(String abschnittstyp) {
        List<Mautabschnitt> liste = new ArrayList<>();
        try (PreparedStatement ps = getConnection().prepareStatement(SQL_GET_TRACK_INFOS)) {
            ps.setString(1, abschnittstyp);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Mautabschnitt m = new Mautabschnitt();
                    m.setAbschnitts_id(rs.getInt("ABSCHNITTS_ID"));
                    double laengeDb = rs.getDouble("LAENGE");
                    m.setLaenge((int) Math.round(laengeDb));
                    m.setStart_koordinate(rs.getString("START_KOORDINATE"));
                    m.setZiel_koordinate(rs.getString("ZIEL_KOORDINATE"));
                    m.setName(rs.getString("NAME"));
                    m.setAbschnittstyp(rs.getString("ABSCHNITTSTYP"));
                    liste.add(m);
                }
            }
            return liste;
        } catch (SQLException e) {
            L.error("Fehler bei getTrackInformations(abschnittstyp={})", abschnittstyp, e);
            throw new DataException(e);
        }
    }
}