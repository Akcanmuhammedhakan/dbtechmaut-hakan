package de.htwberlin.dbtech.aufgaben.ue03.dao.jdbc;

import de.htwberlin.dbtech.aufgaben.ue03.dao.IFahrzeugDao;
import de.htwberlin.dbtech.aufgaben.ue03.dao.rows.VehicleRow;
import de.htwberlin.dbtech.exceptions.DataException;

import java.sql.*;

public class FahrzeugDaoJdbc implements IFahrzeugDao {
    private Connection connection;
    @Override public void setConnection(Connection connection) { this.connection = connection; }

    @Override
    public VehicleRow findByKennzeichenTrim(String kennzeichen) {
        if (connection == null) throw new DataException("Connection not set");
        String sql = "SELECT FZ_ID, SSKL_ID, ACHSEN, ABMELDEDATUM FROM FAHRZEUG WHERE TRIM(KENNZEICHEN) = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, kennzeichen == null ? null : kennzeichen.trim());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    VehicleRow v = new VehicleRow();
                    v.fzId = rs.getLong("FZ_ID");
                    v.ssklId = rs.getInt("SSKL_ID");
                    v.achsen = rs.getInt("ACHSEN");
                    v.aktiv = (rs.getObject("ABMELDEDATUM") == null);
                    return v;
                }
            }
        } catch (SQLException e) { throw new DataException(e); }
        return null;
    }
}

