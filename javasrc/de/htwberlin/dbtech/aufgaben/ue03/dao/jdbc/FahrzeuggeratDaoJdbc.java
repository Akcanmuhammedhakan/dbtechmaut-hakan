package de.htwberlin.dbtech.aufgaben.ue03.dao.jdbc;

import de.htwberlin.dbtech.aufgaben.ue03.dao.IFahrzeuggeratDao;
import de.htwberlin.dbtech.exceptions.DataException;

import java.sql.*;

public class FahrzeuggeratDaoJdbc implements IFahrzeuggeratDao {
    private Connection connection;
    @Override public void setConnection(Connection connection) { this.connection = connection; }

    @Override
    public Long findActiveFzgIdByFzId(Long fzId) {
        if (connection == null) throw new DataException("Connection not set");
        // Gerät gilt als aktiv, wenn es eingebaut ist (AUSBAUDATUM IS NULL)
        String sql = "SELECT FZG_ID FROM FAHRZEUGGERAT WHERE FZ_ID = ? AND AUSBAUDATUM IS NULL";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, fzId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong(1);
            }
        } catch (SQLException e) { throw new DataException(e); }
        return null;
    }
}
