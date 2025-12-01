package de.htwberlin.dbtech.aufgaben.ue03.dao.jdbc;

import de.htwberlin.dbtech.aufgaben.ue03.dao.IMautabschnittDao;
import de.htwberlin.dbtech.exceptions.DataException;

import java.sql.*;

public class MautabschnittDaoJdbc implements IMautabschnittDao {
    private Connection connection;
    @Override public void setConnection(Connection connection) { this.connection = connection; }

    @Override
    public int getLaenge(int abschnittId) {
        if (connection == null) throw new DataException("Connection not set");
        String sql = "SELECT LAENGE FROM MAUTABSCHNITT WHERE ABSCHNITTS_ID = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, abschnittId);
            try (ResultSet rs = ps.executeQuery()) { return rs.next() ? rs.getInt(1) : 0; }
        } catch (SQLException e) { throw new DataException(e); }
    }
}

