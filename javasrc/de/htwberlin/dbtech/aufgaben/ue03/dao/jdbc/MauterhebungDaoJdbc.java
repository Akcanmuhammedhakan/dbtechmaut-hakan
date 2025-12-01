package de.htwberlin.dbtech.aufgaben.ue03.dao.jdbc;

import de.htwberlin.dbtech.aufgaben.ue03.dao.IMauterhebungDao;
import de.htwberlin.dbtech.exceptions.DataException;

import java.math.BigDecimal;
import java.sql.*;

public class MauterhebungDaoJdbc implements IMauterhebungDao {
    private Connection connection;
    @Override public void setConnection(Connection connection) { this.connection = connection; }

    @Override
    public long nextMautId() {
        if (connection == null) throw new DataException("Connection not set");
        String sql = "SELECT NVL(MAX(MAUT_ID),0) FROM MAUTERHEBUNG";
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) + 1L : 1L;
        } catch (SQLException e) { throw new DataException(e); }
    }

    @Override
    public void insert(long mautId, int abschnittsId, long fzgId, int kategorieId, BigDecimal kostenEuro) {
        if (connection == null) throw new DataException("Connection not set");
        String sql = "INSERT INTO MAUTERHEBUNG (MAUT_ID, ABSCHNITTS_ID, FZG_ID, KATEGORIE_ID, BEFAHRUNGSDATUM, KOSTEN) VALUES (?,?,?,?, SYSDATE, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, mautId);
            ps.setInt(2, abschnittsId);
            ps.setLong(3, fzgId);
            ps.setInt(4, kategorieId);
            ps.setBigDecimal(5, kostenEuro);
            ps.executeUpdate();
        } catch (SQLException e) { throw new DataException(e); }
    }
}

