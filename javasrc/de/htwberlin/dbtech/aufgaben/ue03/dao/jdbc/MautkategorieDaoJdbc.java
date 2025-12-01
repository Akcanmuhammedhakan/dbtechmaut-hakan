package de.htwberlin.dbtech.aufgaben.ue03.dao.jdbc;

import de.htwberlin.dbtech.aufgaben.ue03.dao.IMautkategorieDao;
import de.htwberlin.dbtech.aufgaben.ue03.dao.rows.KategorieRow;
import de.htwberlin.dbtech.exceptions.DataException;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MautkategorieDaoJdbc implements IMautkategorieDao {
    private Connection connection;
    @Override public void setConnection(Connection connection) { this.connection = connection; }

    @Override
    public KategorieRow findBySsklAndAchsRule(int ssklId, String achsRule) {
        if (connection == null) throw new DataException("Connection not set");
        String sql = "SELECT KATEGORIE_ID, MAUTSATZ_JE_KM, ACHSZAHL FROM MAUTKATEGORIE WHERE SSKL_ID = ? AND ACHSZAHL = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, ssklId);
            ps.setString(2, achsRule);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    KategorieRow r = new KategorieRow();
                    r.kategorieId = rs.getInt(1);
                    r.satzCentProKm = rs.getDouble(2);
                    r.achsRegel = rs.getString(3);
                    return r;
                }
            }
        } catch (SQLException e) { throw new DataException(e); }
        return null;
    }

    @Override
    public String findAchsRuleByKategorieId(int kategorieId) {
        if (connection == null) throw new DataException("Connection not set");
        String sql = "SELECT ACHSZAHL FROM MAUTKATEGORIE WHERE KATEGORIE_ID = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, kategorieId);
            try (ResultSet rs = ps.executeQuery()) { return rs.next() ? rs.getString(1) : null; }
        } catch (SQLException e) { throw new DataException(e); }
    }

    @Override
    public List<KategorieRow> findAllBySskl(int ssklId) {
        if (connection == null) throw new DataException("Connection not set");
        String sql = "SELECT KATEGORIE_ID, MAUTSATZ_JE_KM, ACHSZAHL FROM MAUTKATEGORIE WHERE SSKL_ID = ? ORDER BY KATEGORIE_ID";
        List<KategorieRow> list = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, ssklId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    KategorieRow r = new KategorieRow();
                    r.kategorieId = rs.getInt(1);
                    r.satzCentProKm = rs.getDouble(2);
                    r.achsRegel = rs.getString(3);
                    list.add(r);
                }
            }
        } catch (SQLException e) { throw new DataException(e); }
        return list;
    }
}
