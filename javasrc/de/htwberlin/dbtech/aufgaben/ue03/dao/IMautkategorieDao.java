package de.htwberlin.dbtech.aufgaben.ue03.dao;

import de.htwberlin.dbtech.aufgaben.ue03.dao.rows.KategorieRow;

import java.sql.Connection;
import java.util.List;

public interface IMautkategorieDao {
    void setConnection(Connection connection);
    KategorieRow findBySsklAndAchsRule(int ssklId, String achsRule);
    String findAchsRuleByKategorieId(int kategorieId);
    List<KategorieRow> findAllBySskl(int ssklId);
}
