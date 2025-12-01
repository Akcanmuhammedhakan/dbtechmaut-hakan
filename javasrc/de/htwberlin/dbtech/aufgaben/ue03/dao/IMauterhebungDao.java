package de.htwberlin.dbtech.aufgaben.ue03.dao;

import java.math.BigDecimal;
import java.sql.Connection;

public interface IMauterhebungDao {
    void setConnection(Connection connection);
    long nextMautId();
    void insert(long mautId, int abschnittsId, long fzgId, int kategorieId, BigDecimal kostenEuro);
}

