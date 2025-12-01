package de.htwberlin.dbtech.aufgaben.ue03.dao;

import de.htwberlin.dbtech.aufgaben.ue03.dao.rows.BuchungRow;

import java.sql.Connection;

public interface IBuchungDao {
    void setConnection(Connection connection);
    boolean existsOffeneBuchungByKennzeichen(String kennzeichen);
    BuchungRow findOffeneBuchungForAbschnitt(String kennzeichen, int abschnittId);
    void finalizeBuchung(long buchungId);
}

