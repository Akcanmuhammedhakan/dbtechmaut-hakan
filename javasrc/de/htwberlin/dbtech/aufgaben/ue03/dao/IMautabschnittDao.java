package de.htwberlin.dbtech.aufgaben.ue03.dao;

import java.sql.Connection;

public interface IMautabschnittDao {
    void setConnection(Connection connection);
    int getLaenge(int abschnittId);
}

