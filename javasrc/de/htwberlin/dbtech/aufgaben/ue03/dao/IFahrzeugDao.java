package de.htwberlin.dbtech.aufgaben.ue03.dao;

import de.htwberlin.dbtech.aufgaben.ue03.dao.rows.VehicleRow;

import java.sql.Connection;

public interface IFahrzeugDao {
    void setConnection(Connection connection);
    VehicleRow findByKennzeichenTrim(String kennzeichen);
}

