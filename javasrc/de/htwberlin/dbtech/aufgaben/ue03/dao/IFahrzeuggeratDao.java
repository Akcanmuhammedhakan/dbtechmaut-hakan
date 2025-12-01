package de.htwberlin.dbtech.aufgaben.ue03.dao;

import java.sql.Connection;

public interface IFahrzeuggeratDao {
    void setConnection(Connection connection);
    Long findActiveFzgIdByFzId(Long fzId);
}

