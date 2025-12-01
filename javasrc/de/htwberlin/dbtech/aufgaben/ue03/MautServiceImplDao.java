package de.htwberlin.dbtech.aufgaben.ue03;

import de.htwberlin.dbtech.aufgaben.ue03.dao.*;
import de.htwberlin.dbtech.aufgaben.ue03.dao.jdbc.*;
import de.htwberlin.dbtech.aufgaben.ue03.dao.rows.*;
import de.htwberlin.dbtech.exceptions.AlreadyCruisedException;
import de.htwberlin.dbtech.exceptions.DataException;
import de.htwberlin.dbtech.exceptions.InvalidVehicleDataException;
import de.htwberlin.dbtech.exceptions.UnkownVehicleException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;

public class MautServiceImplDao implements IMautService {

    private Connection connection;
    private IFahrzeugDao fahrzeugDao = new FahrzeugDaoJdbc();
    private IFahrzeuggeratDao fahrzeuggeratDao = new FahrzeuggeratDaoJdbc();
    private IBuchungDao buchungDao = new BuchungDaoJdbc();
    private IMautabschnittDao mautabschnittDao = new MautabschnittDaoJdbc();
    private IMautkategorieDao mautkategorieDao = new MautkategorieDaoJdbc();
    private IMauterhebungDao mauterhebungDao = new MauterhebungDaoJdbc();

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
        fahrzeugDao.setConnection(connection);
        fahrzeuggeratDao.setConnection(connection);
        buchungDao.setConnection(connection);
        mautabschnittDao.setConnection(connection);
        mautkategorieDao.setConnection(connection);
        mauterhebungDao.setConnection(connection);
    }

    @Override
    public void berechneMaut(int mautAbschnitt, int achszahl, String kennzeichen)
            throws UnkownVehicleException, InvalidVehicleDataException, AlreadyCruisedException {
        VehicleRow v = fahrzeugDao.findByKennzeichenTrim(kennzeichen);

        boolean automatik = false;
        Long fzgId = null;
        if (v != null && v.aktiv) {
            fzgId = fahrzeuggeratDao.findActiveFzgIdByFzId(v.fzId);
            automatik = (fzgId != null);
        }

        boolean manuell = buchungDao.existsOffeneBuchungByKennzeichen(kennzeichen);

        if (automatik) {
            // Achsprüfung Automatik
            boolean axlesOk = (v.achsen <= 4) ? (achszahl == v.achsen) : (achszahl >= 5);
            if (!axlesOk) throw new InvalidVehicleDataException();

            int laengeM = mautabschnittDao.getLaenge(mautAbschnitt);
            double km = laengeM / 1000.0d;
            String achsKey = (achszahl >= 5) ? ">= 5" : "= " + achszahl;
            KategorieRow kat = mautkategorieDao.findBySsklAndAchsRule(v.ssklId, achsKey);
            if (kat == null) throw new DataException("Keine Kategorie gefunden");
            BigDecimal kostenEuro = BigDecimal.valueOf(kat.satzCentProKm)
                    .multiply(BigDecimal.valueOf(km))
                    .divide(BigDecimal.valueOf(100), 10, RoundingMode.HALF_UP)
                    .setScale(2, RoundingMode.HALF_UP);
            long mautId = mauterhebungDao.nextMautId();
            mauterhebungDao.insert(mautId, mautAbschnitt, fzgId, kat.kategorieId, kostenEuro);
            return;
        } else if (manuell) {
            BuchungRow b = buchungDao.findOffeneBuchungForAbschnitt(kennzeichen, mautAbschnitt);
            if (b == null) throw new AlreadyCruisedException();
            String bookedAchsRule = mautkategorieDao.findAchsRuleByKategorieId(b.kategorieId);
            boolean axlesOk;
            if (">= 5".equals(bookedAchsRule)) axlesOk = (achszahl >= 5);
            else if (bookedAchsRule != null && bookedAchsRule.startsWith("= "))
                axlesOk = (achszahl == Integer.parseInt(bookedAchsRule.substring(2).trim()));
            else axlesOk = false;
            if (!axlesOk) throw new InvalidVehicleDataException();
            buchungDao.finalizeBuchung(b.buchungId);
            return;
        } else {
            throw new UnkownVehicleException();
        }
    }
}

