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
    private final IFahrzeugDao fahrzeugDao = new FahrzeugDaoJdbc();
    private final IFahrzeuggeratDao fahrzeuggeratDao = new FahrzeuggeratDaoJdbc();
    private final IBuchungDao buchungDao = new BuchungDaoJdbc();
    private final IMautabschnittDao mautabschnittDao = new MautabschnittDaoJdbc();
    private final IMautkategorieDao mautkategorieDao = new MautkategorieDaoJdbc();
    private final IMauterhebungDao mauterhebungDao = new MauterhebungDaoJdbc();

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
            // Achsprüfung Automatik (zentralisierte Logik)
            if (!validateAxlesForVehicle(achszahl, v)) throw new InvalidVehicleDataException();

            int laengeM = mautabschnittDao.getLaenge(mautAbschnitt);
            double km = laengeM / 1000.0d;
            String achsKey = buildAchsKeyFromNumber(achszahl);
            KategorieRow kat = mautkategorieDao.findBySsklAndAchsRule(v.ssklId, achsKey);
            if (kat == null) throw new DataException(MautConstants.ERR_NO_CATEGORY);
            BigDecimal kostenEuro = BigDecimal.valueOf(kat.satzCentProKm)
                    .multiply(BigDecimal.valueOf(km))
                    .divide(BigDecimal.valueOf(MautConstants.CENT_PER_EURO), 10, RoundingMode.HALF_UP)
                    .setScale(MautConstants.CURRENCY_SCALE, MautConstants.CURRENCY_ROUNDING);
            long mautId = mauterhebungDao.nextMautId();
            mauterhebungDao.insert(mautId, mautAbschnitt, fzgId, kat.kategorieId, kostenEuro);
        } else if (manuell) {
            BuchungRow b = buchungDao.findOffeneBuchungForAbschnitt(kennzeichen, mautAbschnitt);
            if (b == null) throw new AlreadyCruisedException();
            String bookedAchsRule = mautkategorieDao.findAchsRuleByKategorieId(b.kategorieId);
            if (!validateAxlesAgainstRule(achszahl, bookedAchsRule)) throw new InvalidVehicleDataException();
            buchungDao.finalizeBuchung(b.buchungId);
        } else {
            throw new UnkownVehicleException();
        }
    }

    // ...Hilfsmethoden zum Zentralisieren der Achs-Logik...
    private boolean validateAxlesForVehicle(int achszahl, VehicleRow v) {
        if (v == null) return false;
        // wenn Fahrzeug echte Achsanzahl <=4 dann exakte Übereinstimmung, ansonsten >=5
        return (v.achsen <= 4) ? (achszahl == v.achsen) : (achszahl >= 5);
    }

    private String buildAchsKeyFromNumber(int achszahl) {
        return (achszahl >= 5) ? MautConstants.ACHS_RULE_GE_5 : (MautConstants.ACHS_RULE_EQ_PREFIX + achszahl);
    }

    private boolean validateAxlesAgainstRule(int achszahl, String rule) {
        if (rule == null) return false;
        if (MautConstants.ACHS_RULE_GE_5.equals(rule)) return achszahl >= 5;
        if (rule.startsWith(MautConstants.ACHS_RULE_EQ_PREFIX)) {
            try {
                int expected = Integer.parseInt(rule.substring(MautConstants.ACHS_RULE_EQ_PREFIX.length()).trim());
                return achszahl == expected;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }

}
