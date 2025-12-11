package de.htwberlin.dbtech.aufgaben.ue03;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.dbtech.exceptions.DataException;
import de.htwberlin.dbtech.exceptions.AlreadyCruisedException;
import de.htwberlin.dbtech.exceptions.InvalidVehicleDataException;
import de.htwberlin.dbtech.exceptions.UnkownVehicleException;

/**
 * Die Klasse realisiert den AusleiheService.
 * 
 * @author Patrick Dohmeier
 */
public class MautServiceImpl implements IMautService {

    private static final Logger L = LoggerFactory.getLogger(MautServiceImpl.class);
    private Connection connection;

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    private Connection getConnection() {
        if (connection == null) {
            throw new DataException("Connection not set");
        }
        return connection;
    }

    // --- Hilfstypen & Hilfsmethoden (keine Hardcodings) ---
    private static final class Kategorie {
        final int id;
        final double satzCentProKm;
        final String ruleText; // z.B. "= 3", ">= 5"
        final Achsregel rule;
        Kategorie(int id, double satzCentProKm, String ruleText) {
            this.id = id;
            this.satzCentProKm = satzCentProKm;
            this.ruleText = ruleText;
            this.rule = Achsregel.parse(ruleText);
        }
    }

    private List<Kategorie> ladeKategorien(int ssklId) {
        final String sql = "SELECT KATEGORIE_ID, MAUTSATZ_JE_KM, ACHSZAHL FROM MAUTKATEGORIE WHERE SSKL_ID = ? ORDER BY KATEGORIE_ID";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setInt(1, ssklId);
            try (ResultSet rs = ps.executeQuery()) {
                List<Kategorie> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(new Kategorie(rs.getInt(1), rs.getDouble(2), rs.getString(3)));
                }
                return list;
            }
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }

    private int ladeBuchungsstatusId(String statusName) {
        final String sql = "SELECT B_ID FROM BUCHUNGSTATUS WHERE LOWER(STATUS) = LOWER(?)";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, statusName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new DataException(e);
        }
        throw new DataException("Unbekannter Buchungsstatus: " + statusName);
    }

    @Override
    public void berechneMaut(int mautAbschnitt, int achszahl, String kennzeichen)
            throws UnkownVehicleException, InvalidVehicleDataException, AlreadyCruisedException {
        // 1) Fahrzeugdaten laden
        Long fzId = null; // Fahrzeug-ID
        Integer fzAchsen = null; // Achsen des Fahrzeugs (DB)
        Integer ssklId = null; // Schadstoffklasse
        boolean fahrzeugAktiv = false;
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT FZ_ID, SSKL_ID, ACHSEN, ABMELDEDATUM FROM FAHRZEUG WHERE TRIM(KENNZEICHEN) = ?")) {
            ps.setString(1, kennzeichen == null ? null : kennzeichen.trim());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    fzId = rs.getLong("FZ_ID");
                    ssklId = rs.getInt("SSKL_ID");
                    fzAchsen = rs.getInt("ACHSEN");
                    fahrzeugAktiv = (rs.getObject("ABMELDEDATUM") == null);
                }
            }
        } catch (SQLException e) {
            throw new DataException(e);
        }

        // 2) Prüfen, ob automatisches Verfahren möglich (aktives Fahrzeug + eingebautes Gerät)
        Long fzgId = null; // On-Board-Unit ID
        boolean automatik = false;
        if (fzId != null && fahrzeugAktiv) {
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "SELECT FZG_ID FROM FAHRZEUGGERAT WHERE FZ_ID = ? AND AUSBAUDATUM IS NULL")) {
                ps.setLong(1, fzId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        fzgId = rs.getLong(1);
                        automatik = true;
                    }
                }
            } catch (SQLException e) {
                throw new DataException(e);
            }
        }

        // 3) Prüfen, ob manuelles Verfahren (irgendeine offene Buchung fuer das Fahrzeug)
        boolean manuell = false;
        final int statusOffen = ladeBuchungsstatusId(MautConstants.STATUS_OFFEN);
        final int statusAbgeschlossen = ladeBuchungsstatusId(MautConstants.STATUS_ABGESCHLOSSEN);
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT 1 FROM BUCHUNG WHERE TRIM(KENNZEICHEN) = ? AND B_ID = ?")) {
            ps.setString(1, kennzeichen == null ? null : kennzeichen.trim());
            ps.setInt(2, statusOffen);
            try (ResultSet rs = ps.executeQuery()) {
                manuell = rs.next();
            }
        } catch (SQLException e) {
            throw new DataException(e);
        }

        // 4) Routing
        if (automatik) {
            // 4a) Achsprüfung (Automatik): generisch über Kategorien
            if (fzAchsen == null || ssklId == null) throw new UnkownVehicleException();
            List<Kategorie> kategorien = ladeKategorien(ssklId);
            Kategorie grpFahrzeug = null;
            for (Kategorie k : kategorien) {
                if (k.rule != null && k.rule.matches(fzAchsen)) { grpFahrzeug = k; break; }
            }
            Kategorie grpMessung = null;
            for (Kategorie k : kategorien) {
                if (k.rule != null && k.rule.matches(achszahl)) { grpMessung = k; break; }
            }
            if (grpFahrzeug == null || grpMessung == null ||
                    (grpFahrzeug.ruleText != null && !grpFahrzeug.ruleText.equals(grpMessung.ruleText))) {
                throw new InvalidVehicleDataException();
            }

            // 4b) Laenge (m -> km)
            int laengeM = 0;
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "SELECT LAENGE FROM MAUTABSCHNITT WHERE ABSCHNITTS_ID = ?")) {
                ps.setInt(1, mautAbschnitt);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) laengeM = rs.getInt(1);
                }
            } catch (SQLException e) { throw new DataException(e); }
            double km = laengeM / 1000.0d;

            // 4c) Kosten berechnen: Cents -> Euro, auf 2 Nachkommastellen runden
            BigDecimal kostenEuro = BigDecimal.valueOf(grpMessung.satzCentProKm)
                    .multiply(BigDecimal.valueOf(km))
                    .divide(BigDecimal.valueOf(MautConstants.CENT_PER_EURO), 10, RoundingMode.HALF_UP)
                    .setScale(MautConstants.CURRENCY_SCALE, MautConstants.CURRENCY_ROUNDING);

            // 4d) neue MAUTERHEBUNG anlegen mit neuer MAUT_ID
            long newMautId = 0L;
            try (PreparedStatement ps = getConnection().prepareStatement("SELECT NVL(MAX(MAUT_ID),0) FROM MAUTERHEBUNG");
                 ResultSet rs = ps.executeQuery()) {
                if (rs.next()) newMautId = rs.getLong(1) + 1L;
            } catch (SQLException e) { throw new DataException(e); }

            try (PreparedStatement ps = getConnection().prepareStatement(
                    "INSERT INTO MAUTERHEBUNG (MAUT_ID, ABSCHNITTS_ID, FZG_ID, KATEGORIE_ID, BEFAHRUNGSDATUM, KOSTEN) " +
                    "VALUES (?,?,?,?, SYSDATE, ?)") ) {
                ps.setLong(1, newMautId);
                ps.setInt(2, mautAbschnitt);
                ps.setLong(3, fzgId);
                ps.setInt(4, grpMessung.id);
                ps.setBigDecimal(5, kostenEuro);
                ps.executeUpdate();
            } catch (SQLException e) { throw new DataException(e); }
            return;
        }
        else if (manuell) {
            // 5) Manuelles Verfahren
            // 5a) offene Buchung fuer diesen Abschnitt finden
            Long buchungId = null;
            Integer bookingKatId = null;
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "SELECT BUCHUNG_ID, KATEGORIE_ID FROM BUCHUNG WHERE TRIM(KENNZEICHEN) = ? AND ABSCHNITTS_ID = ? AND B_ID = ?")) {
                ps.setString(1, kennzeichen == null ? null : kennzeichen.trim());
                ps.setInt(2, mautAbschnitt);
                ps.setInt(3, statusOffen);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        buchungId = rs.getLong(1);
                        bookingKatId = rs.getInt(2);
                    }
                }
            } catch (SQLException e) { throw new DataException(e); }

            if (buchungId == null) {
                // keine offene Buchung fuer den Abschnitt -> Doppelbefahrung
                throw new AlreadyCruisedException();
            }

            // 5b) Achsprüfung anhand gebuchter Kategorie (generisch)
            String bookedAchsRule = null; // z.B. "= 3" oder ">= 5"
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "SELECT ACHSZAHL FROM MAUTKATEGORIE WHERE KATEGORIE_ID = ?")) {
                ps.setInt(1, bookingKatId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) bookedAchsRule = rs.getString(1);
                }
            } catch (SQLException e) { throw new DataException(e); }

            Achsregel regel = Achsregel.parse(bookedAchsRule);
            if (regel == null || !regel.matches(achszahl)) throw new InvalidVehicleDataException();

            // 5c) Buchung abschliessen
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "UPDATE BUCHUNG SET B_ID = ?, BEFAHRUNGSDATUM = SYSDATE WHERE BUCHUNG_ID = ?")) {
                ps.setInt(1, statusAbgeschlossen);
                ps.setLong(2, buchungId);
                ps.executeUpdate();
            } catch (SQLException e) { throw new DataException(e); }
            return;
        }
        else {
            // 6) Unbekanntes Fahrzeug
            throw new UnkownVehicleException();
        }
    }
}
