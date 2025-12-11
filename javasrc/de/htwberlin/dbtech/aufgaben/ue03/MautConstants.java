package de.htwberlin.dbtech.aufgaben.ue03;

import java.math.RoundingMode;

public final class MautConstants {
    private MautConstants() {}

    // Statusnamen in der DB
    public static final String STATUS_OFFEN = "offen";
    public static final String STATUS_ABGESCHLOSSEN = "abgeschlossen";

    // Achsregel-Format
    public static final String ACHS_RULE_GE_5 = ">= 5";
    public static final String ACHS_RULE_EQ_PREFIX = "= ";

    // Währungs-/Berechnungs-Parameter
    public static final int CENT_PER_EURO = 100;
    public static final int CURRENCY_SCALE = 2;
    public static final RoundingMode CURRENCY_ROUNDING = RoundingMode.HALF_UP;

    // Fehlermeldungen / Message keys
    public static final String ERR_NO_CATEGORY = "Keine Kategorie gefunden";
}
