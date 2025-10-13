package de.htwberlin.dbtech.aufgaben.ue02;

public class Mautabschnitt {

	/** die id des abschnitts **/
	private int abschnitts_id;

	/** die laenge in Metern des abschnittes **/
	private int laenge;

	/**
	 * die Koordinate wo der Abschnitt geopraphisch anf�ngt - Latitude and
	 * Longitude
	 **/
	private String start_koordinate;

	/**
	 * die Koordinate wo der Abschnitt geopraphisch endet - Latitude and
	 * Longitude
	 **/
	private String ziel_koordinate;

	/** der Names des Abschnitts mit Hinweis auf den Ort von - nach **/
	private String name;

	/** die Typenbezeichnung des Abschnitt z.B. A 10 **/
	private String abschnittstyp;

	/** initialisiert ein Objekt der Klasse **/
	public Mautabschnitt(int abschnitts_id, int laenge,
			String start_koordinate, String ziel_koordinate, String name,
			String abschnittstyp) {

		this.abschnitts_id = abschnitts_id;

		this.laenge = laenge;

		this.start_koordinate = start_koordinate;

		this.ziel_koordinate = ziel_koordinate;

		this.name = name;

		this.abschnittstyp = abschnittstyp;

	}

	public Mautabschnitt() {
		// TODO Auto-generated constructor stub
	}

	/** liefert die Id des Abschnitts **/
	public int getAbschnitts_id() {
		return abschnitts_id;
	}

	/** setzte die Id des Abschnitts **/
	public void setAbschnitts_id(int abschnitts_id) {
		this.abschnitts_id = abschnitts_id;
	}

	/** liefert die Laenge in Meteren f�r den Mautabschnitt **/
	public int getLaenge() {
		return laenge;
	}

	/*** setzt die Laenge in Metern f�r den Mautabschnitt */
	public void setLaenge(int laenge) {
		this.laenge = laenge;
	}

	/** liefert die geographische Start-Koordinate - Latitude and Longitude **/
	public String getStart_koordinate() {
		return start_koordinate;
	}

	/** setzt die geographische Start-Koordinate - Latitude and Longitude **/
	public void setStart_koordinate(String start_koordinate) {
		this.start_koordinate = start_koordinate;
	}

	/** liefert die geographische Ziel-Koordinate - Latitude and Longitude **/
	public String getZiel_koordinate() {
		return ziel_koordinate;
	}

	/** setzt die geographische Ziel-Koordinate - Latitude and Longitude **/
	public void setZiel_koordinate(String ziel_koordinate) {
		this.ziel_koordinate = ziel_koordinate;
	}

	/** liefert den Namen des Mautabschnitts mit Hinweise von-Ort nach-Ort **/
	public String getName() {
		return name;
	}

	/** setzt den Namen des Mautabschnitts mit Hinweise von-Ort nach-Ort **/
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * liefert den Abschnittstyp des Mautabschnitts, z.B. A10 (Autobahn) oder
	 * B13 (Bundestrasse)
	 **/
	public String getAbschnittstyp() {
		return abschnittstyp;
	}

	/**
	 * setzt den Abschnittstyp des Mautabschnitts, z.B. A10 (Autobahn) oder B13
	 * (Bundestrasse)
	 **/
	public void setAbschnittstyp(String abschnittstyp) {
		this.abschnittstyp = abschnittstyp;
	}

}

/test