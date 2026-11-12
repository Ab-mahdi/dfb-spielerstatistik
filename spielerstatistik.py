"""
Spielerstatistik für den DFB: eine Zeile pro Spieler.
Ablauf: lesen, rechnen, prüfen, speichern. Start: python spielerstatistik.py

Ich baue eine Auswahl nach drei Mustern: Zählen, Durchschnitt, Summe.
Die anderen Kennzahlen gehen genauso.
"""

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# --- Einstellungen -----------------------------------------------------------

# True: lokal mit CSV und Parquet. False: Oracle lesen, nach Hive schreiben.
LOCAL = True

# ANNAHME: Ich setze die Saison von Hand. Zahlungen haben keine Saison, also gilt das Datum.
SAISON_START = "2026-07-01"
SAISON_ENDE = "2027-06-30"

# ANNAHME: Alles außer "A" sind Junioren. Ein Spiel ohne Kategorie zähle ich nirgends.
KATEGORIE_A = "A"

# ANNAHME: Ich zähle nur DFB-Prämien, bezahlt oder offen. Storniert fällt raus.
TYP_PRAEMIE = "Praemie_Nationalmannschaft"
STATUS_OFFEN = "offen"
GEZAEHLTE_STATUS = ["bezahlt", STATUS_OFFEN]

ORDNER = os.path.dirname(os.path.abspath(__file__))
CSV_ORDNER = os.path.join(ORDNER, "data", "csv_quelle") if LOCAL else os.environ.get("DFB_CSV_PFAD", "hdfs:///daten/dfb/csv")
ORACLE_EXPORT_ORDNER = os.path.join(ORDNER, "data", "oracle_export")
AUSGABE_ORDNER = os.path.join(ORDNER, "output", "spielerstatistik")
HIVE_TABELLE = "dfb.spielerstatistik"

ZIEL_SPALTEN = [
    "Spieler_ID", "Anzahl_Spiele", "Anzahl_LS_A", "Anzahl_LS_Junioren",
    "AVG_Bewertung_5Spiele", "AVG_Bewertung_Gesamt", "SUM_Praemie_Saison", "ausstehende_Praemien",
]

# Keine Zahlung heißt 0 Euro. Keine Bewertung heißt aber nicht Note 0, der Durchschnitt bleibt leer.
LEER_HEISST_0 = ["Anzahl_Spiele", "Anzahl_LS_A", "Anzahl_LS_Junioren", "SUM_Praemie_Saison", "ausstehende_Praemien"]

# Ich gebe die Typen vor, statt Spark raten zu lassen. Geld braucht Decimal.
SCHEMA_SPIELER = """
    Spieler_ID INT, Name STRING, Staatsangehoerigkeit STRING, Telefonnummer STRING,
    Wohnort STRING, Verein STRING, Geburtsdatum DATE, Alter INT
"""
SCHEMA_ZAHLUNGEN = """
    TX_ID STRING, Spieler_ID INT, Sender STRING, Empfaenger STRING, Zahlungsname STRING,
    Zahlungstyp STRING, Zahlungsdatum DATE, Zahlungsbetrag DECIMAL(18,2), Zahlungswaehrung STRING,
    Zahlungsabwicklung STRING, Zahlungsstatus STRING
"""
SCHEMA_SPIEL = """
    Spiel_ID STRING, Spieldatum DATE, Saison STRING, Spielort STRING, Spielland STRING,
    Mannschaft1 STRING, Mannschaft2 STRING, Mannschaft1_Kapitaen INT, Mannschaft2_Kapitaen INT,
    Ergebnis STRING, Zuschauerzahl INT, Zuschauereinnahmen_EUR DECIMAL(18,2),
    Sponsoreneinnahmen_EUR DECIMAL(18,2), isLaenderspiel BOOLEAN, Nationalmannschaftkategorie STRING
"""
SCHEMA_BEWERTUNG = """
    Spiel_ID STRING, Spieler_ID INT, Bewerter STRING, Bewertung_in_Prozent DOUBLE, Kommentar STRING
"""


# --- Daten lesen -------------------------------------------------------------

def lese_csv(spark, pfad, schema):
    return spark.read.option("header", True).schema(schema).csv(pfad)


def lese_oracle(spark, tabelle, schema, spalten):
    """Liest lokal die CSV-Kopie, sonst Oracle per JDBC. Gibt nur die gewünschten Spalten zurück."""
    if LOCAL:
        df = lese_csv(spark, os.path.join(ORACLE_EXPORT_ORDNER, tabelle.lower() + ".csv"), schema)
    else:
        # Die Zugangsdaten kommen aus Umgebungsvariablen.
        df = (
            spark.read.format("jdbc")
            .option("url", os.environ["ORACLE_URL"])
            .option("user", os.environ["ORACLE_USER"])
            .option("password", os.environ["ORACLE_PASSWORD"])
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("dbtable", "DFB." + tabelle)     # ANNAHME: Schema DFB
            .option("fetchsize", 10000)              # Oracle holt sonst nur 10 Zeilen pro Abruf
            .load()
        )
    # Oracle schreibt Namen groß. alias gibt ihnen meine Schreibweise.
    df = df.select([F.col(s).alias(s) for s in spalten])
    return df.withColumn("Spieler_ID", F.col("Spieler_ID").cast("int"))   # Oracle liefert NUMBER, also DECIMAL


def lese_quellen(spark):
    # Von den Spielern lade ich nur die ID. Telefonnummer und Wohnort brauche ich nicht.
    spieler = lese_oracle(spark, "SPIELER", SCHEMA_SPIELER, ["Spieler_ID"])
    zahlungen = lese_oracle(
        spark, "FINANZTRANSAKTIONEN", SCHEMA_ZAHLUNGEN,
        ["Spieler_ID", "Zahlungstyp", "Zahlungsdatum", "Zahlungsbetrag", "Zahlungsstatus"],
    )
    # ANNAHME: Die Tabelle Nationalmannschaft lasse ich weg. Die Kategorie steht im Spiel,
    # und mehrere Zeilen pro Spieler würden beim Join doppeln.
    spiel = lese_csv(spark, os.path.join(CSV_ORDNER, "spiel.csv"), SCHEMA_SPIEL)
    bewertung = lese_csv(spark, os.path.join(CSV_ORDNER, "spielerbewertung.csv"), SCHEMA_BEWERTUNG)
    return spieler, zahlungen, spiel, bewertung


# --- Kennzahlen berechnen ----------------------------------------------------
# Muster: groupBy("Spieler_ID").agg(FUNKTION(F.when(BEDINGUNG, SPALTE)))
# Passt die Bedingung nicht, liefert F.when NULL. count, avg und sum überspringen NULL.

def noten_pro_spiel(bewertung, spiel):
    """
    Schritt 1: eine Note pro Spieler und Spiel, gemittelt über die Bewerter.
    Sonst zählt ein Spiel mit drei Bewertern dreifach.
    """
    # ANNAHME: Wer eine Note hat, hat gespielt. Bewertungen ohne Note fliegen raus.
    mit_note = bewertung.filter(F.col("Bewertung_in_Prozent").isNotNull())
    spiel_infos = spiel.select("Spiel_ID", "Spieldatum", "isLaenderspiel", "Nationalmannschaftkategorie")
    return (
        mit_note.join(spiel_infos, "Spiel_ID")
        .groupBy("Spieler_ID", "Spiel_ID", "Spieldatum", "isLaenderspiel", "Nationalmannschaftkategorie")
        .agg(F.avg("Bewertung_in_Prozent").alias("Note_Spiel"))
    )


def anzahl_spiele(noten):
    """Zählt alle Spiele und die Länderspiele für A und Junioren. Höchstens eine Zeile pro Spieler."""
    ist_a = F.col("isLaenderspiel") & (F.col("Nationalmannschaftkategorie") == KATEGORIE_A)
    ist_junioren = F.col("isLaenderspiel") & (F.col("Nationalmannschaftkategorie") != KATEGORIE_A)

    return noten.groupBy("Spieler_ID").agg(
        F.countDistinct("Spiel_ID").alias("Anzahl_Spiele"),   # zeigt, auf wie vielen Spielen die Ø beruhen
        F.countDistinct(F.when(ist_a, F.col("Spiel_ID"))).alias("Anzahl_LS_A"),
        F.countDistinct(F.when(ist_junioren, F.col("Spiel_ID"))).alias("Anzahl_LS_Junioren"),
    )


def durchschnittsnoten(noten):
    """Schritt 2: Ø der letzten 5 Spielnoten und Ø gesamt. Höchstens eine Zeile pro Spieler."""
    # Das neueste Spiel bekommt Platz 1. row_number vergibt jeden Platz nur einmal.
    # ANNAHME: Am selben Tag gilt die höhere Spiel_ID als später. IDs müssen gleich lang sein (S09).
    reihenfolge = Window.partitionBy("Spieler_ID").orderBy(F.col("Spieldatum").desc(), F.col("Spiel_ID").desc())
    noten = noten.withColumn("platz", F.row_number().over(reihenfolge))

    return noten.groupBy("Spieler_ID").agg(
        F.avg(F.when(F.col("platz") <= 5, F.col("Note_Spiel"))).alias("AVG_Bewertung_5Spiele"),
        F.avg("Note_Spiel").alias("AVG_Bewertung_Gesamt"),   # zum Vergleich: ist die Form besser als sonst?
    )


def praemien(zahlungen):
    """Summiert DFB-Prämien der Saison und die offenen. Höchstens eine Zeile pro Spieler."""
    # ANNAHME: Alle Beträge sind Euro.
    zahlungen = zahlungen.filter((F.col("Zahlungstyp") == TYP_PRAEMIE) & F.col("Zahlungsstatus").isin(GEZAEHLTE_STATUS))
    # Oracle speichert DATE mit Uhrzeit. to_date schneidet sie ab, sonst fehlt der 30.06. nachmittags.
    in_saison = F.to_date("Zahlungsdatum").between(F.to_date(F.lit(SAISON_START)), F.to_date(F.lit(SAISON_ENDE)))
    ist_offen = F.col("Zahlungsstatus") == STATUS_OFFEN

    return zahlungen.groupBy("Spieler_ID").agg(
        F.sum(F.when(in_saison, F.col("Zahlungsbetrag"))).alias("SUM_Praemie_Saison"),
        F.sum(F.when(ist_offen, F.col("Zahlungsbetrag"))).alias("ausstehende_Praemien"),
    )


def zusammenfuehren(spieler, teil_tabellen):
    """
    Hängt alle Kennzahlen per LEFT JOIN an die Spieler. Eine Zeile pro Spieler.
    Jede Teil-Tabelle hat höchstens eine Zeile pro Spieler, also doppelt nichts.
    LEFT, damit Spieler ohne Bewertung oder Prämie drinbleiben.
    """
    ergebnis = spieler
    for teil in teil_tabellen:
        ergebnis = ergebnis.join(teil, "Spieler_ID", "left")
    return ergebnis.fillna(0, subset=LEER_HEISST_0).select(ZIEL_SPALTEN)


# --- Prüfen und speichern ----------------------------------------------------

def pruefe(ergebnis, spieler):
    anzahl_spieler = spieler.count()
    assert ergebnis.count() == anzahl_spieler, "Ergebnis hat nicht genau eine Zeile pro Spieler"
    assert ergebnis.select("Spieler_ID").distinct().count() == anzahl_spieler, "Spieler_ID doppelt"
    print(f"Prüfung OK: {anzahl_spieler} Spieler, keine Duplikate.")


def speichere(spark, ergebnis):
    # Jeder Lauf rechnet neu und überschreibt.
    if LOCAL:
        ergebnis.write.mode("overwrite").parquet(AUSGABE_ORDNER)
    else:
        spark.sql("CREATE DATABASE IF NOT EXISTS dfb")   # saveAsTable legt die Datenbank nicht an
        ergebnis.write.mode("overwrite").format("parquet").saveAsTable(HIVE_TABELLE)


def main():
    builder = SparkSession.builder.appName("DFB_Spielerstatistik")
    if LOCAL:
        builder = builder.master("local[*]").config("spark.sql.shuffle.partitions", "4")
    else:
        builder = builder.enableHiveSupport()
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spieler, zahlungen, spiel, bewertung = lese_quellen(spark)

    noten = noten_pro_spiel(bewertung, spiel)
    teil_tabellen = [anzahl_spiele(noten), durchschnittsnoten(noten), praemien(zahlungen)]
    ergebnis = zusammenfuehren(spieler, teil_tabellen).cache()   # Prüfen, Speichern, Anzeigen: nur einmal rechnen

    pruefe(ergebnis, spieler)
    speichere(spark, ergebnis)
    ergebnis.orderBy("Spieler_ID").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
