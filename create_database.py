#!/usr/bin/env python3
"""Create DuckDB database from school JSON files (VWO only)."""

import json
import duckdb
from pathlib import Path


def create_schema(con):
    """Create database schema for VWO school data."""

    con.execute(
        """
        CREATE TABLE schools (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            address VARCHAR,
            postal_code VARCHAR,
            city VARCHAR,
            aantal_leerlingen INTEGER
        )
    """
    )

    con.execute(
        """
        CREATE TABLE doorstroom_onderbouw (
            school_id INTEGER NOT NULL,
            schooljaar VARCHAR NOT NULL,
            percentage DOUBLE,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """
    )

    con.execute(
        """
        CREATE TABLE doorstroom_bovenbouw (
            school_id INTEGER NOT NULL,
            schooljaar VARCHAR NOT NULL,
            percentage DOUBLE,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """
    )

    con.execute(
        """
        CREATE TABLE schooladvies (
            school_id INTEGER NOT NULL,
            positie VARCHAR NOT NULL,
            percentage DOUBLE,
            vergelijking DOUBLE,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """
    )

    con.execute(
        """
        CREATE TABLE slagingspercentage (
            school_id INTEGER NOT NULL,
            schooljaar VARCHAR NOT NULL,
            percentage DOUBLE,
            vergelijking DOUBLE,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """
    )

    con.execute(
        """
        CREATE TABLE examencijfers (
            school_id INTEGER NOT NULL,
            schooljaar VARCHAR NOT NULL,
            centraal_examen DOUBLE,
            centraal_examen_vergelijking DOUBLE,
            school_examen DOUBLE,
            eindcijfer DOUBLE,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """
    )

    con.execute(
        """
        CREATE TABLE geslaagden_per_profiel (
            school_id INTEGER NOT NULL,
            profiel VARCHAR NOT NULL,
            deelnemers VARCHAR,
            geslaagden VARCHAR,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """
    )

    con.execute(
        """
        CREATE TABLE oordeel_inspectie (
            school_id INTEGER NOT NULL,
            indicator VARCHAR NOT NULL,
            inspectienorm DOUBLE,
            schoolwaarde DOUBLE,
            periode VARCHAR,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """
    )

    # Satisfaction (tevredenheid) tables
    con.execute(
        """
        CREATE TABLE tevredenheid_trend (
            school_id INTEGER NOT NULL,
            metric VARCHAR NOT NULL,
            schooljaar VARCHAR NOT NULL,
            cijfer DOUBLE,
            vergelijking DOUBLE,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """
    )

    con.execute(
        """
        CREATE TABLE tevredenheid_vragen (
            school_id INTEGER NOT NULL,
            respondent VARCHAR NOT NULL,
            vraag VARCHAR NOT NULL,
            cijfer DOUBLE,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """
    )


def load_school_data(con, school_id, data):
    """Load data for one school into the database."""

    school = data.get("school", {})
    con.execute(
        """
        INSERT INTO schools (id, name, address, postal_code, city, aantal_leerlingen)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        [
            school_id,
            school.get("name"),
            school.get("address"),
            school.get("postalCode"),
            school.get("city"),
            school.get("aantalLeerlingen"),
        ],
    )

    # Doorstroom onderbouw (combined for all levels)
    onderbouw = data.get("doorstroom", {}).get("onderbouw", {}).get("combined", [])
    for item in onderbouw:
        con.execute(
            """
            INSERT INTO doorstroom_onderbouw (school_id, schooljaar, percentage)
            VALUES (?, ?, ?)
        """,
            [school_id, item.get("schooljaar"), item.get("percentage")],
        )

    # Doorstroom bovenbouw (VWO only)
    bovenbouw = data.get("doorstroom", {}).get("bovenbouw", {}).get("vwo", [])
    for item in bovenbouw:
        con.execute(
            """
            INSERT INTO doorstroom_bovenbouw (school_id, schooljaar, percentage)
            VALUES (?, ?, ?)
        """,
            [school_id, item.get("schooljaar"), item.get("percentage")],
        )

    # Schooladvies (VWO only)
    schooladvies = data.get("schooladvies", {}).get("vwo", [])
    for item in schooladvies:
        con.execute(
            """
            INSERT INTO schooladvies (school_id, positie, percentage, vergelijking)
            VALUES (?, ?, ?, ?)
        """,
            [
                school_id,
                item.get("positie"),
                item.get("percentage"),
                item.get("vergelijking"),
            ],
        )

    # Slagingspercentage (VWO only)
    slagingspercentage = data.get("slagingspercentage", {}).get("vwo", [])
    for item in slagingspercentage:
        con.execute(
            """
            INSERT INTO slagingspercentage (school_id, schooljaar, percentage, vergelijking)
            VALUES (?, ?, ?, ?)
        """,
            [
                school_id,
                item.get("schooljaar"),
                item.get("percentage"),
                item.get("vergelijking"),
            ],
        )

    # Examencijfers (VWO only)
    examencijfers = data.get("examencijfers", {}).get("vwo", [])
    for item in examencijfers:
        con.execute(
            """
            INSERT INTO examencijfers (school_id, schooljaar, centraal_examen,
                                       centraal_examen_vergelijking, school_examen, eindcijfer)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            [
                school_id,
                item.get("schooljaar"),
                item.get("centraalExamen"),
                item.get("centraalExamenVergelijking"),
                item.get("schoolExamen"),
                item.get("eindcijfer"),
            ],
        )

    # Geslaagden per profiel (VWO only)
    geslaagden = data.get("geslaagdenPerProfiel", {}).get("vwo", [])
    for item in geslaagden:
        con.execute(
            """
            INSERT INTO geslaagden_per_profiel (school_id, profiel, deelnemers, geslaagden)
            VALUES (?, ?, ?, ?)
        """,
            [
                school_id,
                item.get("profiel"),
                str(item.get("deelnemers", "")),
                str(item.get("geslaagden", "")),
            ],
        )

    # Oordeel inspectie
    oordeel = data.get("oordeelInspectie", {})

    # Onderwijspositie
    if "onderwijspositie" in oordeel:
        item = oordeel["onderwijspositie"]
        con.execute(
            """
            INSERT INTO oordeel_inspectie (school_id, indicator, inspectienorm, schoolwaarde, periode)
            VALUES (?, ?, ?, ?, ?)
        """,
            [
                school_id,
                "onderwijspositie",
                item.get("inspectienorm"),
                item.get("schoolwaarde"),
                item.get("periode"),
            ],
        )

    # Onderbouwsnelheid
    if "onderbouwsnelheid" in oordeel:
        item = oordeel["onderbouwsnelheid"]
        con.execute(
            """
            INSERT INTO oordeel_inspectie (school_id, indicator, inspectienorm, schoolwaarde, periode)
            VALUES (?, ?, ?, ?, ?)
        """,
            [
                school_id,
                "onderbouwsnelheid",
                item.get("inspectienorm"),
                item.get("schoolwaarde"),
                item.get("periode"),
            ],
        )

    # Bovenbouwsucces (VWO only)
    bovenbouwsucces = oordeel.get("bovenbouwsucces", {}).get("vwo")
    if bovenbouwsucces:
        con.execute(
            """
            INSERT INTO oordeel_inspectie (school_id, indicator, inspectienorm, schoolwaarde, periode)
            VALUES (?, ?, ?, ?, ?)
        """,
            [
                school_id,
                "bovenbouwsucces",
                bovenbouwsucces.get("inspectienorm"),
                bovenbouwsucces.get("schoolwaarde"),
                bovenbouwsucces.get("periode"),
            ],
        )


def load_tevredenheid_data(con, school_name_to_id, data):
    """Load tevredenheid data for one school into the database."""
    school_name = data.get("school", {}).get("name", "")

    # Find matching school_id
    school_id = school_name_to_id.get(school_name)
    if not school_id:
        print(f"  -> Warning: No matching school found for '{school_name}'")
        return False

    # Load trend data
    trends = data.get("trends", {})
    for metric, items in trends.items():
        for item in items:
            con.execute(
                """
                INSERT INTO tevredenheid_trend (school_id, metric, schooljaar, cijfer, vergelijking)
                VALUES (?, ?, ?, ?, ?)
            """,
                [
                    school_id,
                    metric,
                    item.get("schooljaar"),
                    item.get("cijfer"),
                    item.get("vergelijking"),
                ],
            )

    # Load question data
    vragen = data.get("vragen", {})
    for respondent, questions in vragen.items():
        for item in questions:
            con.execute(
                """
                INSERT INTO tevredenheid_vragen (school_id, respondent, vraag, cijfer)
                VALUES (?, ?, ?, ?)
            """,
                [school_id, respondent, item.get("vraag"), item.get("cijfer")],
            )

    return True


def main():
    base_dir = Path(".")
    db_path = base_dir / "scholen.duckdb"
    json_dir = base_dir / "json"

    # Remove existing database
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))

    # Create schema
    create_schema(con)

    # Find all school result JSON files (not tevredenheid)
    json_files = sorted([f for f in json_dir.glob("resultaten-*.json")])

    school_name_to_id = {}

    for school_id, json_file in enumerate(json_files, start=1):
        print(f"Loading: {json_file.name}")
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        load_school_data(con, school_id, data)
        school_name = data.get("school", {}).get("name", "Unknown")
        school_name_to_id[school_name] = school_id
        print(f"  -> {school_name}")

    # Load tevredenheid data
    print("\n--- Loading Tevredenheid Data ---")
    tevredenheid_files = sorted(json_dir.glob("tevredenheid-*.json"))

    for json_file in tevredenheid_files:
        print(f"Loading: {json_file.name}")
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        if load_tevredenheid_data(con, school_name_to_id, data):
            print(f"  -> {data.get('school', {}).get('name', 'Unknown')}")

    # Show summary
    print("\n--- Database Summary ---")
    for table in [
        "schools",
        "doorstroom_onderbouw",
        "doorstroom_bovenbouw",
        "schooladvies",
        "slagingspercentage",
        "examencijfers",
        "geslaagden_per_profiel",
        "oordeel_inspectie",
        "tevredenheid_trend",
        "tevredenheid_vragen",
    ]:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"{table}: {count} rows")

    con.close()
    print(f"\nDatabase saved to: {db_path}")


if __name__ == "__main__":
    main()
