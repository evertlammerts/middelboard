#!/usr/bin/env python3
"""
Update loting_matching.duckdb from JSON source of truth files.
"""

import json
import duckdb
from pathlib import Path

JSON_DIR = Path("json/matching_en_plaatsing")
DB_PATH = "loting_matching.duckdb"


def load_json(filename):
    """Load a JSON file from the JSON directory."""
    with open(JSON_DIR / filename) as f:
        return json.load(f)


def normalize_niveau(niveau):
    """Normalize niveau names to match database format."""
    niveau = niveau.lower().strip()
    mappings = {
        "havo": "v.a. havo",
        "vmbo-g-t": "v.a. vmbo-g-t",
    }
    return mappings.get(niveau, niveau)


def build_afdeling_lookup(db):
    """Build a lookup from afdeling name to id."""
    result = db.execute("""
        SELECT a.id, a.naam, s.naam as school_naam
        FROM afdeling a
        JOIN loting_school s ON a.school_id = s.id
    """).fetchall()

    lookup = {}
    for afd_id, afd_naam, school_naam in result:
        # Store by full afdeling name
        lookup[afd_naam.lower()] = afd_id
        # Also store by school name + niveau pattern
        # e.g., "Comenius Lyceum Amsterdam - v.a. havo" -> id
        lookup[afd_naam.lower()] = afd_id

    return lookup, result


def find_afdeling_id(lookup, all_afdelingen, school, niveau):
    """Find the afdeling ID for a school+niveau combination."""
    niveau = normalize_niveau(niveau)

    # Try exact match first
    full_name = f"{school} - {niveau}".lower()
    if full_name in lookup:
        return lookup[full_name]

    # Try matching school name from database
    for afd_id, afd_naam, school_naam in all_afdelingen:
        afd_lower = afd_naam.lower()

        # Check if the JSON school name is contained in the afdeling name
        # and the niveau matches
        if niveau.lower() in afd_lower:
            # Try various school name patterns
            school_lower = school.lower()

            # Direct match
            if school_lower in afd_lower:
                return afd_id

            # Handle abbreviated names
            school_parts = school_lower.split()
            if len(school_parts) >= 2:
                # Try first two words
                if all(part in afd_lower for part in school_parts[:2]):
                    # Make sure the niveau also matches at the end
                    if afd_lower.endswith(niveau.lower()):
                        return afd_id

    return None


def create_name_mapping():
    """Create explicit mapping for tricky school names."""
    return {
        # JSON name -> DB afdeling name pattern
        ("Berlage Lyceum - tto", "vwo"): "Berlage Lyceum - Tweetalig - vwo",
        ("Berlage Lyceum - tto", "v.a. havo"): "Berlage Lyceum - Tweetalig - v.a. havo",
        ("College", "v.a. havo"): None,  # Ambiguous - skip
        ("College", "vwo"): None,  # Ambiguous - skip
        ("College", "Havo"): None,  # Ambiguous - skip
        ("Het Lyceum", "v.a. havo"): "Cartesius Amsterdam - Het Lyceum - v.a. havo",
        ("Het Lyceum", "vwo"): "Cartesius Amsterdam - Het Lyceum - vwo",
        ("College de Meer", "Havo"): "College De Meer - Havo potentie - v.a. vmbo-g-t",
        ("College de Meer", "v.a. vmbo-b"): "College De Meer - v.a. vmbo-b",
        ("Comenius Lyceum", "v.a. havo"): "Comenius Lyceum Amsterdam - v.a. havo",
        ("Comenius Lyceum", "vwo"): "Comenius Lyceum Amsterdam - vwo",
        ("Lyceum", "v.a. havo"): None,  # Ambiguous
        ("Lyceum", "vwo"): None,  # Ambiguous
        ("Lyceum - Coderclass", "v.a. havo"): "Metis Montessori Lyceum - Coderclass of Kunst & Co. - v.a. havo",
        ("Lyceum - Coderclass", "vwo"): "Metis Montessori Lyceum - Coderclass of Kunst & Co. - vwo",
        ("Lyceum - Technasium", "v.a. havo"): "Metis Montessori Lyceum - Technasium - v.a. havo",
        ("Lyceum - Technasium", "vwo"): "Metis Montessori Lyceum - Technasium - vwo",
        # Montessori schools - now have correct full names in JSON
        ("Montessori Lyceum Amsterdam", "v.a. havo"): "Montessori Lyceum Amsterdam - v.a. havo",
        ("Montessori Lyceum Amsterdam", "vwo"): "Montessori Lyceum Amsterdam - vwo",
        ("Montessori Lyceum Amsterdam - Gymnasium", "vwo"): "Montessori Lyceum Amsterdam - Gymnasium - vwo",
        ("Montessori Lyceum Oostpoort", "v.a. havo"): "Montessori Lyceum Oostpoort - v.a. havo",
        ("Montessori Lyceum Oostpoort", "v.a. vmbo-b"): "Montessori Lyceum Oostpoort - v.a. vmbo-b",
        ("Montessori Lyceum Terra Nova", "v.a. havo"): "Montessori Lyceum Terra Nova - v.a. havo",
        ("Montessori Lyceum Terra Nova", "v.a. vmbo-b"): "Montessori Lyceum Terra Nova - v.a. vmbo-b",
        ("Montessori Lyceum Terra Nova", "v.a. vmbo-k"): "Montessori Lyceum Terra Nova - v.a. vmbo-k",
        ("Montessori Lyceum Terra Nova", "vwo"): "Montessori Lyceum Terra Nova - vwo",
        # Other schools with corrected names
        ("Cartesius Amsterdam - Het Lyceum", "v.a. havo"): "Cartesius Amsterdam - Het Lyceum - v.a. havo",
        ("Cartesius Amsterdam - Het Lyceum", "vwo"): "Cartesius Amsterdam - Het Lyceum - vwo",
        ("Cornelius Haga Lyceum", "v.a. havo"): "Cornelius Haga Lyceum - v.a. havo",
        ("Cornelius Haga Lyceum", "vwo"): "Cornelius Haga Lyceum - vwo",
        ("Gerrit van der Veen College", "v.a. havo"): "Gerrit van der Veen College - v.a. havo",
        ("Gerrit van der Veen College", "vwo"): "Gerrit van der Veen College - vwo",
        ("Pieter Nieuwland College", "v.a. havo"): "Pieter Nieuwland College - v.a. havo",
        ("Pieter Nieuwland College", "vwo"): "Pieter Nieuwland College - vwo",
        ("Havo de Hof", "v.a. havo"): "Havo de Hof - v.a. havo",
        ("Hervormd Lyceum West", "v.a. havo"): "Hervormd Lyceum West - v.a. havo",
        ("St. Nicolaaslyceum - Tweetalig Onderwijs", "v.a. havo"): "St. Nicolaaslyceum - Tweetalig Onderwijs - v.a. havo",
        ("St. Nicolaaslyceum - Tweetalig Onderwijs", "vwo"): "St. Nicolaaslyceum - Tweetalig Onderwijs - vwo",
        ("Hervormd Lyceum", "v.a. havo"): "HLZ (Hervormd Lyceum Zuid) - v.a. havo",
        ("Hervormd Lyceum", "vwo"): "HLZ (Hervormd Lyceum Zuid) - vwo",
        ("Mediacollege", "v.a. vmbo-b"): "Mediacollege Amsterdam - v.a. vmbo-b",
        ("Mediacollege", "v.a. vmbo-k"): "Mediacollege Amsterdam - v.a. vmbo-k",
        ("Yuverta VMBO", "v.a. vmbo-b"): "Yuverta VMBO Amsterdam Oost - v.a. vmbo-b",
        ("Yuverta VMBO", "vmbo-g-t"): "Yuverta VMBO Amsterdam West - vmbo-g-t",
        ("Yuverta VMBO", "v.a. vmbo-g-t"): "Yuverta VMBO Amsterdam West - vmbo-g-t",
        # Fix Damstede Lyceum mapping
        ("Damstede Lyceum", "v.a. havo"): "Damstede - v.a. havo",
        ("Damstede Lyceum", "vwo"): "Damstede - vwo",
        # Fix Mundus College vmbo-g-t (DB uses vmbo-g-t not v.a. vmbo-g-t)
        ("Mundus College", "vmbo-g-t"): "Mundus College - vmbo-g-t",
        ("Mundus College", "v.a. vmbo-g-t"): "Mundus College - vmbo-g-t",
        # Fix TASC vmbo-g-t
        ("TASC", "vmbo-g-t"): "TASC - vmbo-g-t",
        ("TASC", "v.a. vmbo-g-t"): "TASC - vmbo-g-t",
    }


def update_database():
    """Main function to update the database from JSON files."""
    db = duckdb.connect(DB_PATH)

    # Load JSON data
    detailed_data = load_json("gedetailleerde_schooldata.json")
    jaar_samenvatting = load_json("jaar_samenvatting.json")

    # Build lookup
    lookup, all_afdelingen = build_afdeling_lookup(db)
    name_mapping = create_name_mapping()

    # Create a reverse lookup by afdeling name
    name_to_id = {afd_naam.lower(): afd_id for afd_id, afd_naam, _ in all_afdelingen}

    print("=== Updating database from JSON source of truth ===\n")

    updated_count = 0
    skipped_count = 0
    not_found = []

    for entry in detailed_data:
        school = entry["school"]
        niveau = entry["niveau"]
        niveau_norm = normalize_niveau(niveau)

        # Check explicit mapping first
        key = (school, niveau_norm)
        if key in name_mapping:
            mapped_name = name_mapping[key]
            if mapped_name is None:
                skipped_count += 1
                continue
            afd_id = name_to_id.get(mapped_name.lower())
        else:
            afd_id = find_afdeling_id(lookup, all_afdelingen, school, niveau)

        if afd_id is None:
            not_found.append(f"{school} - {niveau_norm}")
            continue

        # Update capaciteit table for 2025
        cap_2025 = entry.get("capaciteit", {}).get("2025")
        if cap_2025 is not None:
            db.execute("""
                UPDATE capaciteit
                SET definitieve_capaciteit = ?
                WHERE afdeling_id = ? AND jaar = 2025
            """, [cap_2025, afd_id])

            # Check if row exists, if not insert
            exists = db.execute("""
                SELECT 1 FROM capaciteit WHERE afdeling_id = ? AND jaar = 2025
            """, [afd_id]).fetchone()
            if not exists:
                db.execute("""
                    INSERT INTO capaciteit (afdeling_id, jaar, definitieve_capaciteit)
                    VALUES (?, 2025, ?)
                """, [afd_id, cap_2025])

        # Update voorkeuren table for 2025
        vk = entry.get("voorkeuren_2025", {})
        if vk:
            eerste = vk.get("eerste")
            tweede = vk.get("tweede")
            derde = vk.get("derde")

            # Check if exists
            exists = db.execute("""
                SELECT 1 FROM voorkeuren WHERE afdeling_id = ? AND jaar = 2025
            """, [afd_id]).fetchone()

            if exists:
                db.execute("""
                    UPDATE voorkeuren
                    SET eerste_voorkeur = ?, tweede_voorkeur = ?, derde_voorkeur = ?
                    WHERE afdeling_id = ? AND jaar = 2025
                """, [eerste, tweede, derde, afd_id])
            else:
                db.execute("""
                    INSERT INTO voorkeuren (afdeling_id, jaar, eerste_voorkeur, tweede_voorkeur, derde_voorkeur)
                    VALUES (?, 2025, ?, ?, ?)
                """, [afd_id, eerste, tweede, derde])

        # Update plaatsingen table for 2025
        geplaatst_2025 = entry.get("geplaatst", {}).get("2025")
        if geplaatst_2025 is not None:
            exists = db.execute("""
                SELECT 1 FROM plaatsingen WHERE afdeling_id = ? AND jaar = 2025
            """, [afd_id]).fetchone()

            if exists:
                db.execute("""
                    UPDATE plaatsingen
                    SET totaal_geplaatst = ?
                    WHERE afdeling_id = ? AND jaar = 2025
                """, [geplaatst_2025, afd_id])
            else:
                db.execute("""
                    INSERT INTO plaatsingen (afdeling_id, jaar, totaal_geplaatst)
                    VALUES (?, 2025, ?)
                """, [afd_id, geplaatst_2025])

        # Update plaatsing_per_voorkeur table for 2025
        gpv = entry.get("geplaatst_naar_voorkeur_2025", {})
        if gpv:
            # Delete existing entries for this afdeling/year
            db.execute("""
                DELETE FROM plaatsing_per_voorkeur
                WHERE afdeling_id = ? AND jaar = 2025
            """, [afd_id])

            # Insert new entries
            for pos, key in [(1, "eerste"), (2, "tweede"), (3, "derde")]:
                val = gpv.get(key)
                if val is not None and val > 0:
                    db.execute("""
                        INSERT INTO plaatsing_per_voorkeur (afdeling_id, jaar, voorkeur_positie, aantal)
                        VALUES (?, 2025, ?, ?)
                    """, [afd_id, pos, val])

            # Handle 4th+ as position 4
            vierde_plus = gpv.get("vierde_plus")
            if vierde_plus is not None and vierde_plus > 0:
                db.execute("""
                    INSERT INTO plaatsing_per_voorkeur (afdeling_id, jaar, voorkeur_positie, aantal)
                    VALUES (?, 2025, 4, ?)
                """, [afd_id, vierde_plus])

        updated_count += 1
        print(f"✓ Updated: {school} - {niveau_norm} (id={afd_id})")

    # Update jaar_samenvatting
    if "2025" in jaar_samenvatting:
        js = jaar_samenvatting["2025"]
        db.execute("""
            UPDATE jaar_samenvatting
            SET totaal_deelnemers = ?,
                totaal_capaciteit = ?,
                percentage_eerste_voorkeur = ?,
                percentage_top3 = ?
            WHERE jaar = 2025
        """, [
            js.get("totaal_deelnemers"),
            js.get("totaal_capaciteit"),
            js.get("percentage_eerste_voorkeur"),
            js.get("percentage_top3")
        ])
        print(f"\n✓ Updated jaar_samenvatting for 2025")

    db.close()

    print(f"\n=== Summary ===")
    print(f"Updated: {updated_count} entries")
    print(f"Skipped (ambiguous): {skipped_count} entries")
    print(f"Not found in DB: {len(not_found)} entries")

    if not_found:
        print(f"\nEntries not found in database:")
        for name in sorted(set(not_found)):
            print(f"  - {name}")


if __name__ == "__main__":
    update_database()
