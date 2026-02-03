"""
Update loting_matching.duckdb from JSON source files.

Flow: json/matching_en_plaatsing/*.json -> loting_matching.duckdb (updates 2025 data)

Note: The loting_matching.duckdb database contains historical data from previous years.
This module updates the 2025 data from JSON files extracted from the PDF report.
"""

import json
import duckdb
from pathlib import Path


# =============================================================================
# Helper Functions
# =============================================================================

def load_json(json_dir: Path, filename: str):
    """Load a JSON file from the JSON directory."""
    with open(json_dir / filename) as f:
        return json.load(f)


def normalize_niveau(niveau: str) -> str:
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
        lookup[afd_naam.lower()] = afd_id

    return lookup, result


def find_afdeling_id(lookup, all_afdelingen, school: str, niveau: str):
    """Find the afdeling ID for a school+niveau combination."""
    niveau = normalize_niveau(niveau)

    # Try exact match first
    full_name = f"{school} - {niveau}".lower()
    if full_name in lookup:
        return lookup[full_name]

    # Try matching school name from database
    for afd_id, afd_naam, school_naam in all_afdelingen:
        afd_lower = afd_naam.lower()

        if niveau.lower() in afd_lower:
            school_lower = school.lower()

            # Direct match
            if school_lower in afd_lower:
                return afd_id

            # Handle abbreviated names
            school_parts = school_lower.split()
            if len(school_parts) >= 2:
                if all(part in afd_lower for part in school_parts[:2]):
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
        ("College", "vwo"): None,
        ("College", "Havo"): None,
        ("Het Lyceum", "v.a. havo"): "Cartesius Amsterdam - Het Lyceum - v.a. havo",
        ("Het Lyceum", "vwo"): "Cartesius Amsterdam - Het Lyceum - vwo",
        ("College de Meer", "Havo"): "College De Meer - Havo potentie - v.a. vmbo-g-t",
        ("College de Meer", "v.a. vmbo-b"): "College De Meer - v.a. vmbo-b",
        ("Comenius Lyceum", "v.a. havo"): "Comenius Lyceum Amsterdam - v.a. havo",
        ("Comenius Lyceum", "vwo"): "Comenius Lyceum Amsterdam - vwo",
        ("Lyceum", "v.a. havo"): None,  # Ambiguous
        ("Lyceum", "vwo"): None,
        ("Lyceum - Coderclass", "v.a. havo"): "Metis Montessori Lyceum - Coderclass of Kunst & Co. - v.a. havo",
        ("Lyceum - Coderclass", "vwo"): "Metis Montessori Lyceum - Coderclass of Kunst & Co. - vwo",
        ("Lyceum - Technasium", "v.a. havo"): "Metis Montessori Lyceum - Technasium - v.a. havo",
        ("Lyceum - Technasium", "vwo"): "Metis Montessori Lyceum - Technasium - vwo",
        # Montessori schools
        ("Montessori Lyceum Amsterdam", "v.a. havo"): "Montessori Lyceum Amsterdam - v.a. havo",
        ("Montessori Lyceum Amsterdam", "vwo"): "Montessori Lyceum Amsterdam - vwo",
        ("Montessori Lyceum Amsterdam - Gymnasium", "vwo"): "Montessori Lyceum Amsterdam - Gymnasium - vwo",
        ("Montessori Lyceum Oostpoort", "v.a. havo"): "Montessori Lyceum Oostpoort - v.a. havo",
        ("Montessori Lyceum Oostpoort", "v.a. vmbo-b"): "Montessori Lyceum Oostpoort - v.a. vmbo-b",
        ("Montessori Lyceum Terra Nova", "v.a. havo"): "Montessori Lyceum Terra Nova - v.a. havo",
        ("Montessori Lyceum Terra Nova", "v.a. vmbo-b"): "Montessori Lyceum Terra Nova - v.a. vmbo-b",
        ("Montessori Lyceum Terra Nova", "v.a. vmbo-k"): "Montessori Lyceum Terra Nova - v.a. vmbo-k",
        ("Montessori Lyceum Terra Nova", "vwo"): "Montessori Lyceum Terra Nova - vwo",
        # Other schools
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
        # Fix Mundus College vmbo-g-t
        ("Mundus College", "vmbo-g-t"): "Mundus College - vmbo-g-t",
        ("Mundus College", "v.a. vmbo-g-t"): "Mundus College - vmbo-g-t",
        # Fix TASC vmbo-g-t
        ("TASC", "vmbo-g-t"): "TASC - vmbo-g-t",
        ("TASC", "v.a. vmbo-g-t"): "TASC - vmbo-g-t",
    }


# =============================================================================
# Database Update Functions
# =============================================================================

def update_database(base_dir: Path):
    """Update loting_matching.duckdb from JSON files."""
    json_dir = base_dir / "json" / "matching_en_plaatsing"
    db_path = base_dir / "loting_matching.duckdb"

    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        print("The loting_matching.duckdb contains historical data and must exist.")
        return

    db = duckdb.connect(str(db_path))

    # Load JSON data
    detailed_data = load_json(json_dir, "gedetailleerde_schooldata.json")
    jaar_samenvatting = load_json(json_dir, "jaar_samenvatting.json")

    # Build lookup
    lookup, all_afdelingen = build_afdeling_lookup(db)
    name_mapping = create_name_mapping()

    # Create a reverse lookup by afdeling name
    name_to_id = {afd_naam.lower(): afd_id for afd_id, afd_naam, _ in all_afdelingen}

    print("Updating loting_matching.duckdb from JSON files...\n")

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
            db.execute("""
                DELETE FROM plaatsing_per_voorkeur
                WHERE afdeling_id = ? AND jaar = 2025
            """, [afd_id])

            for pos, key in [(1, "eerste"), (2, "tweede"), (3, "derde")]:
                val = gpv.get(key)
                if val is not None and val > 0:
                    db.execute("""
                        INSERT INTO plaatsing_per_voorkeur (afdeling_id, jaar, voorkeur_positie, aantal)
                        VALUES (?, 2025, ?, ?)
                    """, [afd_id, pos, val])

            vierde_plus = gpv.get("vierde_plus")
            if vierde_plus is not None and vierde_plus > 0:
                db.execute("""
                    INSERT INTO plaatsing_per_voorkeur (afdeling_id, jaar, voorkeur_positie, aantal)
                    VALUES (?, 2025, 4, ?)
                """, [afd_id, vierde_plus])

        updated_count += 1
        print(f"  {school} - {niveau_norm}")

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
        print(f"\n  Updated jaar_samenvatting for 2025")

    db.close()

    print(f"\n--- Summary ---")
    print(f"  Updated: {updated_count} entries")
    print(f"  Skipped (ambiguous): {skipped_count} entries")
    print(f"  Not found in DB: {len(not_found)} entries")

    if not_found:
        print(f"\nEntries not found in database:")
        for name in sorted(set(not_found)):
            print(f"  - {name}")


def build(base_dir: Path):
    """Update loting_matching.duckdb from JSON files."""
    print("=" * 60)
    print("Updating loting_matching.duckdb from JSON files")
    print("=" * 60)
    print()

    update_database(base_dir)

    print("\nDone!")
