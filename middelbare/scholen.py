"""
Parse school HTML files from scholenopdekaart.nl and create scholen.duckdb.

Flow: html/*.html -> json/resultaten-*.json + json/tevredenheid-*.json -> scholen.duckdb
"""

import json
import re
import html
import duckdb
from pathlib import Path


# =============================================================================
# HTML Parsing Functions
# =============================================================================

def decode_json_attr(attr_value):
    """Decode HTML-encoded JSON from attribute value."""
    decoded = html.unescape(attr_value)
    return json.loads(decoded)


def extract_school_info(content):
    """Extract basic school information."""
    info = {}

    # School name from title
    match = re.search(r"<title>Resultaten - ([^(]+)\s*\(", content)
    if match:
        info["name"] = match.group(1).strip()

    # Address info from span elements
    match = re.search(r"<span class=school-adres>([^<]+)</span>", content)
    if match:
        info["address"] = match.group(1).strip()

    match = re.search(r"<span class=school-postcode-woonplaats>([^<]+)</span>", content)
    if match:
        postcode_city = match.group(1).strip()
        pc_match = re.match(r"(\d{4}\s*[A-Z]{2})\s+(.*)", postcode_city)
        if pc_match:
            info["postalCode"] = pc_match.group(1)
            info["city"] = pc_match.group(2)
        else:
            info["city"] = postcode_city

    # Extract number of students (leerlingen)
    match = re.search(r'data-dfn="Het aantal leerlingen op de school\.">(\d+)\s*leerlingen', content)
    if match:
        info["aantalLeerlingen"] = int(match.group(1))

    return info


def extract_doorstroom(content):
    """Extract doorstroom (progression) data."""
    doorstroom = {"onderbouw": {}, "bovenbouw": {}}

    pattern = r'<doorstroom-line-chart[^>]*periodes="([^"]+)"[^>]*chart-title="([^"]+)"'
    matches = re.findall(pattern, content)

    for periodes_encoded, title in matches:
        data = decode_json_attr(periodes_encoded)
        title_lower = title.lower()

        if "onderbouw" in title_lower:
            doorstroom["onderbouw"]["combined"] = data
        elif "bovenbouw" in title_lower:
            if "havo" in title_lower:
                doorstroom["bovenbouw"]["havo"] = data
            elif "vwo" in title_lower:
                doorstroom["bovenbouw"]["vwo"] = data
            elif "vmbo" in title_lower:
                doorstroom["bovenbouw"]["vmbo"] = data

    return doorstroom


def extract_schooladvies(content):
    """Extract schooladvies comparison data."""
    schooladvies = {}

    pattern = r'<vergelijking-schooladvies-bar-chart[^>]*vergelijkingen="([^"]+)"[^>]*chart-title="([^"]+)"'
    matches = re.findall(pattern, content)

    for vergelijkingen_encoded, title in matches:
        data = decode_json_attr(vergelijkingen_encoded)
        title_lower = title.lower()

        cleaned = []
        for item in data:
            cleaned.append({
                "positie": item.get("positieVergelekenMetSchooladvies", ""),
                "percentage": item.get("percentage", 0),
                "vergelijking": item.get("percentageVergelijking", 0),
            })

        if "havo" in title_lower:
            schooladvies["havo"] = cleaned
        elif "vwo" in title_lower:
            schooladvies["vwo"] = cleaned
        elif "vmbo" in title_lower:
            schooladvies["vmbo"] = cleaned

    return schooladvies


def extract_slagingspercentage(content):
    """Extract pass rate data over years."""
    slagingspercentage = {}

    pattern = r'<slaagpercentage-trend-line-chart[^>]*slagingspercentages="([^"]+)"[^>]*chart-title="([^"]+)"'
    matches = re.findall(pattern, content)

    for data_encoded, title in matches:
        data = decode_json_attr(data_encoded)
        title_lower = title.lower()

        cleaned = []
        for item in data:
            cleaned.append({
                "schooljaar": item.get("schooljaar", ""),
                "percentage": item.get("percentage", 0),
                "vergelijking": item.get("vergelijking", 0),
            })

        if "havo" in title_lower:
            slagingspercentage["havo"] = cleaned
        elif "vwo" in title_lower:
            slagingspercentage["vwo"] = cleaned
        elif "vmbo" in title_lower:
            slagingspercentage["vmbo"] = cleaned

    return slagingspercentage


def extract_examencijfers(content):
    """Extract exam grade data."""
    examencijfers = {}

    pattern = r'<examencijfers-trend-line-chart[^>]*examencijfers="([^"]+)"[^>]*chart-title="([^"]+)"'
    matches = re.findall(pattern, content)

    for data_encoded, title in matches:
        data = decode_json_attr(data_encoded)
        title_lower = title.lower()

        cleaned = []
        for item in data:
            cleaned.append({
                "schooljaar": item.get("schooljaar", ""),
                "centraalExamen": item.get("centraalExamencijfer", 0),
                "centraalExamenVergelijking": item.get("centraalExamencijferVergelijking", 0),
                "schoolExamen": item.get("schoolExamencijfer", 0),
                "eindcijfer": item.get("eindcijfer", 0),
            })

        if "havo" in title_lower:
            examencijfers["havo"] = cleaned
        elif "vwo" in title_lower:
            examencijfers["vwo"] = cleaned
        elif "vmbo" in title_lower:
            examencijfers["vmbo"] = cleaned

    return examencijfers


def extract_geslaagden_per_profiel(content):
    """Extract pass rates per profile from HTML tables."""
    geslaagden = {}

    sections = re.split(
        r"<tr><th>(havo|vwo|vmbo(?:-\(g\)t)?)</th>", content, flags=re.IGNORECASE
    )

    row_pattern = r"<tr><td>([^<]+)</td><td[^>]*>([^<]+)</td><td[^>]*>([^<]+)</td>"

    for i in range(1, len(sections), 2):
        level = sections[i].lower()
        section_content = sections[i + 1] if i + 1 < len(sections) else ""

        end_match = re.search(r"</table>|<tr><th>", section_content)
        if end_match:
            section_content = section_content[: end_match.start()]

        rows = re.findall(row_pattern, section_content)
        if rows:
            geslaagden[level] = []
            for profiel, deelnemers, geslaagd in rows:
                profiel = profiel.strip()
                if (
                    profiel
                    and profiel not in ["Profiel", "Totaal", ""]
                    and any(
                        p in profiel
                        for p in ["Cultuur", "Economie", "Natuur", "Maatschappij", "Techniek", "Gezondheid"]
                    )
                ):
                    geslaagden[level].append({
                        "profiel": profiel,
                        "deelnemers": html.unescape(deelnemers.strip()),
                        "geslaagden": html.unescape(geslaagd.strip()),
                    })

    return geslaagden


def extract_oordeel_inspectie(content):
    """Extract inspection judgement data."""
    oordeel = {}

    pattern = r'<oordeel-inspectie-bar-chart[^>]*class="[^"]*chart-([^"]+)"[^>]*json-data="([^"]+)"'
    matches = re.findall(pattern, content)

    for chart_type, data_encoded in matches:
        data = decode_json_attr(data_encoded)

        if chart_type == "onderwijspositie":
            if data:
                item = data[0]
                oordeel["onderwijspositie"] = {
                    "inspectienorm": item.get("inspectienorm", 0),
                    "schoolwaarde": item.get("schoolwaarde", 0),
                    "periode": f"{item.get('schooljaarVan', '')} t/m {item.get('schooljaarTotEnMet', '')}",
                }
        elif chart_type == "onderbouwsnelheid":
            if data:
                item = data[0]
                oordeel["onderbouwsnelheid"] = {
                    "inspectienorm": item.get("inspectienorm", 0),
                    "schoolwaarde": item.get("schoolwaarde", 0),
                    "periode": f"{item.get('schooljaarVan', '')} t/m {item.get('schooljaarTotEnMet', '')}",
                }
        elif chart_type == "bovenbouwsucces":
            oordeel["bovenbouwsucces"] = {}
            for item in data:
                level = item.get("onderwijssoort", "").lower()
                if level:
                    oordeel["bovenbouwsucces"][level] = {
                        "inspectienorm": item.get("inspectienorm", 0),
                        "schoolwaarde": item.get("schoolwaarde", 0),
                        "periode": f"{item.get('schooljaarVan', '')} t/m {item.get('schooljaarTotEnMet', '')}",
                    }

    return oordeel


def extract_tevredenheid_trends(content):
    """Extract satisfaction trend data from line charts (VWO + parents)."""
    trends = {}

    pattern = r'<tevredenheid-vergelijking-line-chart[^>]*periodes="([^"]+)"[^>]*chart-title="([^"]+)"[^>]*>'
    matches = re.findall(pattern, content)

    for periodes_encoded, chart_title in matches:
        data = decode_json_attr(periodes_encoded)
        chart_title_lower = chart_title.lower() if chart_title else ""

        metric = None
        if "vwo" in chart_title_lower and "leerlingen" in chart_title_lower:
            metric = "leerlingen"
        elif "ouders" in chart_title_lower:
            metric = "ouders"

        if metric:
            trends[metric] = []
            for item in data:
                trends[metric].append({
                    "schooljaar": item.get("schooljaar", ""),
                    "cijfer": item.get("cijfer"),
                    "vergelijking": item.get("cijferVergelijking"),
                })

    # Sfeer trends
    sfeer_section = re.search(
        r'Sfeer in de afgelopen jaren.*?<tevredenheid-vergelijking-line-chart[^>]*periodes="([^"]+)"',
        content,
        re.DOTALL,
    )
    if sfeer_section:
        data = decode_json_attr(sfeer_section.group(1))
        trends["sfeer"] = []
        for item in data:
            trends["sfeer"].append({
                "schooljaar": item.get("schooljaar", ""),
                "cijfer": item.get("cijfer"),
                "vergelijking": item.get("cijferVergelijking"),
            })

    # Veiligheid trends
    veiligheid_section = re.search(
        r'Veiligheid in de afgelopen jaren.*?<tevredenheid-vergelijking-line-chart[^>]*periodes="([^"]+)"',
        content,
        re.DOTALL,
    )
    if veiligheid_section:
        data = decode_json_attr(veiligheid_section.group(1))
        trends["veiligheid"] = []
        for item in data:
            trends["veiligheid"].append({
                "schooljaar": item.get("schooljaar", ""),
                "cijfer": item.get("cijfer"),
                "vergelijking": item.get("cijferVergelijking"),
            })

    return trends


def extract_tevredenheid_vragen(content):
    """Extract satisfaction questions and scores (VWO students + parents)."""
    vragen = {"leerling": [], "ouder": []}

    # Extract student questions (VWO section only)
    leerling_dialog = re.search(
        r"Hoe tevreden zijn de leerlingen in \d{4}-\d{4}, uitgesplitst per vraag\?.*?</dialog>",
        content,
        re.DOTALL,
    )

    if leerling_dialog:
        dialog_content = leerling_dialog.group(0)
        vwo_section = re.search(
            r"<h4>vwo</h4>(.*?)(?:</section>|</dialog>)", dialog_content, re.DOTALL
        )
        if vwo_section:
            section_content = vwo_section.group(1)
            row_pattern = r"<tr><td>([^<]+)</td><td class=numeric[^>]*>([^<]*)</td>"
            rows = re.findall(row_pattern, section_content)

            for vraag, cijfer in rows:
                vraag = html.unescape(vraag.strip())
                cijfer_str = cijfer.strip().replace(",", ".")
                try:
                    cijfer_val = float(cijfer_str) if cijfer_str else None
                except ValueError:
                    cijfer_val = None

                if vraag:
                    vragen["leerling"].append({"vraag": vraag, "cijfer": cijfer_val})

    # Extract parent questions
    ouder_dialog = re.search(
        r"Hoe tevreden zijn de ouders in \d{4}-\d{4}, uitgesplitst per vraag\?.*?</dialog>",
        content,
        re.DOTALL,
    )

    if ouder_dialog:
        dialog_content = ouder_dialog.group(0)
        row_pattern = r"<tr><td>([^<]+)</td><td class=numeric[^>]*>([^<]*)</td>"
        rows = re.findall(row_pattern, dialog_content)

        for vraag, cijfer in rows:
            vraag = html.unescape(vraag.strip())
            cijfer_str = cijfer.strip().replace(",", ".")
            try:
                cijfer_val = float(cijfer_str) if cijfer_str else None
            except ValueError:
                cijfer_val = None

            if vraag:
                vragen["ouder"].append({"vraag": vraag, "cijfer": cijfer_val})

    return vragen


def parse_school_html(filepath):
    """Parse a school HTML file and return structured data."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    return {
        "school": extract_school_info(content),
        "doorstroom": extract_doorstroom(content),
        "schooladvies": extract_schooladvies(content),
        "slagingspercentage": extract_slagingspercentage(content),
        "examencijfers": extract_examencijfers(content),
        "geslaagdenPerProfiel": extract_geslaagden_per_profiel(content),
        "oordeelInspectie": extract_oordeel_inspectie(content),
    }


def parse_tevredenheid_html(filepath):
    """Parse a tevredenheid HTML file and return structured data."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    info = {}
    match = re.search(r"<title>Tevredenheid - ([^(]+)\s*\(", content)
    if match:
        info["name"] = match.group(1).strip()

    return {
        "school": info,
        "trends": extract_tevredenheid_trends(content),
        "vragen": extract_tevredenheid_vragen(content),
    }


# =============================================================================
# Database Creation Functions
# =============================================================================

def create_schema(con):
    """Create database schema for VWO school data."""

    con.execute("""
        CREATE TABLE schools (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            address VARCHAR,
            postal_code VARCHAR,
            city VARCHAR,
            aantal_leerlingen INTEGER
        )
    """)

    con.execute("""
        CREATE TABLE doorstroom_onderbouw (
            school_id INTEGER NOT NULL,
            schooljaar VARCHAR NOT NULL,
            percentage DOUBLE,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """)

    con.execute("""
        CREATE TABLE doorstroom_bovenbouw (
            school_id INTEGER NOT NULL,
            schooljaar VARCHAR NOT NULL,
            percentage DOUBLE,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """)

    con.execute("""
        CREATE TABLE schooladvies (
            school_id INTEGER NOT NULL,
            positie VARCHAR NOT NULL,
            percentage DOUBLE,
            vergelijking DOUBLE,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """)

    con.execute("""
        CREATE TABLE slagingspercentage (
            school_id INTEGER NOT NULL,
            schooljaar VARCHAR NOT NULL,
            percentage DOUBLE,
            vergelijking DOUBLE,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """)

    con.execute("""
        CREATE TABLE examencijfers (
            school_id INTEGER NOT NULL,
            schooljaar VARCHAR NOT NULL,
            centraal_examen DOUBLE,
            centraal_examen_vergelijking DOUBLE,
            school_examen DOUBLE,
            eindcijfer DOUBLE,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """)

    con.execute("""
        CREATE TABLE geslaagden_per_profiel (
            school_id INTEGER NOT NULL,
            profiel VARCHAR NOT NULL,
            deelnemers VARCHAR,
            geslaagden VARCHAR,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """)

    con.execute("""
        CREATE TABLE oordeel_inspectie (
            school_id INTEGER NOT NULL,
            indicator VARCHAR NOT NULL,
            inspectienorm DOUBLE,
            schoolwaarde DOUBLE,
            periode VARCHAR,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """)

    con.execute("""
        CREATE TABLE tevredenheid_trend (
            school_id INTEGER NOT NULL,
            metric VARCHAR NOT NULL,
            schooljaar VARCHAR NOT NULL,
            cijfer DOUBLE,
            vergelijking DOUBLE,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """)

    con.execute("""
        CREATE TABLE tevredenheid_vragen (
            school_id INTEGER NOT NULL,
            respondent VARCHAR NOT NULL,
            vraag VARCHAR NOT NULL,
            cijfer DOUBLE,
            FOREIGN KEY (school_id) REFERENCES schools(id)
        )
    """)


def load_school_data(con, school_id, data):
    """Load data for one school into the database."""

    school = data.get("school", {})
    con.execute("""
        INSERT INTO schools (id, name, address, postal_code, city, aantal_leerlingen)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [
        school_id,
        school.get("name"),
        school.get("address"),
        school.get("postalCode"),
        school.get("city"),
        school.get("aantalLeerlingen"),
    ])

    # Doorstroom onderbouw
    onderbouw = data.get("doorstroom", {}).get("onderbouw", {}).get("combined", [])
    for item in onderbouw:
        con.execute("""
            INSERT INTO doorstroom_onderbouw (school_id, schooljaar, percentage)
            VALUES (?, ?, ?)
        """, [school_id, item.get("schooljaar"), item.get("percentage")])

    # Doorstroom bovenbouw (VWO only)
    bovenbouw = data.get("doorstroom", {}).get("bovenbouw", {}).get("vwo", [])
    for item in bovenbouw:
        con.execute("""
            INSERT INTO doorstroom_bovenbouw (school_id, schooljaar, percentage)
            VALUES (?, ?, ?)
        """, [school_id, item.get("schooljaar"), item.get("percentage")])

    # Schooladvies (VWO only)
    schooladvies = data.get("schooladvies", {}).get("vwo", [])
    for item in schooladvies:
        con.execute("""
            INSERT INTO schooladvies (school_id, positie, percentage, vergelijking)
            VALUES (?, ?, ?, ?)
        """, [school_id, item.get("positie"), item.get("percentage"), item.get("vergelijking")])

    # Slagingspercentage (VWO only)
    slagingspercentage = data.get("slagingspercentage", {}).get("vwo", [])
    for item in slagingspercentage:
        con.execute("""
            INSERT INTO slagingspercentage (school_id, schooljaar, percentage, vergelijking)
            VALUES (?, ?, ?, ?)
        """, [school_id, item.get("schooljaar"), item.get("percentage"), item.get("vergelijking")])

    # Examencijfers (VWO only)
    examencijfers = data.get("examencijfers", {}).get("vwo", [])
    for item in examencijfers:
        con.execute("""
            INSERT INTO examencijfers (school_id, schooljaar, centraal_examen,
                                       centraal_examen_vergelijking, school_examen, eindcijfer)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            school_id,
            item.get("schooljaar"),
            item.get("centraalExamen"),
            item.get("centraalExamenVergelijking"),
            item.get("schoolExamen"),
            item.get("eindcijfer"),
        ])

    # Geslaagden per profiel (VWO only)
    geslaagden = data.get("geslaagdenPerProfiel", {}).get("vwo", [])
    for item in geslaagden:
        con.execute("""
            INSERT INTO geslaagden_per_profiel (school_id, profiel, deelnemers, geslaagden)
            VALUES (?, ?, ?, ?)
        """, [school_id, item.get("profiel"), str(item.get("deelnemers", "")), str(item.get("geslaagden", ""))])

    # Oordeel inspectie
    oordeel = data.get("oordeelInspectie", {})

    if "onderwijspositie" in oordeel:
        item = oordeel["onderwijspositie"]
        con.execute("""
            INSERT INTO oordeel_inspectie (school_id, indicator, inspectienorm, schoolwaarde, periode)
            VALUES (?, ?, ?, ?, ?)
        """, [school_id, "onderwijspositie", item.get("inspectienorm"), item.get("schoolwaarde"), item.get("periode")])

    if "onderbouwsnelheid" in oordeel:
        item = oordeel["onderbouwsnelheid"]
        con.execute("""
            INSERT INTO oordeel_inspectie (school_id, indicator, inspectienorm, schoolwaarde, periode)
            VALUES (?, ?, ?, ?, ?)
        """, [school_id, "onderbouwsnelheid", item.get("inspectienorm"), item.get("schoolwaarde"), item.get("periode")])

    bovenbouwsucces = oordeel.get("bovenbouwsucces", {}).get("vwo")
    if bovenbouwsucces:
        con.execute("""
            INSERT INTO oordeel_inspectie (school_id, indicator, inspectienorm, schoolwaarde, periode)
            VALUES (?, ?, ?, ?, ?)
        """, [school_id, "bovenbouwsucces", bovenbouwsucces.get("inspectienorm"), bovenbouwsucces.get("schoolwaarde"), bovenbouwsucces.get("periode")])


def load_tevredenheid_data(con, school_name_to_id, data):
    """Load tevredenheid data for one school into the database."""
    school_name = data.get("school", {}).get("name", "")

    school_id = school_name_to_id.get(school_name)
    if not school_id:
        print(f"  -> Warning: No matching school found for '{school_name}'")
        return False

    # Load trend data
    trends = data.get("trends", {})
    for metric, items in trends.items():
        for item in items:
            con.execute("""
                INSERT INTO tevredenheid_trend (school_id, metric, schooljaar, cijfer, vergelijking)
                VALUES (?, ?, ?, ?, ?)
            """, [school_id, metric, item.get("schooljaar"), item.get("cijfer"), item.get("vergelijking")])

    # Load question data
    vragen = data.get("vragen", {})
    for respondent, questions in vragen.items():
        for item in questions:
            con.execute("""
                INSERT INTO tevredenheid_vragen (school_id, respondent, vraag, cijfer)
                VALUES (?, ?, ?, ?)
            """, [school_id, respondent, item.get("vraag"), item.get("cijfer")])

    return True


# =============================================================================
# Main Entry Points
# =============================================================================

def parse_html_to_json(base_dir: Path):
    """Parse all HTML files to JSON."""
    html_dir = base_dir / "html"
    json_dir = base_dir / "json"
    json_dir.mkdir(exist_ok=True)

    # Process school result files
    result_html_files = list(html_dir.glob("resultaten-*.html"))
    print(f"Parsing {len(result_html_files)} result HTML files...")

    for html_file in result_html_files:
        try:
            data = parse_school_html(html_file)
            output_name = html_file.stem + ".json"
            output_path = json_dir / output_name

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            print(f"  {html_file.name} -> {output_name}")
        except Exception as e:
            print(f"  Error processing {html_file.name}: {e}")

    # Process tevredenheid files
    tevredenheid_files = list(html_dir.glob("tevredenheid-*.html"))
    print(f"Parsing {len(tevredenheid_files)} tevredenheid HTML files...")

    for html_file in tevredenheid_files:
        try:
            data = parse_tevredenheid_html(html_file)
            output_name = html_file.stem + ".json"
            output_path = json_dir / output_name

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            print(f"  {html_file.name} -> {output_name}")
        except Exception as e:
            print(f"  Error processing {html_file.name}: {e}")


def create_database(base_dir: Path):
    """Create scholen.duckdb from JSON files."""
    db_path = base_dir / "scholen.duckdb"
    json_dir = base_dir / "json"

    # Remove existing database
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))
    create_schema(con)

    # Find all school result JSON files
    json_files = sorted(json_dir.glob("resultaten-*.json"))
    print(f"Loading {len(json_files)} schools into database...")

    school_name_to_id = {}

    for school_id, json_file in enumerate(json_files, start=1):
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        load_school_data(con, school_id, data)
        school_name = data.get("school", {}).get("name", "Unknown")
        school_name_to_id[school_name] = school_id
        print(f"  {school_name}")

    # Load tevredenheid data
    tevredenheid_files = sorted(json_dir.glob("tevredenheid-*.json"))
    print(f"Loading {len(tevredenheid_files)} tevredenheid records...")

    for json_file in tevredenheid_files:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        load_tevredenheid_data(con, school_name_to_id, data)

    # Show summary
    print("\n--- Database Summary ---")
    for table in [
        "schools", "doorstroom_onderbouw", "doorstroom_bovenbouw", "schooladvies",
        "slagingspercentage", "examencijfers", "geslaagden_per_profiel",
        "oordeel_inspectie", "tevredenheid_trend", "tevredenheid_vragen",
    ]:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count} rows")

    con.close()
    print(f"\nDatabase saved to: {db_path}")


def build(base_dir: Path):
    """Full pipeline: HTML -> JSON -> scholen.duckdb"""
    print("=" * 60)
    print("Building scholen.duckdb from HTML files")
    print("=" * 60)

    print("\n[1/2] Parsing HTML to JSON...")
    parse_html_to_json(base_dir)

    print("\n[2/2] Creating database from JSON...")
    create_database(base_dir)

    print("\nDone!")
