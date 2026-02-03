#!/usr/bin/env python3
"""Parse school HTML files and extract data to JSON."""

import json
import re
import html
from pathlib import Path


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
    # Format: <span class=school-adres>Weteringschans 31</span>
    match = re.search(r"<span class=school-adres>([^<]+)</span>", content)
    if match:
        info["address"] = match.group(1).strip()

    # Format: <span class=school-postcode-woonplaats>1017 RV Amsterdam</span>
    match = re.search(r"<span class=school-postcode-woonplaats>([^<]+)</span>", content)
    if match:
        postcode_city = match.group(1).strip()
        # Split into postal code and city (format: "1017 RV Amsterdam")
        # Postal code is always 4 digits + 2 letters
        pc_match = re.match(r"(\d{4}\s*[A-Z]{2})\s+(.*)", postcode_city)
        if pc_match:
            info["postalCode"] = pc_match.group(1)
            info["city"] = pc_match.group(2)
        else:
            # Fallback: store the whole string as city
            info["city"] = postcode_city

    # Extract number of students (leerlingen)
    # Format: <span class=infotip-term data-dfn="Het aantal leerlingen op de school.">797 leerlingen
    match = re.search(r'data-dfn="Het aantal leerlingen op de school\.">(\d+)\s*leerlingen', content)
    if match:
        info["aantalLeerlingen"] = int(match.group(1))

    return info


def extract_doorstroom(content):
    """Extract doorstroom (progression) data."""
    doorstroom = {"onderbouw": {}, "bovenbouw": {}}

    # Find all doorstroom-line-chart elements
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

        # Convert to cleaner format
        cleaned = []
        for item in data:
            cleaned.append(
                {
                    "positie": item.get("positieVergelekenMetSchooladvies", ""),
                    "percentage": item.get("percentage", 0),
                    "vergelijking": item.get("percentageVergelijking", 0),
                }
            )

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

        # Convert to cleaner format
        cleaned = []
        for item in data:
            cleaned.append(
                {
                    "schooljaar": item.get("schooljaar", ""),
                    "percentage": item.get("percentage", 0),
                    "vergelijking": item.get("vergelijking", 0),
                }
            )

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

        # Convert to cleaner format
        cleaned = []
        for item in data:
            cleaned.append(
                {
                    "schooljaar": item.get("schooljaar", ""),
                    "centraalExamen": item.get("centraalExamencijfer", 0),
                    "centraalExamenVergelijking": item.get(
                        "centraalExamencijferVergelijking", 0
                    ),
                    "schoolExamen": item.get("schoolExamencijfer", 0),
                    "eindcijfer": item.get("eindcijfer", 0),
                }
            )

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

    # The HTML structure has level headers like <th>vwo</th> followed by profile rows
    # Split by level headers (standalone <th>havo</th>, <th>vwo</th>, <th>vmbo</th>)
    # Pattern matches <tr><th>level</th>... as level header rows
    sections = re.split(
        r"<tr><th>(havo|vwo|vmbo(?:-\(g\)t)?)</th>", content, flags=re.IGNORECASE
    )

    # Pattern for profile data rows: <tr><td>Profile Name</td><td>N</td><td>N</td>...
    row_pattern = r"<tr><td>([^<]+)</td><td[^>]*>([^<]+)</td><td[^>]*>([^<]+)</td>"

    for i in range(1, len(sections), 2):
        level = sections[i].lower()
        section_content = sections[i + 1] if i + 1 < len(sections) else ""

        # Only get rows until the next level header or end of table
        # Stop at </table> or next <tr><th>
        end_match = re.search(r"</table>|<tr><th>", section_content)
        if end_match:
            section_content = section_content[: end_match.start()]

        rows = re.findall(row_pattern, section_content)
        if rows:
            geslaagden[level] = []
            for profiel, deelnemers, geslaagd in rows:
                profiel = profiel.strip()
                # Filter out non-profile rows
                if (
                    profiel
                    and profiel not in ["Profiel", "Totaal", ""]
                    and any(
                        p in profiel
                        for p in [
                            "Cultuur",
                            "Economie",
                            "Natuur",
                            "Maatschappij",
                            "Techniek",
                            "Gezondheid",
                        ]
                    )
                ):
                    geslaagden[level].append(
                        {
                            "profiel": profiel,
                            "deelnemers": html.unescape(deelnemers.strip()),
                            "geslaagden": html.unescape(geslaagd.strip()),
                        }
                    )

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

    # Find all tevredenheid-vergelijking-line-chart elements with chart-title
    pattern = r'<tevredenheid-vergelijking-line-chart[^>]*periodes="([^"]+)"[^>]*chart-title="([^"]+)"[^>]*>'
    matches = re.findall(pattern, content)

    for periodes_encoded, chart_title in matches:
        data = decode_json_attr(periodes_encoded)
        chart_title_lower = chart_title.lower() if chart_title else ""

        # Determine metric type based on chart title
        metric = None

        # "Hoe tevreden waren de leerlingen de afgelopen jaren op het vwo?"
        if "vwo" in chart_title_lower and "leerlingen" in chart_title_lower:
            metric = "leerlingen"
        # "Hoe tevreden waren de ouders de afgelopen jaren?"
        elif "ouders" in chart_title_lower:
            metric = "ouders"

        if metric:
            trends[metric] = []
            for item in data:
                trends[metric].append(
                    {
                        "schooljaar": item.get("schooljaar", ""),
                        "cijfer": item.get("cijfer"),
                        "vergelijking": item.get("cijferVergelijking"),
                    }
                )

    # Sfeer trends - find chart after "Sfeer in de afgelopen jaren"
    sfeer_section = re.search(
        r'Sfeer in de afgelopen jaren.*?<tevredenheid-vergelijking-line-chart[^>]*periodes="([^"]+)"',
        content,
        re.DOTALL,
    )
    if sfeer_section:
        data = decode_json_attr(sfeer_section.group(1))
        trends["sfeer"] = []
        for item in data:
            trends["sfeer"].append(
                {
                    "schooljaar": item.get("schooljaar", ""),
                    "cijfer": item.get("cijfer"),
                    "vergelijking": item.get("cijferVergelijking"),
                }
            )

    # Veiligheid trends - find chart after "Veiligheid in de afgelopen jaren"
    veiligheid_section = re.search(
        r'Veiligheid in de afgelopen jaren.*?<tevredenheid-vergelijking-line-chart[^>]*periodes="([^"]+)"',
        content,
        re.DOTALL,
    )
    if veiligheid_section:
        data = decode_json_attr(veiligheid_section.group(1))
        trends["veiligheid"] = []
        for item in data:
            trends["veiligheid"].append(
                {
                    "schooljaar": item.get("schooljaar", ""),
                    "cijfer": item.get("cijfer"),
                    "vergelijking": item.get("cijferVergelijking"),
                }
            )

    return trends


def extract_tevredenheid_vragen(content):
    """Extract satisfaction questions and scores (VWO students + parents)."""
    vragen = {"leerling": [], "ouder": []}

    # Extract student questions (VWO section only)
    # Find the dialog with "Hoe tevreden zijn de leerlingen in 2023-2024, uitgesplitst per vraag?"
    leerling_dialog = re.search(
        r"Hoe tevreden zijn de leerlingen in \d{4}-\d{4}, uitgesplitst per vraag\?.*?</dialog>",
        content,
        re.DOTALL,
    )

    if leerling_dialog:
        dialog_content = leerling_dialog.group(0)

        # Find the VWO section - it's after <h4>vwo</h4>
        vwo_section = re.search(
            r"<h4>vwo</h4>(.*?)(?:</section>|</dialog>)", dialog_content, re.DOTALL
        )
        if vwo_section:
            section_content = vwo_section.group(1)

            # Extract questions from tables
            # Pattern: <tr><td>Question</td><td class=numeric ...>Score</td>...
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
    # Find the dialog with "Hoe tevreden zijn de ouders in 2023-2024, uitgesplitst per vraag?"
    ouder_dialog = re.search(
        r"Hoe tevreden zijn de ouders in \d{4}-\d{4}, uitgesplitst per vraag\?.*?</dialog>",
        content,
        re.DOTALL,
    )

    if ouder_dialog:
        dialog_content = ouder_dialog.group(0)

        # Extract questions from tables (no VWO filter needed for parents)
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


def parse_tevredenheid_html(filepath):
    """Parse a tevredenheid HTML file and return structured data."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Extract school name from title
    info = {}
    match = re.search(r"<title>Tevredenheid - ([^(]+)\s*\(", content)
    if match:
        info["name"] = match.group(1).strip()

    return {
        "school": info,
        "trends": extract_tevredenheid_trends(content),
        "vragen": extract_tevredenheid_vragen(content),
    }


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


def main():
    base_dir = Path(".")
    html_dir = base_dir / "html"
    json_dir = base_dir / "json"

    # Process school result files
    result_html_files = [f for f in html_dir.glob("resultaten-*.html")]

    for html_file in result_html_files:
        print(f"Processing: {html_file.name}")
        try:
            data = parse_school_html(html_file)

            # Create output filename
            output_name = html_file.stem + ".json"
            output_path = json_dir / output_name

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            print(f"  -> Saved: {output_name}")
            print(f"     School: {data['school'].get('name', 'Unknown')}")
        except Exception as e:
            print(f"  -> Error: {e}")

    # Process tevredenheid files
    tevredenheid_files = list(html_dir.glob("tevredenheid-*.html"))

    for html_file in tevredenheid_files:
        print(f"Processing: {html_file.name}")
        try:
            data = parse_tevredenheid_html(html_file)

            # Create output filename
            output_name = html_file.stem + ".json"
            output_path = json_dir / output_name

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            print(f"  -> Saved: {output_name}")
            print(f"     School: {data['school'].get('name', 'Unknown')}")
        except Exception as e:
            print(f"  -> Error: {e}")


if __name__ == "__main__":
    main()
