import marimo

__generated_with = "0.19.7"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import altair as alt
    import polars as pl
    import duckdb
    db = duckdb.connect()
    return alt, db, mo, pl


@app.cell
def _(db):
    # Create a shared connection with both databases attached
    _ = db.execute("ATTACH 'scholen.duckdb' AS scholen_db (READ_ONLY)")
    _ = db.execute("ATTACH 'loting_matching.duckdb' AS loting_db (READ_ONLY)")
    return


@app.cell
def _():
    SCHOOL_MAPPING = {
        "Ignatiusgymnasium": "Sint Ignatiusgymnasium",
        "Alasca": "ALASCA",
        "Damstede": "Damstede Lyceum",
        "Geert Groote College": "Geert Groote College Amsterdam",
        "HLZ (Hervormd Lyceum Zuid)": "HLZ",
        "Hervormd Lyceum West": "Het Hervormd Lyceum West",
        "Cartesius Amsterdam": "Het Cartesius Lyceum",
        "Metis Montessori Lyceum": "Metis Montessori Lyceum . vwo . havo",
        "Montessori Lyceum Amsterdam": "Montessori Lyceum Amsterdam - Hoofdlocatie",
        "OSB": "OSB Amsterdam",
        "Spinoza Lyceum": "Spinoza Lyceum Amsterdam",
        "Xplore": "Xplore - Agora Amsterdam",
        "Cornelius Haga Lyceum": "Cornelius Haga Lyceum, Islamitische Scholengemeenschap voor mavo havo en vwo",
    }
    SCHOOL_MAPPING_REVERSE = {v: k for k, v in SCHOOL_MAPPING.items()}

    SCHOOL_COORDS = {
        "Alasca": (52.3947, 4.9183),
        "Barlaeus Gymnasium": (52.3545, 4.8809),
        "Berlage Lyceum": (52.3489, 4.9283),
        "Calandlyceum": (52.3750, 4.8300),
        "Cartesius Amsterdam": (52.3867, 4.8550),
        "Comenius Lyceum Amsterdam": (52.3633, 4.8117),
        "Cornelius Haga Lyceum": (52.3583, 4.8167),
        "Cygnus Gymnasium": (52.3617, 4.9383),
        "DENISE": (52.3676, 4.8936),
        "Damstede": (52.3700, 4.9483),
        "Fons Vitae Lyceum": (52.3483, 4.8700),
        "Geert Groote College": (52.3450, 4.8617),
        "Gerrit van der Veen College": (52.3533, 4.9267),
        "HLZ (Hervormd Lyceum Zuid)": (52.3367, 4.8817),
        "Hervormd Lyceum West": (52.3833, 4.8417),
        "Het 4e Gymnasium": (52.3550, 4.8650),
        "Het Amsterdams Lyceum": (52.3517, 4.8633),
        "Hyperion Lyceum": (52.3400, 4.8783),
        "Ignatiusgymnasium": (52.3500, 4.8833),
        "Ir. Lely Lyceum": (52.3767, 4.9417),
        "Kairos Tienercollege": (52.3017, 4.8517),
        "Lumion": (52.3683, 4.8033),
        "Marcanti College": (52.3733, 4.8367),
        "Metis Montessori Lyceum": (52.3567, 4.9267),
        "Metropolis Lyceum": (52.3933, 4.8750),
        "Montessori Lyceum Amsterdam": (52.3567, 4.8633),
        "Montessori Lyceum Pax": (52.3733, 4.8183),
        "Montessori Lyceum Terra Nova": (52.3167, 4.9567),
        "OSB": (52.3133, 4.9500),
        "Pieter Nieuwland College": (52.3617, 4.9267),
        "Spinoza Lyceum": (52.3633, 4.9450),
        "St. Nicolaaslyceum": (52.3433, 4.8833),
        "Vinse School": (52.3850, 4.8700),
        "Vossius Gymnasium": (52.3550, 4.8717),
        "Xplore": (52.3317, 4.9383),
    }
    return SCHOOL_COORDS, SCHOOL_MAPPING


@app.cell
def _(mo):
    import json
    from pathlib import Path

    # File for persisting hidden schools
    _hidden_file = Path("hidden_schools.json")

    # Load hidden schools from file
    def _load_hidden():
        if _hidden_file.exists():
            try:
                return set(json.loads(_hidden_file.read_text()))
            except (json.JSONDecodeError, TypeError):
                return set()
        return set()

    # Save hidden schools to file
    def _save_hidden(hidden_set):
        _hidden_file.write_text(json.dumps(list(hidden_set)))

    _initial_hidden = _load_hidden()

    my_list_state, set_my_list = mo.state([])
    selected_school_state, set_selected_school = mo.state(None)  # (afdeling_id, school_name)
    active_tab_state, set_active_tab = mo.state("Overzicht")
    hidden_schools_state, _set_hidden_schools_raw = mo.state(_initial_hidden)
    show_hidden_state, set_show_hidden = mo.state(False)

    # Wrapper to also persist when setting hidden schools
    def set_hidden_schools(new_hidden):
        _save_hidden(new_hidden)
        _set_hidden_schools_raw(new_hidden)

    return (
        active_tab_state, hidden_schools_state, my_list_state, selected_school_state,
        set_active_tab, set_hidden_schools, set_my_list, set_selected_school, set_show_hidden, show_hidden_state
    )


@app.cell
def _(db, mo, pl):
    # Fetch current year stats only for overview
    _latest_stats = pl.from_arrow(db.execute("""
        SELECT totaal_deelnemers, totaal_capaciteit, percentage_eerste_voorkeur, percentage_top3
        FROM loting_db.jaar_samenvatting
        WHERE jaar = 2025
    """).fetch_arrow_table())

    _latest = _latest_stats.to_dicts()[0] if not _latest_stats.is_empty() else {}

    _overview_stats = mo.hstack([
        mo.stat(
            value=f"{_latest.get('percentage_eerste_voorkeur', 0):.1f}%",
            label="Eerste voorkeur",
            caption="krijgt 1e keuze",
            bordered=True,
        ),
        mo.stat(
            value=f"{_latest.get('percentage_top3', 0):.1f}%",
            label="Top 3",
            caption="krijgt top-3 keuze",
            bordered=True,
        ),
        mo.stat(
            value=str(_latest.get('totaal_deelnemers', 0)),
            label="Deelnemers",
            caption="leerlingen",
            bordered=True,
        ),
        mo.stat(
            value=str(_latest.get('totaal_capaciteit', 0)),
            label="Capaciteit",
            caption="plekken",
            bordered=True,
        ),
    ], justify="start", gap=2)

    overview_content = mo.vstack([
        mo.md("""
    ## Welkom

    Dit dashboard helpt je bij het samenstellen van je voorkeurslijst voor de **Centrale Loting & Matching** van Amsterdamse VWO-scholen.

    **Gebruik de tabs hieronder om:**
    - Scholen te verkennen op populariteit en kwaliteit
    - Gedetailleerde schoolinformatie te bekijken
    - Je voorkeurslijst samen te stellen

    ---

    ### Hoe werkt de loting?

    1. Elke leerling krijgt een **willekeurig lotnummer**
    2. Leerlingen worden op volgorde verwerkt
    3. Je wordt geplaatst op de **hoogst gerankte school** met plek

    **Tips:** Zet droomscholen bovenaan, vul alle 12 slots, neem "veilige" scholen op (ratio < 1.0).
        """),
        mo.md("### Loting 2025"),
        _overview_stats,
    ])
    return (overview_content,)


@app.cell
def _(db, mo, pl):
    # Fetch stadsdelen for dropdown (no display needed)
    _stadsdeel_data = pl.from_arrow(db.execute(
        "SELECT id, naam FROM loting_db.stadsdeel ORDER BY naam"
    ).fetch_arrow_table())
    _stadsdeel_options = {"Alle stadsdelen": None}
    for _row in _stadsdeel_data.to_dicts():
        _stadsdeel_options[_row['naam']] = _row['id']

    stadsdeel_filter = mo.ui.dropdown(
        options=_stadsdeel_options,
        value="Alle stadsdelen",
        label="Stadsdeel"
    )
    type_filter = mo.ui.dropdown(
        options={"Alle types": None, "Breed": "Breed", "Categoraal": "Categoraal"},
        value="Alle types",
        label="Schooltype"
    )
    ratio_filter = mo.ui.range_slider(
        start=0, stop=3, step=0.1, value=[0, 3],
        label="Populariteitsratio"
    )
    return ratio_filter, stadsdeel_filter, type_filter


@app.cell
def _(
    SCHOOL_MAPPING,
    db,
    hidden_schools_state,
    mo,
    my_list_state,
    pl,
    ratio_filter,
    set_active_tab,
    set_selected_school,
    show_hidden_state,
    stadsdeel_filter,
    type_filter,
):
    _stadsdeel_clause = f"AND ls.stadsdeel_id = {stadsdeel_filter.value}" if stadsdeel_filter.value else ""
    _type_clause = f"AND ls.type = '{type_filter.value}'" if type_filter.value else ""

    # Fetch filtered schools (no display - we show via explorer_table)
    _filtered = pl.from_arrow(db.execute(f"""
        SELECT
            a.id as afdeling_id, ls.naam as school, a.naam as afdeling, a.variant,
            ls.type, sd.naam as stadsdeel, c.definitieve_capaciteit as capaciteit,
            v.eerste_voorkeur,
            ROUND(CAST(v.eerste_voorkeur AS FLOAT) / NULLIF(c.definitieve_capaciteit, 0), 2) as ratio
        FROM loting_db.afdeling a
        JOIN loting_db.loting_school ls ON a.school_id = ls.id
        JOIN loting_db.stadsdeel sd ON ls.stadsdeel_id = sd.id
        LEFT JOIN loting_db.capaciteit c ON a.id = c.afdeling_id AND c.jaar = 2025
        LEFT JOIN loting_db.voorkeuren v ON a.id = v.afdeling_id AND v.jaar = 2025
        WHERE a.onderwijsniveau_id = 1 {_stadsdeel_clause} {_type_clause}
        ORDER BY ratio DESC NULLS LAST
    """).fetch_arrow_table())

    # Fetch quality data for all schools (most recent year)
    _quality = pl.from_arrow(db.execute("""
        SELECT
            s.name,
            e.eindcijfer,
            e.centraal_examen as ce,
            e.centraal_examen_vergelijking as ce_land,
            sp.percentage as slaag_pct,
            sp.vergelijking as slaag_land,
            MAX(CASE WHEN t.metric = 'leerlingen' THEN t.cijfer END) as tevr_leerlingen,
            MAX(CASE WHEN t.metric = 'ouders' THEN t.cijfer END) as tevr_ouders,
            MAX(CASE WHEN t.metric = 'sfeer' THEN t.cijfer END) as tevr_sfeer,
            MAX(CASE WHEN t.metric = 'veiligheid' THEN t.cijfer END) as tevr_veiligheid
        FROM scholen_db.schools s
        LEFT JOIN (
            SELECT school_id, eindcijfer, centraal_examen, centraal_examen_vergelijking,
                   ROW_NUMBER() OVER (PARTITION BY school_id ORDER BY schooljaar DESC) as rn
            FROM scholen_db.examencijfers
        ) e ON s.id = e.school_id AND e.rn = 1
        LEFT JOIN (
            SELECT school_id, percentage, vergelijking,
                   ROW_NUMBER() OVER (PARTITION BY school_id ORDER BY schooljaar DESC) as rn
            FROM scholen_db.slagingspercentage
        ) sp ON s.id = sp.school_id AND sp.rn = 1
        LEFT JOIN (
            SELECT school_id, metric, cijfer,
                   ROW_NUMBER() OVER (PARTITION BY school_id, metric ORDER BY schooljaar DESC) as rn
            FROM scholen_db.tevredenheid_trend
        ) t ON s.id = t.school_id AND t.rn = 1
        GROUP BY s.name, e.eindcijfer, e.centraal_examen, e.centraal_examen_vergelijking, sp.percentage, sp.vergelijking
    """).fetch_arrow_table())

    # Create lookup dict for quality data
    _quality_lookup = {row['name']: row for row in _quality.to_dicts()}

    _min_ratio, _max_ratio = ratio_filter.value
    _hidden = hidden_schools_state()
    _show_hidden = show_hidden_state()

    schools_list = []
    school_buttons = {}  # Map school name to button for clicking

    for _row in _filtered.to_dicts():
        _afdeling_id = _row['afdeling_id']
        _is_hidden = _afdeling_id in _hidden

        # Skip hidden schools unless show_hidden is enabled
        if _is_hidden and not _show_hidden:
            continue

        _ratio = _row.get('ratio') or 0
        if _ratio < _min_ratio or _ratio > _max_ratio:
            continue
        _in_list = any(item['afdeling_id'] == _afdeling_id for item in my_list_state())
        if _ratio > 1.0:
            _ratio_display = f"🔴 {_ratio:.2f}"
        elif _ratio >= 0.7:
            _ratio_display = f"🟡 {_ratio:.2f}"
        else:
            _ratio_display = f"🟢 {_ratio:.2f}"

        # Get quality data using school name mapping
        _school_name = _row['school']
        _quality_name = SCHOOL_MAPPING.get(_school_name, _school_name)
        _q = _quality_lookup.get(_quality_name, {})

        # Format CE with landelijk baseline in parentheses
        _ce = _q.get('ce')
        _ce_land = _q.get('ce_land')
        _ce_display = f"{_ce:.1f} (land: {_ce_land:.1f})" if _ce and _ce_land else (f"{_ce:.1f}" if _ce else "-")

        # Format slagingspercentage with landelijk baseline
        _slaag = _q.get('slaag_pct')
        _slaag_land = _q.get('slaag_land')
        _slaag_display = f"{_slaag:.0f}% (land: {_slaag_land:.0f}%)" if _slaag and _slaag_land else (f"{_slaag:.0f}%" if _slaag else "-")

        # Create clickable button for school name (show hidden indicator if hidden)
        def _make_click_handler(aid, sname):
            def _handler(_):
                set_selected_school((aid, sname))
                set_active_tab("School Details")
            return _handler

        # Include variant in display name if it exists
        _variant = _row.get('variant')
        _display_name = f"{_school_name} ({_variant})" if _variant else _school_name
        _label = f"👁️‍🗨️ {_display_name}" if _is_hidden else _display_name
        _btn = mo.ui.button(
            label=_label,
            on_click=_make_click_handler(_afdeling_id, _school_name),
            kind="neutral",
        )
        school_buttons[_display_name] = _btn

        schools_list.append({
            "School": _btn,
            "Capaciteit": _row.get('capaciteit') or 0,
            "1e Voorkeur": _row.get('eerste_voorkeur') or 0,
            "Ratio": _ratio_display,
            "Eindcijfer": f"{_q.get('eindcijfer'):.1f}" if _q.get('eindcijfer') else "-",
            "CE": _ce_display,
            "Slaag%": _slaag_display,
            "Leerl.": f"{_q.get('tevr_leerlingen'):.1f}" if _q.get('tevr_leerlingen') else "-",
            "Ouders": f"{_q.get('tevr_ouders'):.1f}" if _q.get('tevr_ouders') else "-",
            "Sfeer": f"{_q.get('tevr_sfeer'):.1f}" if _q.get('tevr_sfeer') else "-",
            "Veilig": f"{_q.get('tevr_veiligheid'):.1f}" if _q.get('tevr_veiligheid') else "-",
            "In Lijst": "✓" if _in_list else "",
            "_afdeling_id": _afdeling_id,
            "_ratio": _ratio,
            "_stadsdeel": _row['stadsdeel'],
            "_variant": _row.get('variant') or "Regulier",
            "_school_name": _school_name,
            "_is_hidden": _is_hidden,
        })

    # For the table display, we need to use the buttons directly
    explorer_table = mo.ui.table(
        schools_list,
        selection="multi", page_size=15, label="Selecteer scholen om toe te voegen",
        show_column_summaries=False,
    ) if schools_list else mo.md("*Geen scholen gevonden*")

    # Count hidden schools
    hidden_count = len(_hidden)

    return explorer_table, hidden_count, school_buttons, schools_list


@app.cell
def _(SCHOOL_COORDS, mo, my_list_state, schools_list):
    import folium
    _map = folium.Map(location=[52.36, 4.89], zoom_start=12)
    _my_schools = {item['school'] for item in my_list_state()}
    for _school in schools_list:
        _name = _school['_school_name']
        _coords = SCHOOL_COORDS.get(_name)
        if not _coords:
            continue
        _ratio = _school['_ratio']
        _in_list = _name in _my_schools
        _color = 'red' if _ratio > 1.0 else ('orange' if _ratio >= 0.7 else 'green')
        _icon = folium.Icon(color=_color, icon='star' if _in_list else 'info-sign')
        _popup = f"<b>{_name}</b><br>Ratio: {_ratio:.2f}<br>{'⭐ In je lijst' if _in_list else ''}"
        folium.Marker(location=_coords, popup=folium.Popup(_popup, max_width=200), icon=_icon, tooltip=_name).add_to(_map)
    explorer_map = mo.Html(_map._repr_html_())
    return (explorer_map,)


@app.cell
def _(explorer_table, hidden_schools_state, mo, my_list_state, schools_list, set_hidden_schools, set_my_list, show_hidden_state):
    # Create lookup by afdeling_id for quick access
    _schools_by_id = {s['_afdeling_id']: s for s in schools_list}

    def _add_selected():
        if explorer_table.value is None or len(explorer_table.value) == 0:
            return
        _current = list(my_list_state())
        if len(_current) >= 12:
            return
        for _sel in explorer_table.value:
            _afdeling_id = _sel['_afdeling_id']
            _row = _schools_by_id.get(_afdeling_id)
            if _row and not any(item['afdeling_id'] == _afdeling_id for item in _current):
                _current.append({
                    'afdeling_id': _afdeling_id,
                    'school': _row['_school_name'],
                    'variant': _row['_variant'],
                    'stadsdeel': _row['_stadsdeel'],
                    'ratio': _row['_ratio'],
                })
                if len(_current) >= 12:
                    break
        set_my_list(_current)

    def _hide_selected():
        if explorer_table.value is None or len(explorer_table.value) == 0:
            return
        _hidden = set(hidden_schools_state())
        for _sel in explorer_table.value:
            _hidden.add(_sel['_afdeling_id'])
        set_hidden_schools(_hidden)

    def _unhide_selected():
        if explorer_table.value is None or len(explorer_table.value) == 0:
            return
        _hidden = set(hidden_schools_state())
        for _sel in explorer_table.value:
            _hidden.discard(_sel['_afdeling_id'])
        set_hidden_schools(_hidden)

    add_button = mo.ui.button(
        label=f"Voeg toe aan lijst ({len(my_list_state())}/12)",
        on_click=lambda _: _add_selected(),
        disabled=len(my_list_state()) >= 12
    )

    hide_button = mo.ui.button(
        label="Verberg selectie",
        on_click=lambda _: _hide_selected(),
        kind="neutral",
    )

    unhide_button = mo.ui.button(
        label="Toon selectie weer",
        on_click=lambda _: _unhide_selected(),
        kind="neutral",
    )

    list_status = mo.md(f"**{len(my_list_state())}** scholen in je lijst") if my_list_state() else mo.md("*Nog geen scholen*")

    # Show unhide button only when viewing hidden schools
    _show_hidden = show_hidden_state()
    visibility_button = unhide_button if _show_hidden else hide_button

    return add_button, hide_button, list_status, unhide_button, visibility_button


@app.cell
def _(
    add_button,
    explorer_map,
    explorer_table,
    hidden_count,
    list_status,
    mo,
    ratio_filter,
    set_show_hidden,
    show_hidden_state,
    stadsdeel_filter,
    type_filter,
    visibility_button,
):
    # Checkbox to toggle showing hidden schools
    show_hidden_checkbox = mo.ui.checkbox(
        label=f"Toon verborgen scholen ({hidden_count})",
        value=show_hidden_state(),
        on_change=set_show_hidden,
    )

    explorer_content = mo.vstack([
        mo.md("""## Scholen Verkenner

Verken alle VWO-scholen op populariteit en kwaliteit. 🔴 = populair (ratio > 1), 🟡 = gemiddeld, 🟢 = minder populair.

**Kwaliteitskolommen:** CE en Slaag% tonen landelijk gemiddelde tussen haakjes. Tevredenheid: Leerl./Ouders/Sfeer/Veilig (schaal 1-10)."""),
        mo.hstack([stadsdeel_filter, type_filter, ratio_filter], gap=2),
        mo.hstack([add_button, visibility_button, show_hidden_checkbox, list_status], gap=2),
        explorer_map,
        explorer_table,
    ])
    return (explorer_content,)


@app.cell
def _(mo, my_list_state, pl, set_my_list):
    _current_list = my_list_state()

    if not _current_list:
        list_table = mo.callout("Je lijst is nog leeg. Ga naar 'Scholen Verkenner' om scholen toe te voegen.", kind="info")
        list_controls = mo.md("")
        list_analysis = mo.md("")
    else:
        _list_data = []
        for _idx, _item in enumerate(_current_list):
            _ratio = _item.get('ratio', 0) or 0
            _tier = "🔴 Droom" if _ratio > 1.0 else ("🟡 Competitief" if _ratio >= 0.7 else "🟢 Veilig")
            _list_data.append({
                "Pos": _idx + 1, "School": _item['school'], "Variant": _item.get('variant', 'Regulier'),
                "Stadsdeel": _item['stadsdeel'], "Ratio": f"{_ratio:.2f}", "Tier": _tier,
            })
        _list_df = pl.DataFrame(_list_data)
        list_table = mo.ui.table(_list_df, selection="multi", page_size=12)

        def _move_up():
            if list_table.value is not None and len(list_table.value) == 1:
                _pos = list_table.value.to_dicts()[0]['Pos'] - 1
                if _pos > 0:
                    _new = list(_current_list)
                    _new[_pos], _new[_pos-1] = _new[_pos-1], _new[_pos]
                    set_my_list(_new)

        def _move_down():
            if list_table.value is not None and len(list_table.value) == 1:
                _pos = list_table.value.to_dicts()[0]['Pos'] - 1
                if _pos < len(_current_list) - 1:
                    _new = list(_current_list)
                    _new[_pos], _new[_pos+1] = _new[_pos+1], _new[_pos]
                    set_my_list(_new)

        def _remove():
            if list_table.value is not None and len(list_table.value) > 0:
                _positions = {r['Pos'] - 1 for r in list_table.value.to_dicts()}
                set_my_list([item for i, item in enumerate(_current_list) if i not in _positions])

        list_controls = mo.hstack([
            mo.ui.button(label="⬆️ Omhoog", on_click=lambda _: _move_up()),
            mo.ui.button(label="⬇️ Omlaag", on_click=lambda _: _move_down()),
            mo.ui.button(label="🗑️ Verwijderen", on_click=lambda _: _remove()),
        ], gap=1)

        _dream = sum(1 for item in _current_list if (item.get('ratio') or 0) > 1.0)
        _comp = sum(1 for item in _current_list if 0.7 <= (item.get('ratio') or 0) <= 1.0)
        _safe = sum(1 for item in _current_list if (item.get('ratio') or 0) < 0.7)
        if _safe == 0 and len(_current_list) > 3:
            _risk = mo.callout("⚠️ **Geen veilige scholen!** Voeg scholen toe met ratio < 0.7", kind="warn")
        elif _dream > len(_current_list) * 0.7:
            _risk = mo.callout("⚠️ **Risicovolle lijst** - veel populaire scholen", kind="warn")
        else:
            _risk = mo.callout("✅ **Gebalanceerde lijst**", kind="success")
        list_analysis = mo.vstack([
            mo.md(f"**Samenstelling:** 🔴 Droom: {_dream} | 🟡 Competitief: {_comp} | 🟢 Veilig: {_safe}"),
            _risk,
        ])
    return list_analysis, list_controls, list_table


@app.cell
def _(list_analysis, list_controls, list_table, mo):
    list_content = mo.vstack([
        mo.md("## Mijn Voorkeurslijst\n\nBouw je lijst van maximaal 12 scholen. Volgorde is belangrijk!"),
        list_table,
        list_controls,
        list_analysis,
    ])
    return (list_content,)


@app.cell
def _(db, mo, pl, selected_school_state, set_selected_school):
    # Fetch all schools for dropdown
    _all_schools = pl.from_arrow(db.execute("""
        SELECT a.id, ls.naam || ' - ' || COALESCE(a.variant, 'VWO') as display_name, ls.naam as school
        FROM loting_db.loting_school ls
        JOIN loting_db.afdeling a ON ls.id = a.school_id
        WHERE a.onderwijsniveau_id = 1
        ORDER BY ls.naam, a.variant
    """).fetch_arrow_table())
    _options = {_row['display_name']: (_row['id'], _row['school']) for _row in _all_schools.to_dicts()}
    _options_list = list(_options.keys())

    # Determine initial value based on selected_school_state
    _initial_value = _options_list[0] if _options_list else None
    if selected_school_state():
        _sel_id, _sel_name = selected_school_state()
        # Find matching dropdown option
        for _disp, (_aid, _sname) in _options.items():
            if _aid == _sel_id:
                _initial_value = _disp
                break

    def _on_dropdown_change(value):
        if value and value in _options:
            _aid, _sname = _options[value]
            set_selected_school((_aid, _sname))

    school_dropdown = mo.ui.dropdown(
        options=_options_list,
        label="Selecteer school",
        value=_initial_value,
        on_change=_on_dropdown_change
    )
    detail_school_options = _options
    return detail_school_options, school_dropdown


@app.cell
def _(SCHOOL_MAPPING, db, detail_school_options, mo, pl, school_dropdown, selected_school_state, set_active_tab):
    # Back button to return to explorer
    back_button = mo.ui.button(
        label="← Terug naar Verkenner",
        on_click=lambda _: set_active_tab("Scholen Verkenner"),
        kind="neutral",
    )

    # Determine which school to show - prefer state, fall back to dropdown
    _selected_id = None
    _selected_name = None

    if selected_school_state():
        _selected_id, _selected_name = selected_school_state()
    elif school_dropdown.value and school_dropdown.value in detail_school_options:
        _selected_id, _selected_name = detail_school_options[school_dropdown.value]

    if _selected_id is None:
        detail_content = mo.vstack([
            back_button,
            mo.md("## School Details"),
            school_dropdown,
            mo.md("*Selecteer een school uit de dropdown of klik op een school in de Verkenner*")
        ])
    else:
        _quality_name = SCHOOL_MAPPING.get(_selected_name, _selected_name)

        # Get lottery info with preference breakdown (1st-5th)
        _loting = pl.from_arrow(db.execute(f"""
            SELECT ls.naam as school, a.naam as afdeling, a.variant, ls.type, sd.naam as stadsdeel,
                   c.definitieve_capaciteit as capaciteit,
                   v.eerste_voorkeur, v.tweede_voorkeur, v.derde_voorkeur,
                   ROUND(CAST(v.eerste_voorkeur AS FLOAT) / NULLIF(c.definitieve_capaciteit, 0), 2) as ratio
            FROM loting_db.afdeling a
            JOIN loting_db.loting_school ls ON a.school_id = ls.id
            JOIN loting_db.stadsdeel sd ON ls.stadsdeel_id = sd.id
            LEFT JOIN loting_db.capaciteit c ON a.id = c.afdeling_id AND c.jaar = 2025
            LEFT JOIN loting_db.voorkeuren v ON a.id = v.afdeling_id AND v.jaar = 2025
            WHERE a.id = {_selected_id}
        """).fetch_arrow_table())

        _sections = []
        if not _loting.is_empty():
            _info = _loting.to_dicts()[0]
            _ratio = _info.get('ratio') or 0
            _sections.append(mo.md(f"## {_info['school']}"))
            _sections.append(mo.md(f"**Type:** {_info['type']} | **Stadsdeel:** {_info['stadsdeel']} | **Variant:** {_info.get('variant') or 'Regulier'}"))
            _sections.append(mo.md("### Loting & Matching (2025)"))
            _sections.append(mo.hstack([
                mo.stat(value=str(_info.get('capaciteit') or 0), label="Capaciteit", bordered=True),
                mo.stat(value=f"{_ratio:.2f}", label="Populariteit", bordered=True),
            ], gap=2))

            # Preference breakdown
            _ev1 = _info.get('eerste_voorkeur') or 0
            _ev2 = _info.get('tweede_voorkeur') or 0
            _ev3 = _info.get('derde_voorkeur') or 0
            _sections.append(mo.md("#### Voorkeuren verdeling"))
            _sections.append(mo.md("*Hoeveel leerlingen hadden deze school op welke positie in hun voorkeurslijst?*"))
            _sections.append(mo.hstack([
                mo.stat(value=str(_ev1), label="1e Voorkeur", bordered=True),
                mo.stat(value=str(_ev2), label="2e Voorkeur", bordered=True),
                mo.stat(value=str(_ev3), label="3e Voorkeur", bordered=True),
            ], gap=2))

        # Get exam results
        _exams = pl.from_arrow(db.execute(f"""
            SELECT e.schooljaar, ROUND(e.centraal_examen, 2) as ce, ROUND(e.centraal_examen_vergelijking, 2) as ce_land,
                   ROUND(e.eindcijfer, 2) as eindcijfer
            FROM scholen_db.schools s
            JOIN scholen_db.examencijfers e ON s.id = e.school_id
            WHERE s.name = '{_quality_name}'
            ORDER BY e.schooljaar DESC
        """).fetch_arrow_table())
        if not _exams.is_empty():
            _ex = _exams.to_dicts()[0]
            _ce = _ex.get('ce') or 0
            _ce_land = _ex.get('ce_land') or 0
            _diff = _ce - _ce_land if _ce and _ce_land else 0
            _sections.append(mo.md("---"))
            _sections.append(mo.md("### Examencijfers VWO"))
            _sections.append(mo.hstack([
                mo.stat(value=f"{_ex.get('eindcijfer', 0):.1f}", label="Eindcijfer", caption=_ex.get('schooljaar', ''), bordered=True),
                mo.stat(value=f"{_ce:.2f}", label="Centraal Examen", caption=f"landelijk: {_ce_land:.2f}", bordered=True),
                mo.stat(value=f"{'+' if _diff >= 0 else ''}{_diff:.2f}", label="vs Landelijk", bordered=True),
            ], gap=2))
            _sections.append(mo.ui.table(_exams, page_size=5))

        # Get pass rates
        _pass = pl.from_arrow(db.execute(f"""
            SELECT sp.schooljaar, ROUND(sp.percentage, 1) as pct, ROUND(sp.vergelijking, 1) as land
            FROM scholen_db.schools s
            JOIN scholen_db.slagingspercentage sp ON s.id = sp.school_id
            WHERE s.name = '{_quality_name}'
            ORDER BY sp.schooljaar DESC LIMIT 1
        """).fetch_arrow_table())
        if not _pass.is_empty():
            _p = _pass.to_dicts()[0]
            _sections.append(mo.md("### Slagingspercentage"))
            _sections.append(mo.hstack([
                mo.stat(value=f"{_p.get('pct', 0):.0f}%", label="Geslaagd", caption=_p.get('schooljaar', ''), bordered=True),
                mo.stat(value=f"{_p.get('land', 0):.0f}%", label="Landelijk", bordered=True),
            ], gap=2))

        # Get satisfaction summary
        _sat = pl.from_arrow(db.execute(f"""
            SELECT t.metric, ROUND(t.cijfer, 1) as cijfer
            FROM scholen_db.schools s
            JOIN scholen_db.tevredenheid_trend t ON s.id = t.school_id
            WHERE s.name = '{_quality_name}'
            ORDER BY t.schooljaar DESC
        """).fetch_arrow_table())
        if not _sat.is_empty():
            _metrics = {}
            for _r in _sat.to_dicts():
                if _r['metric'] not in _metrics:
                    _metrics[_r['metric']] = _r['cijfer']
            _labels = {'leerlingen': 'Leerlingen', 'ouders': 'Ouders', 'sfeer': 'Sfeer', 'veiligheid': 'Veiligheid'}
            _stats = [mo.stat(value=f"{_metrics[m]:.1f}", label=l, bordered=True) for m, l in _labels.items() if m in _metrics and _metrics[m]]
            if _stats:
                _sections.append(mo.md("---"))
                _sections.append(mo.md("### Tevredenheid (Samenvatting)"))
                _sections.append(mo.hstack(_stats, gap=2))

        # Get detailed satisfaction questions from tevredenheid_vragen
        _tevr_vragen = pl.from_arrow(db.execute(f"""
            SELECT tv.respondent, tv.vraag, ROUND(tv.cijfer, 1) as cijfer
            FROM scholen_db.schools s
            JOIN scholen_db.tevredenheid_vragen tv ON s.id = tv.school_id
            WHERE s.name = '{_quality_name}'
            ORDER BY tv.respondent, tv.cijfer DESC
        """).fetch_arrow_table())
        if not _tevr_vragen.is_empty():
            _sections.append(mo.md("#### Tevredenheid: Gedetailleerde vragen"))

            # Split by respondent type
            _leerling_vragen = _tevr_vragen.filter(pl.col("respondent") == "leerling")
            _ouder_vragen = _tevr_vragen.filter(pl.col("respondent") == "ouder")

            _tevr_tabs = {}
            if not _leerling_vragen.is_empty():
                _tevr_tabs["Leerlingen"] = mo.ui.table(
                    _leerling_vragen.select(["vraag", "cijfer"]).rename({"vraag": "Vraag", "cijfer": "Cijfer"}),
                    page_size=10, show_column_summaries=False
                )
            if not _ouder_vragen.is_empty():
                _tevr_tabs["Ouders"] = mo.ui.table(
                    _ouder_vragen.select(["vraag", "cijfer"]).rename({"vraag": "Vraag", "cijfer": "Cijfer"}),
                    page_size=10, show_column_summaries=False
                )
            if _tevr_tabs:
                _sections.append(mo.ui.tabs(_tevr_tabs))

        # Get inspection indicators
        _insp = pl.from_arrow(db.execute(f"""
            SELECT o.indicator, ROUND(o.schoolwaarde, 2) as waarde, ROUND(o.inspectienorm, 2) as norm
            FROM scholen_db.schools s
            JOIN scholen_db.oordeel_inspectie o ON s.id = o.school_id
            WHERE s.name = '{_quality_name}'
        """).fetch_arrow_table())
        if not _insp.is_empty():
            _ind_labels = {'onderwijspositie': 'Onderwijspositie', 'onderbouwsnelheid': 'Onderbouwsnelheid', 'bovenbouwsucces': 'Bovenbouwsucces'}
            _stats = []
            for _r in _insp.to_dicts():
                _label = _ind_labels.get(_r['indicator'], _r['indicator'])
                if _r.get('waarde') is not None:
                    _stats.append(mo.stat(value=f"{_r['waarde']:.2f}", label=_label, caption=f"norm: {_r.get('norm', 0):.2f}", bordered=True))
            if _stats:
                _sections.append(mo.md("---"))
                _sections.append(mo.md("### Oordeel Inspectie"))
                _sections.append(mo.md("*Hoe presteren leerlingen t.o.v. verwachting? Waarde boven de norm is positief.*"))
                _sections.append(mo.hstack(_stats, gap=2))

        if not _sections:
            _sections.append(mo.callout("Geen data beschikbaar voor deze school", kind="warn"))

        detail_content = mo.vstack([
            back_button,
            school_dropdown,
            mo.vstack(_sections),
        ])
    return (detail_content,)


@app.cell
def _(mo):
    strategy_content = mo.vstack([
        mo.md("""
    ## Strategisch Advies

    ### Tips voor een goede voorkeurslijst

    1. **Droomscholen bovenaan** - Zet populaire scholen die je echt wilt bovenaan
    2. **Veilige scholen onderaan** - Minimaal 2-3 scholen met ratio < 0.7 als vangnet
    3. **Vul alle 12 slots** - Een lege plek is een gemiste kans
    4. **Diversifieer stadsdelen** - Meer opties = meer kans
    5. **Check kwaliteitsdata** - Kijk ook naar examencijfers en tevredenheid

    ### Risicoprofielen

    | Profiel | Droom | Competitief | Veilig |
    |---------|-------|-------------|--------|
    | Conservatief | 30% | 30% | 40% |
    | Gebalanceerd | 40% | 35% | 25% |
    | Ambitieus | 60% | 25% | 15% |

    ### Ratio uitleg

    - **Ratio > 1.0** 🔴 = Meer aanmeldingen dan plekken (loting)
    - **Ratio 0.7-1.0** 🟡 = Competitief maar haalbaar
    - **Ratio < 0.7** 🟢 = Grote kans op plaatsing
        """),
    ])
    return (strategy_content,)


@app.cell
def _(alt, db, mo, pl):
    # Historical summary: yearly participation and placement rates (2019-2025)
    _year_summary = pl.from_arrow(db.execute("""
        SELECT
            CAST(jaar AS VARCHAR) as jaar,
            totaal_deelnemers,
            totaal_capaciteit,
            ROUND(percentage_eerste_voorkeur, 1) as eerste_voorkeur_pct,
            ROUND(percentage_top3, 1) as top3_pct
        FROM loting_db.jaar_samenvatting
        WHERE percentage_eerste_voorkeur IS NOT NULL
        ORDER BY jaar
    """).fetch_arrow_table())

    # Create trend chart
    _trend_chart = mo.md("")
    if not _year_summary.is_empty():
        _trend_data = _year_summary.unpivot(
            index='jaar',
            on=['eerste_voorkeur_pct', 'top3_pct'],
            variable_name='Metric',
            value_name='Percentage'
        )

        _chart = alt.Chart(_trend_data).mark_line(point=True).encode(
            x=alt.X('jaar:N', title='Jaar', sort=None),
            y=alt.Y('Percentage:Q', scale=alt.Scale(domain=[70, 100]), title='Percentage'),
            color=alt.Color('Metric:N', legend=alt.Legend(title=""), scale=alt.Scale(
                domain=['eerste_voorkeur_pct', 'top3_pct'],
                range=['#3498db', '#2ecc71']
            )),
            tooltip=['jaar:N', 'Metric:N', alt.Tooltip('Percentage:Q', format='.1f')]
        ).properties(
            width=500,
            height=250,
            title='Plaatsingspercentages door de jaren'
        )
        _trend_chart = mo.ui.altair_chart(_chart)

    # Schools overview: 3 years of capacity, demand and ratio per school
    _schools_overview = pl.from_arrow(db.execute("""
        WITH school_years AS (
            SELECT
                ls.naam as school,
                sd.naam as stadsdeel,
                c.jaar,
                c.definitieve_capaciteit as capaciteit,
                v.eerste_voorkeur,
                ROUND(CAST(v.eerste_voorkeur AS FLOAT) / NULLIF(c.definitieve_capaciteit, 0), 2) as ratio
            FROM loting_db.afdeling a
            JOIN loting_db.loting_school ls ON a.school_id = ls.id
            JOIN loting_db.stadsdeel sd ON ls.stadsdeel_id = sd.id
            LEFT JOIN loting_db.capaciteit c ON a.id = c.afdeling_id
            LEFT JOIN loting_db.voorkeuren v ON a.id = v.afdeling_id AND v.jaar = c.jaar
            WHERE a.onderwijsniveau_id = 1
              AND a.variant IS NULL
              AND c.jaar >= 2023
        )
        SELECT
            school,
            stadsdeel,
            MAX(CASE WHEN jaar = 2023 THEN capaciteit END) as cap_2023,
            MAX(CASE WHEN jaar = 2023 THEN eerste_voorkeur END) as ev_2023,
            MAX(CASE WHEN jaar = 2023 THEN ratio END) as ratio_2023,
            MAX(CASE WHEN jaar = 2024 THEN capaciteit END) as cap_2024,
            MAX(CASE WHEN jaar = 2024 THEN eerste_voorkeur END) as ev_2024,
            MAX(CASE WHEN jaar = 2024 THEN ratio END) as ratio_2024,
            MAX(CASE WHEN jaar = 2025 THEN capaciteit END) as cap_2025,
            MAX(CASE WHEN jaar = 2025 THEN eerste_voorkeur END) as ev_2025,
            MAX(CASE WHEN jaar = 2025 THEN ratio END) as ratio_2025
        FROM school_years
        GROUP BY school, stadsdeel
        ORDER BY school
    """).fetch_arrow_table())

    stats_content = mo.vstack([
        mo.md("""
    ## Statistieken

    ### Lotingresultaten per jaar
    Hoeveel leerlingen worden bij hun eerste voorkeur of top-3 keuze geplaatst?
    Deze cijfers geven een beeld van hoe de loting door de jaren heen verloopt.
        """),
        _trend_chart,
        mo.ui.table(_year_summary.rename({
            'jaar': 'Jaar',
            'totaal_deelnemers': 'Deelnemers',
            'totaal_capaciteit': 'Capaciteit',
            'eerste_voorkeur_pct': '1e Voorkeur %',
            'top3_pct': 'Top 3 %'
        }), page_size=10),
        mo.md("""
    ---

    ### Scholen overzicht (2023-2025)
    Capaciteit, aantal eerste voorkeuren en populariteitsratio per school over de laatste drie jaar.
    De ratio geeft aan hoeveel aanmeldingen er zijn per beschikbare plek (>1 = meer vraag dan aanbod).
        """),
        mo.ui.table(_schools_overview.rename({
            'school': 'School',
            'stadsdeel': 'Stadsdeel',
            'cap_2023': 'Cap 23',
            'ev_2023': 'EV 23',
            'ratio_2023': 'Ratio 23',
            'cap_2024': 'Cap 24',
            'ev_2024': 'EV 24',
            'ratio_2024': 'Ratio 24',
            'cap_2025': 'Cap 25',
            'ev_2025': 'EV 25',
            'ratio_2025': 'Ratio 25',
        }), page_size=20),
    ])
    return (stats_content,)


@app.cell
def _(
    active_tab_state,
    detail_content,
    explorer_content,
    list_content,
    mo,
    overview_content,
    set_active_tab,
    stats_content,
    strategy_content,
):
    _title = mo.md("# Amsterdam VWO Loting Dashboard")
    _tabs = mo.ui.tabs(
        {
            "Overzicht": overview_content,
            "Scholen Verkenner": explorer_content,
            "School Details": detail_content,
            "Mijn Lijst": list_content,
            "Statistieken": stats_content,
            "Strategie": strategy_content,
        },
        value=active_tab_state(),
        on_change=set_active_tab,
    )
    mo.vstack([_title, _tabs])
    return


if __name__ == "__main__":
    app.run()
