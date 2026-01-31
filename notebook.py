import marimo

__generated_with = "0.19.7"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb
    import altair as alt
    return alt, mo


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        ATTACH 'scholen.duckdb' as scholen_db (READ_ONLY);
        """
    )
    return


@app.cell
def _(mo):
    mo.md("""
    # Scholen op de Kaart - Dashboard

    Interactieve analyse van VWO-resultaten en tevredenheid voor Amsterdamse scholen.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Vergelijking Alle Scholen
    """)
    return


@app.cell
def _(mo):
    examens = mo.sql(
        f"""
        ATTACH 'scholen.duckdb' (READ_ONLY);
        SELECT
            s.name as School,
            round(e.eindcijfer, 2) as Eindcijfer,
            round(e.centraal_examen, 2) as "Centraal Examen",
            round(e.school_examen, 2) as "School Examen",
            round(e.centraal_examen_vergelijking, 2) as "CE Landelijk"
        FROM scholen_db.schools s
        JOIN scholen_db.examencijfers e ON s.id = e.school_id
        WHERE e.schooljaar = '2023-2024'
        ORDER BY e.eindcijfer DESC
        """
    )
    return (examens,)


@app.cell
def _(alt, examens, mo):
    exam_chart = alt.Chart(examens).mark_bar().encode(
        x=alt.X('School:N', sort='-y'),
        y=alt.Y('Eindcijfer:Q', scale=alt.Scale(domain=[6.0, 7.2])),
        color=alt.Color('Eindcijfer:Q', scale=alt.Scale(scheme='redyellowgreen', domain=[6.5, 7.0])),
        tooltip=['School', 'Eindcijfer', 'Centraal Examen', 'School Examen']
    ).properties(width=600, height=300, title='Examencijfers 2023-2024 (VWO)')

    mo.ui.altair_chart(exam_chart)
    return


@app.cell
def _(mo):
    slagingspercentages = mo.sql(
        f"""
        SELECT
            s.name as School,
            round(sp.percentage, 1) as Slaagpercentage,
            round(sp.vergelijking, 1) as Landelijk,
            round(sp.percentage - sp.vergelijking, 1) as Verschil
        FROM scholen_db.schools s
        JOIN scholen_db.slagingspercentage sp ON s.id = sp.school_id
        WHERE sp.schooljaar = '2023-2024'
        ORDER BY sp.percentage DESC
        """
    )
    return (slagingspercentages,)


@app.cell
def _(alt, mo, slagingspercentages):
    pass_chart = alt.Chart(slagingspercentages).mark_bar().encode(
        x=alt.X('School:N', sort='-y'),
        y=alt.Y('Slaagpercentage:Q', scale=alt.Scale(domain=[80, 100])),
        color=alt.condition(
            alt.datum.Verschil >= 0,
            alt.value('#2ecc71'),
            alt.value('#e74c3c')
        ),
        tooltip=['School', 'Slaagpercentage', 'Landelijk', 'Verschil']
    ).properties(width=600, height=300, title='Slaagpercentage 2023-2024 - Groen = boven landelijk')

    rule = alt.Chart(slagingspercentages).mark_rule(color='black', strokeDash=[5,5]).encode(
        y='mean(Landelijk):Q'
    )

    mo.ui.altair_chart(pass_chart + rule)
    return


@app.cell
def _(mo):
    tevredenheid = mo.sql(
        f"""
        SELECT
            s.name as School,
            t.metric as Metric,
            round(t.cijfer, 2) as Score,
            round(t.vergelijking, 2) as Benchmark
        FROM scholen_db.schools s
        JOIN scholen_db.tevredenheid_trend t ON s.id = t.school_id
        WHERE t.schooljaar = '2023-2024'
        ORDER BY s.name, t.metric
        """
    )
    return (tevredenheid,)


@app.cell
def _(alt, mo, tevredenheid):
    tevr_chart = alt.Chart(tevredenheid).mark_bar().encode(
        x=alt.X('Metric:N'),
        y=alt.Y('Score:Q', scale=alt.Scale(domain=[5, 10])),
        color=alt.Color('School:N'),
        xOffset='School:N',
        tooltip=['School', 'Metric', 'Score', 'Benchmark']
    ).properties(width=600, height=350, title='Tevredenheid 2023-2024 per School')

    mo.ui.altair_chart(tevr_chart)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Ranking & Totaalscore
    """)
    return


@app.cell
def _(mo):
    ranking = mo.sql(
        f"""
        WITH metrics AS (
            SELECT
                s.id, s.name,
                e.eindcijfer,
                sp.percentage as slaagpct,
                sp.vergelijking as slaag_land,
                bb.percentage as doorstroom_bb,
                t_l.cijfer as leerling,
                t_o.cijfer as ouder,
                t_sf.cijfer as sfeer,
                t_v.cijfer as veiligheid
            FROM scholen_db.schools s
            LEFT JOIN scholen_db.examencijfers e ON s.id = e.school_id AND e.schooljaar = '2023-2024'
            LEFT JOIN scholen_db.slagingspercentage sp ON s.id = sp.school_id AND sp.schooljaar = '2023-2024'
            LEFT JOIN scholen_db.doorstroom_bovenbouw bb ON s.id = bb.school_id AND bb.schooljaar = '2023-2024'
            LEFT JOIN scholen_db.tevredenheid_trend t_l ON s.id = t_l.school_id AND t_l.schooljaar = '2023-2024' AND t_l.metric = 'leerlingen'
            LEFT JOIN scholen_db.tevredenheid_trend t_o ON s.id = t_o.school_id AND t_o.schooljaar = '2023-2024' AND t_o.metric = 'ouders'
            LEFT JOIN scholen_db.tevredenheid_trend t_sf ON s.id = t_sf.school_id AND t_sf.schooljaar = '2023-2024' AND t_sf.metric = 'sfeer'
            LEFT JOIN scholen_db.tevredenheid_trend t_v ON s.id = t_v.school_id AND t_v.schooljaar = '2023-2024' AND t_v.metric = 'veiligheid'
        )
        SELECT
            name as School,
            round(eindcijfer, 2) as Eindcijfer,
            round(slaagpct, 1) as "Slaag%",
            round(slaagpct - slaag_land, 1) as "vs Land",
            round(doorstroom_bb, 1) as Doorstroom,
            round(leerling, 2) as Leerling,
            round(ouder, 2) as Ouder,
            round(sfeer, 2) as Sfeer,
            round(veiligheid, 2) as Veilig,
            round(
                COALESCE(eindcijfer, 6.5) * 10 +
                COALESCE(slaagpct, 90) * 0.3 +
                COALESCE(leerling, 6) * 3 +
                COALESCE(ouder, 7.5) * 2 +
                COALESCE(sfeer, 7) * 2 +
                COALESCE(veiligheid, 9) * 1
            , 1) as Totaal
        FROM metrics
        ORDER BY Totaal DESC
        """
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## School Details
    """)
    return


@app.cell
def _(mo):
    schools_list = mo.sql(
        f"""
        SELECT name FROM scholen_db.schools ORDER BY name
        """
    )
    return (schools_list,)


@app.cell
def _(mo, schools_list):
    school_names = schools_list['name'].to_list()
    school_dropdown = mo.ui.dropdown(
        options=school_names,
        label="Selecteer school",
        value=school_names[0] if school_names else None
    )
    school_dropdown
    return (school_dropdown,)


@app.cell
def _(mo, school_dropdown):
    selected_school = school_dropdown.value or ''
    school_exams = mo.sql(
        f"""
        SELECT
            e.schooljaar as Jaar,
            round(e.centraal_examen, 2) as CE,
            round(e.school_examen, 2) as SE,
            round(e.eindcijfer, 2) as Eind
        FROM scholen_db.schools s
        JOIN scholen_db.examencijfers e ON s.id = e.school_id
        WHERE s.name = '{selected_school}'
        ORDER BY e.schooljaar
        """
    )
    return school_exams, selected_school


@app.cell
def _(alt, mo, school_dropdown, school_exams):
    if not school_exams.is_empty():
        exam_long = school_exams.unpivot(index='Jaar', variable_name='Type', value_name='Cijfer')
        exam_trend = alt.Chart(exam_long).mark_line(point=True).encode(
            x='Jaar:N',
            y=alt.Y('Cijfer:Q', scale=alt.Scale(domain=[6.0, 7.5])),
            color='Type:N',
            tooltip=['Jaar', 'Type', 'Cijfer']
        ).properties(width=400, height=250, title=f'Examencijfers {school_dropdown.value}')
        mo.ui.altair_chart(exam_trend)
    return


@app.cell
def _(mo, selected_school):
    school_tevr = mo.sql(
        f"""
        SELECT
            t.schooljaar as Jaar,
            t.metric as Metric,
            round(t.cijfer, 2) as Score
        FROM scholen_db.schools s
        JOIN scholen_db.tevredenheid_trend t ON s.id = t.school_id
        WHERE s.name = '{selected_school}'
        ORDER BY t.metric, t.schooljaar
        """
    )
    return (school_tevr,)


@app.cell
def _(alt, mo, school_dropdown, school_tevr):
    if not school_tevr.is_empty():
        tevr_trend = alt.Chart(school_tevr).mark_line(point=True).encode(
            x='Jaar:N',
            y=alt.Y('Score:Q', scale=alt.Scale(domain=[5, 10])),
            color='Metric:N',
            tooltip=['Jaar', 'Metric', 'Score']
        ).properties(width=400, height=250, title=f'Tevredenheid {school_dropdown.value}')
        mo.ui.altair_chart(tevr_trend)
    return


@app.cell
def _(mo):
    mo.md("""
    ### Tevredenheid Vragen
    """)
    return


@app.cell
def _(mo, selected_school):
    school_vragen = mo.sql(
        f"""
        SELECT
            v.respondent as Type,
            v.vraag as Vraag,
            round(v.cijfer, 1) as Cijfer,
            CASE
                WHEN v.cijfer < 5.5 THEN 'Kritiek'
                WHEN v.cijfer < 6.5 THEN 'Matig'
                ELSE 'Goed'
            END as Status
        FROM scholen_db.schools s
        JOIN scholen_db.tevredenheid_vragen v ON s.id = v.school_id
        WHERE s.name = '{selected_school}'
        ORDER BY v.cijfer ASC NULLS LAST
        """
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ### Inspectie Oordeel
    """)
    return


@app.cell
def _(mo, selected_school):
    school_inspectie = mo.sql(
        f"""
        SELECT
            o.indicator as Indicator,
            round(o.schoolwaarde, 2) as School,
            round(o.inspectienorm, 2) as Norm,
            round(o.schoolwaarde - o.inspectienorm, 2) as Marge,
            o.periode as Periode
        FROM scholen_db.schools s
        JOIN scholen_db.oordeel_inspectie o ON s.id = o.school_id
        WHERE s.name = '{selected_school}'
        ORDER BY o.indicator
        """
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ### Doorstroom
    """)
    return


@app.cell
def _(mo, selected_school):
    school_doorstroom = mo.sql(
        f"""
        SELECT 'Onderbouw' as Type, ob.schooljaar as Jaar, round(ob.percentage, 1) as Percentage
        FROM scholen_db.schools s
        JOIN scholen_db.doorstroom_onderbouw ob ON s.id = ob.school_id
        WHERE s.name = '{selected_school}'
        UNION ALL
        SELECT 'Bovenbouw VWO', bb.schooljaar, round(bb.percentage, 1)
        FROM scholen_db.schools s
        JOIN scholen_db.doorstroom_bovenbouw bb ON s.id = bb.school_id
        WHERE s.name = '{selected_school}'
        ORDER BY Type, Jaar
        """
    )
    return (school_doorstroom,)


@app.cell
def _(alt, mo, school_doorstroom, school_dropdown):
    if not school_doorstroom.is_empty():
        doorstr_chart = alt.Chart(school_doorstroom).mark_line(point=True).encode(
            x='Jaar:N',
            y=alt.Y('Percentage:Q', scale=alt.Scale(domain=[80, 100])),
            color='Type:N',
            tooltip=['Jaar', 'Type', 'Percentage']
        ).properties(width=400, height=250, title=f'Doorstroom {school_dropdown.value}')
        mo.ui.altair_chart(doorstr_chart)
    return


@app.cell
def _(mo):
    mo.md("""
    ### Schooladvies
    """)
    return


@app.cell
def _(mo, selected_school):
    school_advies = mo.sql(
        f"""
        SELECT
            sa.positie as Positie,
            round(sa.percentage, 1) as School,
            round(sa.vergelijking, 1) as Landelijk,
            round(sa.percentage - sa.vergelijking, 1) as Verschil
        FROM scholen_db.schools s
        JOIN scholen_db.schooladvies sa ON s.id = sa.school_id
        WHERE s.name = '{selected_school}'
        """
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## Metric Vergelijking
    """)
    return


@app.cell
def _(mo):
    metric_dropdown = mo.ui.dropdown(
        options=["Eindcijfer", "Slaagpercentage", "Leerlingen", "Ouders", "Sfeer", "Veiligheid", "Doorstroom"],
        label="Selecteer metric",
        value="Eindcijfer"
    )
    metric_dropdown
    return (metric_dropdown,)


@app.cell
def _(metric_dropdown, mo):
    selected_metric = metric_dropdown.value
    if selected_metric == "Eindcijfer":
        metric_query = "SELECT s.name as School, e.eindcijfer as Waarde, 6.6 as Benchmark FROM scholen_db.schools s JOIN scholen_db.examencijfers e ON s.id = e.school_id WHERE e.schooljaar = '2023-2024'"
    elif selected_metric == "Slaagpercentage":
        metric_query = "SELECT s.name as School, sp.percentage as Waarde, sp.vergelijking as Benchmark FROM scholen_db.schools s JOIN scholen_db.slagingspercentage sp ON s.id = sp.school_id WHERE sp.schooljaar = '2023-2024'"
    elif selected_metric == "Leerlingen":
        metric_query = "SELECT s.name as School, t.cijfer as Waarde, t.vergelijking as Benchmark FROM scholen_db.schools s JOIN scholen_db.tevredenheid_trend t ON s.id = t.school_id WHERE t.schooljaar = '2023-2024' AND t.metric = 'leerlingen'"
    elif selected_metric == "Ouders":
        metric_query = "SELECT s.name as School, t.cijfer as Waarde, t.vergelijking as Benchmark FROM scholen_db.schools s JOIN scholen_db.tevredenheid_trend t ON s.id = t.school_id WHERE t.schooljaar = '2023-2024' AND t.metric = 'ouders'"
    elif selected_metric == "Sfeer":
        metric_query = "SELECT s.name as School, t.cijfer as Waarde, t.vergelijking as Benchmark FROM scholen_db.schools s JOIN scholen_db.tevredenheid_trend t ON s.id = t.school_id WHERE t.schooljaar = '2023-2024' AND t.metric = 'sfeer'"
    elif selected_metric == "Veiligheid":
        metric_query = "SELECT s.name as School, t.cijfer as Waarde, t.vergelijking as Benchmark FROM scholen_db.schools s JOIN scholen_db.tevredenheid_trend t ON s.id = t.school_id WHERE t.schooljaar = '2023-2024' AND t.metric = 'veiligheid'"
    else:
        metric_query = "SELECT s.name as School, bb.percentage as Waarde, 88.7 as Benchmark FROM scholen_db.schools s JOIN scholen_db.doorstroom_bovenbouw bb ON s.id = bb.school_id WHERE bb.schooljaar = '2023-2024'"

    metric_data = mo.sql(f"{metric_query}")
    return (metric_data,)


@app.cell
def _(alt, metric_data, metric_dropdown, mo):
    if not metric_data.is_empty():
        m_chart = alt.Chart(metric_data).mark_bar().encode(
            x=alt.X('School:N', sort='-y'),
            y=alt.Y('Waarde:Q'),
            color=alt.condition(
                alt.datum.Waarde >= alt.datum.Benchmark,
                alt.value('#2ecc71'),
                alt.value('#e74c3c')
            ),
            tooltip=['School', 'Waarde', 'Benchmark']
        ).properties(width=600, height=300, title=f'{metric_dropdown.value} per School')

        m_rule = alt.Chart(metric_data).mark_rule(color='black', strokeDash=[5,5]).encode(
            y='mean(Benchmark):Q'
        )
        mo.ui.altair_chart(m_chart + m_rule)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Red Flags
    """)
    return


@app.cell
def _(mo):
    red_flags = mo.sql(
        f"""
        WITH flags AS (
            SELECT s.name as School, 'Laag slaagpercentage' as Issue,
                   round(sp.percentage, 1) || '% (land: ' || round(sp.vergelijking, 1) || '%)' as Detail
            FROM scholen_db.schools s
            JOIN scholen_db.slagingspercentage sp ON s.id = sp.school_id
            WHERE sp.schooljaar = '2023-2024' AND sp.percentage < sp.vergelijking - 3

            UNION ALL

            SELECT s.name, 'CE onder landelijk',
                   round(e.centraal_examen, 2) || ' (land: ' || round(e.centraal_examen_vergelijking, 2) || ')'
            FROM scholen_db.schools s
            JOIN scholen_db.examencijfers e ON s.id = e.school_id
            WHERE e.schooljaar = '2023-2024' AND e.centraal_examen < e.centraal_examen_vergelijking - 0.1

            UNION ALL

            SELECT s.name, 'Dalende ' || t24.metric,
                   round(t21.cijfer, 2) || ' -> ' || round(t24.cijfer, 2)
            FROM scholen_db.schools s
            JOIN scholen_db.tevredenheid_trend t24 ON s.id = t24.school_id AND t24.schooljaar = '2023-2024'
            JOIN scholen_db.tevredenheid_trend t21 ON s.id = t21.school_id AND t21.schooljaar = '2021-2022' AND t21.metric = t24.metric
            WHERE t21.cijfer - t24.cijfer > 0.5

            UNION ALL

            SELECT s.name, 'Kritieke score (' || v.respondent || ')',
                   substr(v.vraag, 1, 50) || '...: ' || round(v.cijfer, 1)
            FROM scholen_db.schools s
            JOIN scholen_db.tevredenheid_vragen v ON s.id = v.school_id
            WHERE v.cijfer < 5.0
        )
        SELECT * FROM flags ORDER BY School, Issue
        """
    )
    return


if __name__ == "__main__":
    app.run()
