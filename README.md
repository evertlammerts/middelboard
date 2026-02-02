# Middelbare scholen voor Z

Prerequisites: [Astral uv](https://docs.astral.sh/uv/getting-started/installation/)

```
git clone https://github.com/evertlammerts/middelboard.git
uv run marimo run notebook.py
```

## Scholen

- Alasca
- Barlaeus Gymnasium
- Berlage Lyceum
- Calandlyceum
- Cartesius Amsterdam
- Comenius Lyceum Amsterdam
- Cornelius Haga Lyceum
- Cygnus Gymnasium
- Damstede
- DENISE
- Fons Vitae Lyceum
- Geert Groote College
- Gerrit van der Veen College
- Hervormd Lyceum West
- Het 4e Gymnasium
- Het Amsterdams Lyceum
- HLZ (Hervormd Lyceum Zuid)
- Hyperion Lyceum
- Ignatiusgymnasium
- Ir. Lely Lyceum
- Kairos Tienercollege
- Lumion
- Marcanti College
- Metis Montessori Lyceum
- Metropolis Lyceum
- Montessori Lyceum Pax
- Montessori Lyceum Terra Nova
- OSB
- Pieter Nieuwland College
- Spinoza Lyceum
- St. Nicolaaslyceum
- Vinse School
- Vossius Gymnasium
- Xplore

## Scholen toevoegen

Je kunt scholen toevoegen aan het dashboard om de resultaat- en tevredenheidcijfers te vergelijken. De "red flags" sectie wordt dan neit ge-update - die is gemaakt door Claude.

Stappen:
1. Voeg de URL van de school toe aan urls.txt.
2. Download de HTML: 
```
for url in `cat urls.txt`; do
  filename_suffix="$( basename $url ).html"
  curl -o html/resultaten-${filename_suffix} "${url}resultaten/"
  curl -o html/tevredenheid-${filename_suffix} "${url}tevredenheid/"
done
```
3. Extraheer de data naar JSON:
```
uv run python parse_schools.py
```
4. Re-genereer de database:
```
uv run python create_database.py
```
5. Bekijk de school in Marimo:
```
uv run marimo run notebook.py
```

