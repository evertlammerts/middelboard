# Middelbare scholen voor Z

Prerequisites: [Astral uv](https://docs.astral.sh/uv/getting-started/installation/)

```
git clone https://github.com/evertlammerts/middelboard.git
uv run marimo run notebook.py
```

## Scholen

* Barlaeus Gymnasium
* Het Amsterdams Lyceum
* Spinoza Lyceum Amsterdam
* Geert Groote College Amsterdam
* Montessori Lyceum Amsterdam - Hoofdlocatie
* St. Nicolaaslyceum
* Gerrit van der Veen College
* Fons Vitae Lyceum

## Scholen toevoegen

Stappen:
1. Voeg de URLs van de "tevredenheid" en "resultaten" pagina's toe aan urls.txt.
2. Download de HTML: 
```
for url in `cat urls.txt`; do
  file_path="html/$( basename $url )-$( basename `dirname $url` ).html"
  echo $file_path
  curl -o $file_path "$url"
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
