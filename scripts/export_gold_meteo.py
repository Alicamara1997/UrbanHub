"""
UrbanHub - Export des tables GOLD meteo pour l'equipe (Partie 4).

Lit les tables gold/meteo* du bucket commun et les ecrit en CSV dans
data/export_gold_meteo/, puis cree data/export_gold_meteo.zip a partager.
"""
import os
import zipfile
import pyarrow.dataset as ds
from pyarrow import fs

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
BUCKET = os.getenv("MINIO_BUCKET", "urbanhub")
s3 = fs.S3FileSystem(endpoint_override=MINIO_ENDPOINT.split("://")[1],
                     scheme=MINIO_ENDPOINT.split("://")[0],
                     access_key=os.getenv("MINIO_ACCESS_KEY", "urbanhub"),
                     secret_key=os.getenv("MINIO_SECRET_KEY", "urbanhub123"),
                     region="us-east-1")

TABLES = {   # chemin dans le bucket -> description (pour le fichier LISEZMOI)
    "gold/meteo_horaire_ville": "Meteo recente (Open-Meteo), 1 ligne = city x heure. CLE DE JOINTURE : city + timestamp (UTC) ou city + date_local + hour_local (Paris).",
    "gold/meteo_journaliere_ville": "Meteo recente (Open-Meteo), 1 ligne = city x jour (date_local, heure de Paris).",
    "gold/meteo/journalier": "NOAA 2021-aout 2025, 1 ligne = station x jour (city vide si station hors des 5 villes).",
    "gold/meteo/temperature_mensuelle": "NOAA, temperature moyenne par city (et France) et par mois.",
    "gold/meteo/anomalies_france": "NOAA, ecart a la normale de la temperature France, jour par jour.",
    "gold/meteo/jours_extremes_villes": "NOAA, jours extremes (chaleur, froid, vent, pluie) par city.",
}

SORTIE = "/home/jovyan/work/data/export_gold_meteo"
os.makedirs(SORTIE, exist_ok=True)
lisezmoi = ["Tables GOLD meteo - UrbanHub (Ali)", "Conventions : city, timestamp en UTC, date_local / hour_local en heure de Paris.", ""]
for chemin, description in TABLES.items():
    try:
        table = ds.dataset(f"{BUCKET}/{chemin}", filesystem=s3, format="parquet",
                           partitioning="hive").to_table()
    except Exception as e:
        print(f"ABSENT  {chemin} ({e.__class__.__name__}) : lancer d'abord le notebook / meteo_recente.py")
        continue
    df = table.to_pandas()
    nom = chemin.replace("gold/", "").replace("/", "_") + ".csv"
    df.to_csv(f"{SORTIE}/{nom}", index=False)
    lisezmoi.append(f"- {nom} ({len(df)} lignes) : {description}")
    lisezmoi.append(f"    colonnes : {', '.join(df.columns)}")
    print(f"OK      {nom:40s} {len(df):>8} lignes")

with open(f"{SORTIE}/LISEZMOI.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(lisezmoi) + "\n")

with zipfile.ZipFile(SORTIE + ".zip", "w", zipfile.ZIP_DEFLATED) as z:
    for nom in sorted(os.listdir(SORTIE)):
        z.write(f"{SORTIE}/{nom}", f"export_gold_meteo/{nom}")
taille = os.path.getsize(SORTIE + ".zip") / 1e6
print(f"\nZip pret : data/export_gold_meteo.zip ({taille:.1f} Mo)")
