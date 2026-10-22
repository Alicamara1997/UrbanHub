"""
UrbanHub - Meteo recente (pour la Partie 4 : croisement avec velos et pollution)

Les donnees NOAA de la Partie 1 s'arretent en aout 2025. Pour avoir des jours
en commun avec les velos et la pollution (2026), on recupere la meteo heure par
heure des 92 derniers jours des 5 villes avec l'API gratuite Open-Meteo.

Conventions de l'equipe : un seul bucket (MINIO_BUCKET, "urbanhub"), colonne
"city", timestamp en UTC + date_local / hour_local (Europe/Paris), Parquet.

  bronze/meteo_recente/extraction=AAAA-MM-JJ/<city>.jsonl.gz   reponses brutes
  silver/meteo_recente/meteo_recente.parquet                   1 ligne = ville x heure
  gold/meteo_horaire_ville/meteo_horaire_ville.parquet         table de jointure (Partie 4)
  gold/meteo_journaliere_ville/meteo_journaliere_ville.parquet 1 ligne = ville x jour (Paris)

Usage :  python meteo_recente.py
A relancer avant la Partie 4 / la demo : chaque extraction ajoute les derniers
jours, et les doublons sont enleves en Silver (on garde l'extraction la plus recente).
"""
import gzip
import io
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

import pandas as pd
import requests

try:
    from minio import Minio
except ImportError:
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "minio"], check=True)
    from minio import Minio

API = "https://api.open-meteo.com/v1/forecast"
CITIES = {                                    # memes villes et coordonnees que config/cities.yaml
    "Paris":     (48.8566, 2.3522),
    "Lyon":      (45.7640, 4.8357),
    "Marseille": (43.2965, 5.3698),
    "Toulouse":  (43.6047, 1.4442),
    "Bordeaux":  (44.8378, -0.5792),
}
VARIABLES = {                                 # nom Open-Meteo -> nom du projet (comme la Partie 1)
    "temperature_2m": "temperature",          # degres C
    "relative_humidity_2m": "humidity",       # %
    "precipitation": "precipitation",         # mm sur l'heure
    "wind_speed_10m": "wind_speed",           # m/s
    "wind_direction_10m": "wind_direction",   # degres
    "pressure_msl": "pressure",               # hPa
    "visibility": "visibility",               # metres
    "cloud_cover": "cloud_cover",             # %
}

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
BUCKET = os.getenv("MINIO_BUCKET", "urbanhub")
client = Minio(MINIO_ENDPOINT.split("://")[1],
               access_key=os.getenv("MINIO_ACCESS_KEY", "urbanhub"),
               secret_key=os.getenv("MINIO_SECRET_KEY", "urbanhub123"),
               secure=MINIO_ENDPOINT.startswith("https"))


def ecrire(chemin, donnees, type_contenu):
    client.put_object(BUCKET, chemin, io.BytesIO(donnees), len(donnees), content_type=type_contenu)


def ecrire_parquet(chemin, df):
    tampon = io.BytesIO()
    # microsecondes : Spark 3.5 ne sait pas lire les timestamps en nanosecondes (defaut de pandas)
    df.to_parquet(tampon, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    ecrire(chemin, tampon.getvalue(), "application/octet-stream")


# ------------------------------------------------------------------ BRONZE
def collecter(city, lat, lon, maintenant):
    r = requests.get(API, params={
        "latitude": lat, "longitude": lon, "hourly": ",".join(VARIABLES),
        "past_days": 92, "forecast_days": 1,
        "timezone": "UTC", "wind_speed_unit": "ms",
    }, timeout=60)
    r.raise_for_status()
    h = r.json()["hourly"]
    lignes = []
    for i, t in enumerate(h["time"]):
        if datetime.fromisoformat(t).replace(tzinfo=timezone.utc) > maintenant:
            break                                   # on ne garde pas les previsions
        ligne = {"city": city, "latitude": lat, "longitude": lon, "timestamp": t + ":00Z",
                 "extraction": maintenant.strftime("%Y-%m-%dT%H:%M:%SZ")}
        ligne.update({nom: h[v][i] for v, nom in VARIABLES.items()})
        lignes.append(ligne)
    texte = "\n".join(json.dumps(l, ensure_ascii=False) for l in lignes)
    ecrire(f"bronze/meteo_recente/extraction={maintenant:%Y-%m-%d}/{city}.jsonl.gz",
           gzip.compress(texte.encode("utf-8")), "application/gzip")
    return len(lignes)


def lire_bronze():
    morceaux = []
    for obj in client.list_objects(BUCKET, prefix="bronze/meteo_recente/", recursive=True):
        if obj.object_name.endswith(".jsonl.gz"):
            r = client.get_object(BUCKET, obj.object_name)
            try:
                morceaux.append(pd.read_json(io.BytesIO(gzip.decompress(r.read())), lines=True))
            finally:
                r.close(); r.release_conn()
    return pd.concat(morceaux, ignore_index=True)


# ------------------------------------------------------------- SILVER / GOLD
def silver_gold():
    df = lire_bronze()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    # une ligne par ville et par heure : on garde l'extraction la plus recente
    df = (df.sort_values("extraction")
            .drop_duplicates(["city", "timestamp"], keep="last")
            .drop(columns=["extraction"]))
    # valeurs impossibles -> vide (memes limites que la Partie 1)
    limites = {"temperature": (-50, 50), "wind_speed": (0, 75), "pressure": (870, 1090),
               "visibility": (0, 160000), "precipitation": (0, 300), "humidity": (0, 100)}
    for col, (mini, maxi) in limites.items():
        df.loc[~df[col].between(mini, maxi), col] = None
    # convention equipe : timestamp UTC + date / heure de Paris
    paris = df["timestamp"].dt.tz_convert("Europe/Paris")
    df["date_local"] = paris.dt.date
    df["hour_local"] = paris.dt.hour.astype("int32")
    df = df.sort_values(["city", "timestamp"]).reset_index(drop=True)
    colonnes = ["city", "latitude", "longitude", "timestamp", "date_local", "hour_local",
                *VARIABLES.values()]
    df = df[colonnes]

    ecrire_parquet("silver/meteo_recente/meteo_recente.parquet", df)
    # table de jointure pour la Partie 4 (meme grain que gold/pollution_horaire_ville)
    ecrire_parquet("gold/meteo_horaire_ville/meteo_horaire_ville.parquet",
                   df.drop(columns=["latitude", "longitude"]))
    jour = df.groupby(["city", "date_local"]).agg(
        temp_moy=("temperature", "mean"), temp_min=("temperature", "min"),
        temp_max=("temperature", "max"), humidite_moy=("humidity", "mean"),
        pluie_mm=("precipitation", "sum"), vent_moy=("wind_speed", "mean"),
        vent_max=("wind_speed", "max"), pression_moy=("pressure", "mean"),
        visibilite_moy=("visibility", "mean"), nuages_moy=("cloud_cover", "mean"),
        nb_heures=("temperature", "count")).round(1).reset_index()
    ecrire_parquet("gold/meteo_journaliere_ville/meteo_journaliere_ville.parquet", jour)
    return df, jour


if __name__ == "__main__":
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
    maintenant = datetime.now(timezone.utc)
    for city, (lat, lon) in CITIES.items():
        print(f"Bronze  {city:10s} : {collecter(city, lat, lon, maintenant)} heures")
    df, jour = silver_gold()
    print(f"Silver  : {len(df)} lignes (ville x heure), du {df['timestamp'].min():%Y-%m-%d} "
          f"au {df['timestamp'].max():%Y-%m-%d %H:%M} UTC")
    print(f"Gold    : meteo_horaire_ville ({len(df)} lignes), meteo_journaliere_ville ({len(jour)} lignes)")
    print(f"Bucket  : s3://{BUCKET}/  (colonnes : {', '.join(df.columns)})")
