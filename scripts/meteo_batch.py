"""
UrbanHub - Partie 1 : flux Batch (meteo NOAA)
Telecharge EN PARALLELE les fichiers des stations francaises sur 5 ans
depuis le bucket S3 public de la NOAA, garde les colonnes utiles
et les depose dans la couche Bronze de MinIO.
Usage :  python meteo_batch.py        (toutes les stations)
         python meteo_batch.py test   (3 stations, pour tester)
"""
import io
import os
import re
import sys
import time
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
try:
    from minio import Minio
except ImportError:                      # conteneur recree : on reinstalle la librairie
    import subprocess, sys
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "minio"], check=True)
    from minio import Minio

ANNEES = range(2021, 2026)          # 5 dernieres annees completes
BASE_URL = "https://noaa-global-hourly-pds.s3.amazonaws.com"
LISTE_STATIONS = "https://noaa-isd-pds.s3.amazonaws.com/isd-history.csv"
COLONNES = ["STATION", "DATE", "LATITUDE", "LONGITUDE", "NAME",
            "WND", "VIS", "TMP", "SLP", "AA1"]
DOSSIER = "/home/jovyan/work/data/bronze/meteo"
NB_TELECHARGEMENTS_PARALLELES = 16
ENTETES = {"User-Agent": "UrbanHub-TP-etudiant/1.0 (projet pedagogique)"}

# MinIO commun de l'equipe : memes variables que le reste du projet (voir .env)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
BUCKET = os.getenv("MINIO_BUCKET", "urbanhub")
client = Minio(MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
               access_key=os.getenv("MINIO_ACCESS_KEY", "urbanhub"),
               secret_key=os.getenv("MINIO_SECRET_KEY", "urbanhub123"),
               secure=MINIO_ENDPOINT.startswith("https"))


def get(url, timeout=180):
    """Requete HTTP avec 3 essais. Renvoie None si le fichier n'existe pas."""
    for essai in range(3):
        try:
            r = requests.get(url, headers=ENTETES, timeout=timeout)
            if r.status_code in (403, 404):          # S3 : fichier absent
                return None
            r.raise_for_status()
            return r
        except requests.RequestException:
            if essai == 2:
                raise
            time.sleep(10)


def creer_buckets():
    # un seul bucket pour toute l'equipe : bronze/, silver/, gold/ sont des prefixes
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)


def stations_par_code_07(annee):
    """Plan B : liste les fichiers du bucket S3 dont le code commence par 07
    (zone OMM 07 = France)."""
    ids, jeton = [], None
    while True:
        url = f"{BASE_URL}/?list-type=2&prefix={annee}/07"
        if jeton:
            url += f"&continuation-token={requests.utils.quote(jeton)}"
        texte = get(url, timeout=120).text
        ids += re.findall(rf"<Key>{annee}/(07\d{{9}})\.csv</Key>", texte)
        suite = re.search(r"<NextContinuationToken>(.+?)</NextContinuationToken>", texte)
        if not suite:
            return sorted(set(ids))
        jeton = suite.group(1)


def stations_france():
    """Stations francaises actives depuis 2021."""
    try:
        r = get(LISTE_STATIONS, timeout=60)
        df = pd.read_csv(io.BytesIO(r.content), dtype=str)
        df = df[(df["CTRY"] == "FR") & (df["END"] >= "20210101")].copy()
        df["STATION_ID"] = df["USAF"] + df["WBAN"]
        print("Liste des stations : fichier officiel isd-history (S3)")
    except Exception as e:
        print(f"Liste officielle indisponible ({e}) -> recherche par code 07")
        df = pd.DataFrame({"STATION_ID": stations_par_code_07(max(ANNEES))})

    os.makedirs(DOSSIER, exist_ok=True)
    chemin = f"{DOSSIER}/stations_france.csv"
    df.to_csv(chemin, index=False)
    client.fput_object(BUCKET, "bronze/meteo/stations_france.csv", chemin)
    return df


def telecharger(station_id, annee):
    """Telecharge un fichier station/annee, garde les colonnes utiles,
    l'ecrit en Parquet et l'envoie dans Bronze."""
    r = get(f"{BASE_URL}/{annee}/{station_id}.csv")
    if r is None:
        return station_id, annee, 0                  # pas de donnees cette annee

    df = pd.read_csv(io.BytesIO(r.content), dtype=str,
                     usecols=lambda c: c in COLONNES)
    for c in COLONNES:                               # colonne absente -> vide
        if c not in df.columns:
            df[c] = None
    df = df[COLONNES]

    dossier = f"{DOSSIER}/annee={annee}"
    os.makedirs(dossier, exist_ok=True)
    chemin = f"{dossier}/{station_id}.parquet"
    df.to_parquet(chemin, index=False)
    client.fput_object(BUCKET, f"bronze/meteo/annee={annee}/{station_id}.parquet", chemin)
    os.remove(chemin)                                # copie locale inutile
    return station_id, annee, len(df)


def main():
    debut = time.time()
    creer_buckets()
    stations = stations_france()
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        stations = stations.head(3)
    print(f"{len(stations)} stations francaises x {len(ANNEES)} annees")

    taches = [(s, a) for s in stations["STATION_ID"] for a in ANNEES]
    total_lignes, fichiers, erreurs = 0, 0, 0

    with ThreadPoolExecutor(max_workers=NB_TELECHARGEMENTS_PARALLELES) as pool:
        futurs = {pool.submit(telecharger, s, a): (s, a) for s, a in taches}
        for i, futur in enumerate(as_completed(futurs), 1):
            try:
                _, _, n = futur.result()
                if n:
                    fichiers += 1
                    total_lignes += n
            except Exception as e:
                erreurs += 1
                print(f"   Erreur {futurs[futur]} : {e}")
            if i % 50 == 0 or i == len(taches):
                print(f"   {i}/{len(taches)} taches terminees")

    print(f"\nTermine en {time.time() - debut:.0f} s : {fichiers} fichiers, "
          f"{total_lignes:,} lignes, {erreurs} erreurs")


if __name__ == "__main__":
    main()