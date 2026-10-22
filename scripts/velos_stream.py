"""
UrbanHub - Partie 2 : flux Streaming (velos en libre-service, API CityBikes)

Toutes les 2 min 30, recupere l'etat de toutes les stations des 5 villes
du projet et depose chaque releve dans la couche Bronze de MinIO :
    bronze/velos/date=AAAA-MM-JJ/network=<reseau>/<reseau>_<heure>.jsonl.gz

Usage (dans le conteneur Jupyter) :
  python velos_stream.py test   -> un seul releve, pour verifier
  python velos_stream.py live   -> collecte en continu
"""
import gzip
import io
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

try:
    from minio import Minio
except ImportError:
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "minio"], check=True)
    from minio import Minio

API = "https://api.citybik.es/v2/networks/{}"
RESEAUX = {                      # identifiant CityBikes -> ville
    "velib": "Paris",
    "velov": "Lyon",
    "le-velo": "Marseille",
    "velo": "Toulouse",
    "v3-bordeaux": "Bordeaux",
}
INTERVALLE_S = 180               # 3 min (limite haute du sujet : 2-3 min) pour menager l'API
BUCKET, PREFIXE = "bronze", "velos"
LOG = "/home/jovyan/work/data/logs/velos_stream.log"
ENTETES = {"User-Agent": "UrbanHub-TP-etudiant/1.0 (projet pedagogique)"}

client = Minio("minio:9000", access_key="minioadmin",
               secret_key="minioadmin123", secure=False)


def log(message):
    ligne = f"{datetime.now():%Y-%m-%d %H:%M:%S}  {message}"
    print(ligne, flush=True)
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(ligne + "\n")


def collecter_reseau(reseau, maintenant):
    """Recupere un reseau et l'ecrit dans MinIO. Renvoie le nombre de stations."""
    for essai in range(3):
        try:
            r = requests.get(API.format(reseau), params={"fields": "stations"},
                             headers=ENTETES, timeout=30)
            if r.status_code == 429:                 # trop de demandes : on patiente
                attente = int(r.headers.get("Retry-After", 30) or 30)
                time.sleep(min(attente, 60))
                continue
            r.raise_for_status()
            break
        except requests.RequestException as e:
            if essai == 2:
                log(f"  {reseau} : echec ({e})")
                return 0
            time.sleep(5)
    else:
        log(f"  {reseau} : echec (limite de l'API CityBikes, 429)")
        return 0
    stations = r.json().get("network", {}).get("stations", [])
    lignes = []
    for s in stations:
        extra = s.get("extra") or {}
        lignes.append({
            "network_id": reseau,
            "city": RESEAUX[reseau],
            "station_id": s.get("id"),
            "station_name": s.get("name"),
            "latitude": s.get("latitude"),
            "longitude": s.get("longitude"),
            "timestamp": s.get("timestamp"),            # heure donnee par l'API
            "bikes_available": s.get("free_bikes"),
            "free_slots": s.get("empty_slots"),
            "slots": extra.get("slots"),
            "ebikes": extra.get("ebikes"),
            "renting": extra.get("renting"),
            "collected_at": maintenant.isoformat().replace("+00:00", "Z"),
        })
    if lignes:
        texte = "\n".join(json.dumps(l, ensure_ascii=False) for l in lignes)
        donnees = gzip.compress(texte.encode("utf-8"))
        chemin = (f"{PREFIXE}/date={maintenant:%Y-%m-%d}/network={reseau}/"
                  f"{reseau}_{maintenant:%Y%m%dT%H%M%S}.jsonl.gz")
        client.put_object(BUCKET, chemin, io.BytesIO(donnees), len(donnees),
                          content_type="application/gzip")
    return len(lignes)


def releve(numero=1):
    maintenant = datetime.now(timezone.utc)
    resultats = {}
    for reseau in RESEAUX:                    # une ville apres l'autre, avec une pause :
        resultats[reseau] = collecter_reseau(reseau, maintenant)   # evite la limite 429
        time.sleep(3)
    detail = ", ".join(f"{RESEAUX[r]} {n}" for r, n in resultats.items())
    log(f"Releve {numero} : {sum(resultats.values())} stations ({detail})")


def live():
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
    log(f"Flux velos demarre : un releve toutes les {INTERVALLE_S} s.")
    n = 1
    while True:
        debut = time.time()
        try:
            releve(n)
        except Exception as e:
            log(f"Erreur releve {n} : {e}")
        n += 1
        time.sleep(max(0, INTERVALLE_S - (time.time() - debut)))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "live":
        live()
    else:
        releve()
        log("TEST termine")
