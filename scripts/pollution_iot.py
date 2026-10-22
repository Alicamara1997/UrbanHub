"""
UrbanHub - Partie 3 : flux IoT (pollution de l'air, API OpenAQ v3)

Recupere les mesures des capteurs de pollution des 5 villes du projet
(Paris, Lyon, Marseille, Toulouse, Bordeaux) et les depose dans la couche
Bronze de MinIO : bronze/pollution/...

Usage (dans le conteneur Jupyter) :
  python pollution_iot.py capteurs        -> liste des capteurs (referentiel)
  python pollution_iot.py historique 30   -> moyennes horaires des 30 derniers jours
  python pollution_iot.py live            -> flux IoT : nouvelle mesure toutes les 10 min
  python pollution_iot.py test            -> petit essai (2 lieux)

La cle API est lue dans la variable d'environnement OPENAQ_API_KEY (fichier .env).
"""
import gzip
import io
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

try:
    from minio import Minio
except ImportError:                       # le conteneur a ete recree : on reinstalle
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "minio"], check=True)
    from minio import Minio

# ----------------------------------------------------------------- reglages
API = "https://api.openaq.org/v3"
CLE = os.environ.get("OPENAQ_API_KEY", "")
VILLES = {
    "Paris":     (48.8566, 2.3522),
    "Lyon":      (45.7640, 4.8357),
    "Marseille": (43.2965, 5.3698),
    "Toulouse":  (43.6047, 1.4442),
    "Bordeaux":  (44.8378, -0.5792),
}
RAYON_M = 25000                                   # maximum autorise par OpenAQ
POLLUANTS = {"pm25", "pm10", "no2", "o3", "co"}
INTERVALLE_LIVE_S = 600                           # 10 minutes
PAUSE_APPEL_S = 1.1                               # 60 appels/min max (offre gratuite)
BUCKET = "bronze"
PREFIXE = "pollution"
LOG = "/home/jovyan/work/data/logs/pollution_iot.log"

client = Minio("minio:9000", access_key="minioadmin",
               secret_key="minioadmin123", secure=False)


def log(message):
    ligne = f"{datetime.now():%Y-%m-%d %H:%M:%S}  {message}"
    print(ligne, flush=True)
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(ligne + "\n")


def appel(chemin, params=None):
    """Appel a l'API OpenAQ avec pause (limite de debit) et 3 essais."""
    for essai in range(4):
        time.sleep(PAUSE_APPEL_S)
        try:
            r = requests.get(API + chemin, params=params,
                             headers={"X-API-Key": CLE}, timeout=60)
        except requests.RequestException as e:
            log(f"  reseau : {e} (essai {essai + 1})")
            time.sleep(10)
            continue
        if r.status_code == 429:                  # trop d'appels : on attend
            attente = int(float(r.headers.get("x-ratelimit-reset", 60) or 60)) + 1
            log(f"  limite atteinte, pause {attente} s")
            time.sleep(attente)
            continue
        if r.status_code in (404, 422):
            return None
        if r.status_code >= 500 or r.status_code == 408:
            time.sleep(5 * (essai + 1))
            continue
        r.raise_for_status()
        return r.json()
    return None


def ecrire_minio(chemin, lignes):
    """Ecrit une liste de dicts en JSON Lines compresse dans MinIO."""
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
    texte = "\n".join(json.dumps(l, ensure_ascii=False) for l in lignes)
    donnees = gzip.compress(texte.encode("utf-8"))
    client.put_object(BUCKET, f"{PREFIXE}/{chemin}", io.BytesIO(donnees),
                      len(donnees), content_type="application/gzip")


def lire_minio(chemin):
    r = client.get_object(BUCKET, f"{PREFIXE}/{chemin}")
    try:
        texte = gzip.decompress(r.read()).decode("utf-8")
    finally:
        r.close()
        r.release_conn()
    return [json.loads(l) for l in texte.splitlines() if l.strip()]


# ------------------------------------------------- 1. referentiel capteurs
def capteurs(villes=VILLES, max_lieux=None, sauver=True):
    """Cherche les lieux de mesure actifs a moins de 25 km de chaque ville
    et garde leurs capteurs des 5 polluants principaux."""
    limite_activite = datetime.now(timezone.utc) - timedelta(days=7)
    referentiel = []
    for ville, (lat, lon) in villes.items():
        rep = appel("/locations", {"coordinates": f"{lat},{lon}",
                                   "radius": RAYON_M, "limit": 1000})
        lieux = (rep or {}).get("results", [])
        actifs = 0
        for lieu in lieux:
            dernier = ((lieu.get("datetimeLast") or {}).get("utc"))
            if not dernier or datetime.fromisoformat(dernier.replace("Z", "+00:00")) < limite_activite:
                continue                          # capteur eteint
            actifs += 1
            if max_lieux and actifs > max_lieux:
                break
            coord = lieu.get("coordinates") or {}
            for s in lieu.get("sensors", []):
                p = s.get("parameter") or {}
                if p.get("name") in POLLUANTS:
                    referentiel.append({
                        "sensor_id": s["id"],
                        "location_id": lieu["id"],
                        "location_name": lieu.get("name"),
                        "ville": ville,
                        "pollutant": p["name"],
                        "unit": p.get("units"),
                        "latitude": coord.get("latitude"),
                        "longitude": coord.get("longitude"),
                        "provider": (lieu.get("provider") or {}).get("name"),
                    })
        nb = sum(1 for c in referentiel if c["ville"] == ville)
        log(f"{ville:10s} : {len(lieux)} lieux trouves, {min(actifs, max_lieux or actifs)} actifs, {nb} capteurs gardes")
    if not sauver:
        return referentiel
    ecrire_minio("referentiel/capteurs.jsonl.gz", referentiel)
    log(f"Referentiel : {len(referentiel)} capteurs -> {BUCKET}/{PREFIXE}/referentiel/capteurs.jsonl.gz")
    return referentiel


def charger_referentiel():
    try:
        return lire_minio("referentiel/capteurs.jsonl.gz")
    except Exception:
        log("Referentiel absent : on le cree d'abord.")
        return capteurs()


# --------------------------------------------- 2. historique (moyennes horaires)
def historique(jours=30):
    ref = charger_referentiel()
    fin = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    debut = fin - timedelta(days=jours)
    total = 0
    for i, c in enumerate(ref, 1):
        lignes, page = [], 1
        while True:
            rep = appel(f"/sensors/{c['sensor_id']}/hours", {
                "datetime_from": debut.isoformat().replace("+00:00", "Z"),
                "datetime_to": fin.isoformat().replace("+00:00", "Z"),
                "limit": 1000, "page": page})
            res = (rep or {}).get("results", [])
            for m in res:
                lignes.append({
                    **{k: c[k] for k in ("sensor_id", "location_id", "location_name",
                                         "ville", "pollutant", "unit", "latitude", "longitude")},
                    "value": m.get("value"),
                    "timestamp": ((m.get("period") or {}).get("datetimeFrom") or {}).get("utc"),
                })
            if len(res) < 1000:
                break
            page += 1
        if lignes:
            ecrire_minio(f"historique/ville={c['ville']}/sensor_{c['sensor_id']}.jsonl.gz", lignes)
        total += len(lignes)
        log(f"[{i}/{len(ref)}] {c['ville']} {c['pollutant']:4s} capteur {c['sensor_id']} : {len(lignes)} heures")
    log(f"Historique termine : {total} mesures horaires sur {jours} jours.")


# ------------------------------------------------- 3. flux IoT (temps reel)
def releve(ref, numero=1):
    """Un releve : derniere valeur de chaque capteur du referentiel."""
    par_capteur = {c["sensor_id"]: c for c in ref}
    lieux = sorted({c["location_id"] for c in ref})
    maintenant = datetime.now(timezone.utc)
    lignes = []
    for lieu in lieux:
        rep = appel(f"/locations/{lieu}/latest", {"limit": 1000})
        for m in (rep or {}).get("results", []):
            c = par_capteur.get(m.get("sensorsId"))
            if not c:
                continue                          # autre polluant (ex. temperature)
            coord = m.get("coordinates") or {}
            lignes.append({
                "sensor_id": c["sensor_id"],
                "location_id": c["location_id"],
                "location_name": c["location_name"],
                "ville": c["ville"],
                "pollutant": c["pollutant"],
                "unit": c["unit"],
                "value": m.get("value"),
                "timestamp": (m.get("datetime") or {}).get("utc"),
                "latitude": coord.get("latitude", c["latitude"]),
                "longitude": coord.get("longitude", c["longitude"]),
                "collected_at": maintenant.isoformat().replace("+00:00", "Z"),
            })
    if lignes:
        ecrire_minio(f"flux/date={maintenant:%Y-%m-%d}/releve_{maintenant:%H%M%S}.jsonl.gz", lignes)
    log(f"Releve {numero} : {len(lignes)} mesures de {len(lieux)} lieux -> flux/date={maintenant:%Y-%m-%d}/")
    return lignes


def live():
    ref = charger_referentiel()
    log(f"Flux IoT demarre : {len(ref)} capteurs, un releve toutes les {INTERVALLE_LIVE_S // 60} min (Ctrl+C pour arreter).")
    n = 1
    while True:
        debut = time.time()
        try:
            releve(ref, n)
        except Exception as e:                    # on ne s'arrete jamais sur une erreur
            log(f"Erreur releve {n} : {e}")
        n += 1
        time.sleep(max(0, INTERVALLE_LIVE_S - (time.time() - debut)))


def test():
    log("TEST : 2 lieux pour Paris")
    ref = capteurs({"Paris": VILLES["Paris"]}, max_lieux=2, sauver=False)
    lignes = releve(ref)
    for l in lignes[:5]:
        log(f"  {l['location_name']} | {l['pollutant']} = {l['value']} {l['unit']} a {l['timestamp']}")
    log("TEST OK" if lignes else "TEST : aucune mesure recue")


if __name__ == "__main__":
    if not CLE:
        sys.exit("OPENAQ_API_KEY manquante : verifier le fichier .env et docker compose up -d")
    mode = sys.argv[1] if len(sys.argv) > 1 else "test"
    if mode == "capteurs":
        capteurs()
    elif mode == "historique":
        historique(int(sys.argv[2]) if len(sys.argv) > 2 else 30)
    elif mode == "live":
        live()
    else:
        test()
